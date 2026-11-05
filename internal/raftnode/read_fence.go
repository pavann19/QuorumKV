package raftnode

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

// completed application is distinct from Raft.AppliedIndex (dispatch only).
func (f *FSM) publishCommandIndex(index uint64) {
	f.appliedMu.Lock()
	defer f.appliedMu.Unlock()
	f.lastCommandIndex.Store(index)
	if f.appliedChanged != nil {
		close(f.appliedChanged)
		f.appliedChanged = nil
	}
}
func (f *FSM) waitCommand(ctx context.Context, index uint64) error {
	for {
		f.appliedMu.Lock()
		if err := ctx.Err(); err != nil {
			f.appliedMu.Unlock()
			return err
		}
		if f.lastCommandIndex.Load() >= index {
			f.appliedMu.Unlock()
			return nil
		}
		if f.appliedChanged == nil {
			f.appliedChanged = make(chan struct{})
		}
		changed := f.appliedChanged
		f.appliedMu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

type readFenceCache struct {
	sync.Mutex
	index, term, command uint64
	valid                bool
}

func (n *Node) ReadMode() string {
	if n.readMode == "barrier" {
		return "barrier"
	}
	return "quorum"
}
func (n *Node) readTerm() (uint64, error) {
	if !n.IsLeader() {
		return 0, raft.ErrNotLeader
	}
	term, err := strconv.ParseUint(n.Raft.Stats()["term"], 10, 64)
	if err != nil || term == 0 || !n.IsLeader() {
		return 0, raft.ErrNotLeader
	}
	return term, nil
}

var errReadTermPending = errors.New("current-term entry not yet committed")

// committedReadTarget maps a Raft commit index to the last command that can
// affect the keyspace. No-op/configuration/barrier indices are not FSM commands.
// Compacted entries are already in a completed snapshot. Cache only metadata,
// never values; unchanged read-only load performs no repeated log traversal.
func (n *Node) committedReadTarget(ctx context.Context, term uint64) (uint64, error) {
	if n.fsm == nil || n.readLogs == nil {
		return 0, errors.New("read fence not initialized")
	}
	index := n.Raft.CommitIndex()
	n.readFence.Lock()
	defer n.readFence.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if n.readFence.valid && n.readFence.index == index && n.readFence.term == term {
		return n.readFence.command, nil
	}
	stats := n.Raft.Stats()
	snapshotIndex, _ := strconv.ParseUint(stats["last_snapshot_index"], 10, 64)
	snapshotTerm, _ := strconv.ParseUint(stats["last_snapshot_term"], 10, 64)
	var committed raft.Log
	err := n.readLogs.GetLog(index, &committed)
	if err != nil {
		if !errors.Is(err, raft.ErrLogNotFound) || index != snapshotIndex {
			return 0, fmt.Errorf("current-term commitment unavailable: %w", err)
		}
		if snapshotTerm != term {
			return 0, errReadTermPending
		}
	} else if committed.Term != term {
		// A new leader must commit an entry in its own term before a read fence.
		return 0, errReadTermPending
	}
	completed := n.fsm.lastCommandIndex.Load()
	target := completed
	for cursor := index; cursor > completed; cursor-- {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if cursor <= snapshotIndex {
			break
		}
		entry := committed
		if cursor != index {
			if err := n.readLogs.GetLog(cursor, &entry); err != nil {
				// A concurrent snapshot may compact only already-completed state.
				latestSnapshot, _ := strconv.ParseUint(n.Raft.Stats()["last_snapshot_index"], 10, 64)
				if errors.Is(err, raft.ErrLogNotFound) && cursor <= latestSnapshot {
					break
				}
				return 0, err
			}
		}
		if entry.Type == raft.LogCommand {
			target = entry.Index
			break
		}
	}
	n.readFence.index, n.readFence.term, n.readFence.command, n.readFence.valid = index, term, target, true
	return target, nil
}

// The adopted library has no commit notification for its internal leader no-op.
// Only this short leadership-initialization wait probes commitment. Completed
// FSM waits remain event-driven, and the steady read path does not poll.
func (n *Node) waitReadTarget(ctx context.Context, term uint64) (uint64, error) {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		target, err := n.committedReadTarget(ctx, term)
		if !errors.Is(err, errReadTermPending) {
			return target, err
		}
		current, err := n.readTerm()
		if err != nil || current != term {
			return 0, raft.ErrNotLeader
		}
		if timer == nil {
			timer = time.NewTimer(5 * time.Millisecond)
		} else {
			timer.Reset(5 * time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
}

func (n *Node) fencedRead(ctx context.Context, key []byte) (*pb.GetResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if n.ReadMode() == "barrier" {
		deadline, ok := ctx.Deadline()
		remaining := time.Until(deadline)
		if !ok || remaining <= 0 {
			return nil, context.DeadlineExceeded
		}
		if err := n.Raft.Barrier(remaining).Error(); err != nil {
			return nil, err
		}
	} else {
		var phases [4]uint64
		phaseStart := time.Now()
		term, err := n.readTerm()
		if err != nil {
			return nil, err
		}
		target, err := n.waitReadTarget(ctx, term)
		if err != nil {
			return nil, err
		}
		phases[0] = uint64(time.Since(phaseStart).Nanoseconds())
		phaseStart = time.Now()
		// Do not abandon this future on cancellation: the enclosing worker retains
		// its shared admission slot until verification resolves or Raft shuts down.
		if err := n.Raft.VerifyLeader().Error(); err != nil {
			return nil, err
		}
		phases[1] = uint64(time.Since(phaseStart).Nanoseconds())
		phaseStart = time.Now()
		if err := n.fsm.waitCommand(ctx, target); err != nil {
			return nil, err
		}
		phases[2] = uint64(time.Since(phaseStart).Nanoseconds())
		phaseStart = time.Now()
		value, found := n.Get(key)
		finalTerm, err := n.readTerm()
		if err != nil || finalTerm != term {
			return nil, raft.ErrNotLeader
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		phases[3] = uint64(time.Since(phaseStart).Nanoseconds())
		n.observeReadPhases(phases)
		return &pb.GetResponse{Value: value, Found: found}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, found := n.Get(key)
	return &pb.GetResponse{Value: value, Found: found}, nil
}
