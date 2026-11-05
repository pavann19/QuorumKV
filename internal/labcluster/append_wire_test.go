package labcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	transportpb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type appendWireProbe struct {
	sync.Mutex
	phase                                                             string
	maxBytes, maxEntries, frames, multiFrames, catchupMulti, installs int
}

func (p *appendWireProbe) observe(message any) {
	p.Lock()
	defer p.Unlock()
	switch request := message.(type) {
	case *transportpb.AppendEntriesRequest:
		if len(request.Entries) == 0 {
			return
		}
		p.frames++
		p.maxBytes = max(p.maxBytes, proto.Size(request))
		p.maxEntries = max(p.maxEntries, len(request.Entries))
		if len(request.Entries) > 1 {
			p.multiFrames++
			if p.phase == "catchup" {
				p.catchupMulti++
			}
		}
	case *transportpb.InstallSnapshotRequest:
		p.installs++
	}
}
func (p *appendWireProbe) unary(ctx context.Context, request any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	p.observe(request)
	return next(ctx, request)
}

type observedAppendStream struct {
	grpc.ServerStream
	probe *appendWireProbe
}

func (s *observedAppendStream) RecvMsg(message any) error {
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return err
	}
	s.probe.observe(message)
	return nil
}
func (p *appendWireProbe) stream(server any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	return next(server, &observedAppendStream{stream, p})
}

func TestMaximumValueBatchWireBoundAndFollowerCatchup(t *testing.T) {
	for _, mtls := range []bool{false, true} {
		t.Run(fmt.Sprint("mtls-", mtls), func(t *testing.T) {
			settings := raftnode.DefaultRaftSettings()
			settings.MaxAppendEntries = raftnode.MaxReplicationEntries
			settings.SnapshotMS = time.Hour.Milliseconds()
			settings.SnapshotThreshold = 1 << 20
			probe := &appendWireProbe{phase: "writes"}
			c, err := newModeRaftProfile(t.TempDir(), mtls, 0, "quorum", settings, grpc.ChainUnaryInterceptor(probe.unary), grpc.ChainStreamInterceptor(probe.stream))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			leader, err := c.Leader(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte("deleted"), Value: []byte("seed")}); err != nil {
				t.Fatal(err)
			}
			lag := (leader + 1) % len(c.Nodes)
			if err = c.Stop(lag); err != nil {
				t.Fatal(err)
			}
			const writes = 16
			keys := make([][]byte, writes)
			values := make([][]byte, writes)
			for i := range keys {
				prefix := []byte(fmt.Sprintf("entry-%02d-", i))
				keys[i] = append(prefix, bytes.Repeat([]byte("k"), requestlimits.MaxKeyBytes-len(prefix))...)
				values[i] = bytes.Repeat([]byte{byte(i + 1)}, requestlimits.MaxValueBytes)
			}
			var wg sync.WaitGroup
			errs := make(chan error, writes)
			for i := range keys {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_, err := c.Clients[leader].Put(ctx, &pb.PutRequest{Key: keys[i], Value: values[i]})
					if err != nil {
						errs <- err
					}
				}(i)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			if t.Failed() {
				return
			}
			if _, err = c.Clients[leader].Delete(ctx, &pb.DeleteRequest{Key: []byte("deleted")}); err != nil {
				t.Fatal(err)
			}
			target := c.Nodes[leader].Raft.CommitIndex()
			probe.Lock()
			probe.phase = "catchup"
			probe.Unlock()
			started := time.Now()
			if err = c.Restart(lag); err != nil {
				t.Fatal(err)
			}
			for {
				_, deletedStillPresent := c.Nodes[lag].Store.Get([]byte("deleted"))
				if c.Nodes[lag].Raft.CommitIndex() >= target && c.Nodes[lag].Store.Usage().Entries == writes && !deletedStillPresent {
					break
				}
				select {
				case <-ctx.Done():
					t.Fatal("lagging replica did not recover exact acknowledged prefix", ctx.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			catchupSeconds := time.Since(started).Seconds()
			for replica, node := range c.Nodes {
				for i := range keys {
					value, found := node.Store.Get(keys[i])
					if !found || !bytes.Equal(value, values[i]) {
						t.Fatalf("replica %d lost maximum-size acknowledgement %d", replica, i)
					}
				}
				if node.Store.Usage().Entries != writes {
					t.Fatal("replica has unexpected keys", replica)
				}
				if _, found := node.Store.Get([]byte("deleted")); found {
					t.Fatal("deletion resurrected", replica)
				}
			}
			for i := range keys {
				result, err := c.Clients[leader].Get(ctx, &pb.GetRequest{Key: keys[i]})
				if err != nil || !result.GetFound() || !bytes.Equal(result.GetValue(), values[i]) {
					t.Fatal("linearizable maximum value readback", i, err)
				}
			}
			probe.Lock()
			maxBytes, maxEntries, frames, multi, catchupMulti, installs := probe.maxBytes, probe.maxEntries, probe.frames, probe.multiFrames, probe.catchupMulti, probe.installs
			probe.Unlock()
			if maxBytes > requestlimits.MaxMessageBytes || maxEntries > raftnode.MaxReplicationEntries || multi == 0 || catchupMulti == 0 || installs != 0 {
				t.Fatalf("wire/catchup gate: bytes=%d entries=%d frames=%d multi=%d catchup_multi=%d snapshots=%d", maxBytes, maxEntries, frames, multi, catchupMulti, installs)
			}
			report := map[string]any{"status": "PASS_LOCAL_MAXIMUM_VALUE_LOG_CATCHUP", "mtls": mtls, "nodes": 3, "writes": writes, "value_bytes": requestlimits.MaxValueBytes, "key_bytes": requestlimits.MaxKeyBytes, "configured_append_entries": settings.MaxAppendEntries, "physical_message_limit": requestlimits.MaxMessageBytes, "largest_received_canonical_proto_bytes": maxBytes, "largest_physical_entry_count": maxEntries, "received_append_frames": frames, "multi_entry_frames": multi, "catchup_multi_entry_frames": catchupMulti, "snapshot_chunks": installs, "catchup_to_exact_state_seconds": catchupSeconds, "scope": "local log catch-up only, not production RTO/capacity"}
			if mtls {
				testRawOversizePeerFramesStillRefused(t, c, leader)
				report["unary_and_stream_oversize_refused_without_raft_effect"] = true
			}
			output := os.Getenv("QUORUMKV_RESULTS_DIR")
			if output != "" {
				if err = os.MkdirAll(output, 0700); err != nil {
					t.Fatal(err)
				}
				raw, _ := json.MarshalIndent(report, "", "  ")
				if err = os.WriteFile(filepath.Join(output, fmt.Sprintf("append-wire-mtls-%v.json", mtls)), append(raw, '\n'), 0600); err != nil {
					t.Fatal(err)
				}
			}
			raw, _ := json.Marshal(report)
			t.Log(string(raw))
		})
	}
}
func testRawOversizePeerFramesStillRefused(t *testing.T, c *Cluster, target int) {
	t.Helper()
	sender := (target + 1) % len(c.Nodes)
	connection, err := grpc.NewClient(c.Peers[target].Addr, grpc.WithTransportCredentials(c.identities[sender].ClientCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(raftnode.MaxLogicalAppendBytes)))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	before := c.Nodes[target].Raft.Stats()
	beforeIndex := c.Nodes[target].Raft.LastIndex()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs(raftnode.PeerClusterHeader, c.clusterID, raftnode.PeerProtocolHeader, raftnode.PeerProtocolVersion, raftnode.PeerSnapshotFormatHeader, "0"))
	request := &transportpb.AppendEntriesRequest{RpcHeader: &transportpb.RPCHeader{ProtocolVersion: 3, Id: []byte(c.Peers[sender].ID), Addr: []byte(c.Peers[sender].Addr)}, Term: 10000, Leader: []byte(c.Peers[sender].Addr), PrevLogEntry: beforeIndex, PrevLogTerm: 2}
	for i := 0; i < 8; i++ {
		request.Entries = append(request.Entries, &transportpb.Log{Index: beforeIndex + uint64(i) + 1, Term: 10000, Data: make([]byte, requestlimits.MaxValueBytes)})
	}
	client := transportpb.NewRaftTransportClient(connection)
	if _, err = client.AppendEntries(ctx, request); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("raw oversized unary reached Raft", err)
	}
	stream, err := client.AppendEntriesPipeline(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(request)
	_, err = stream.Recv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("raw oversized stream reached Raft", err)
	}
	if c.Nodes[target].Raft.Stats()["term"] != before["term"] || c.Nodes[target].Raft.LastIndex() != beforeIndex {
		t.Fatal("rejected oversized frame changed Raft term/log")
	}
}
