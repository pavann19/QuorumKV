package raftnode

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/pavann19/quorumkv/internal/wal"
)

const FatalExitCode = 70

var ErrFatalState = errors.New("raftnode: fatal state requires operator recovery")

type fatalIncident struct {
	Version int    `json:"version"`
	UTC     string `json:"utc"`
	Reason  string `json:"reason"`
	Index   uint64 `json:"index"`
}

// writeFatal never overwrites the first incident. Reasons are fixed identifiers;
// committed bytes, keys, values, identities and raw I/O errors are not recorded.
func writeFatal(dir, reason string, index uint64) error {
	f, err := os.OpenFile(filepath.Join(dir, "FATAL"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if errors.Is(err, os.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	raw, err := json.Marshal(fatalIncident{1, time.Now().UTC().Format(time.RFC3339Nano), reason, index})
	if err == nil {
		_, err = f.Write(raw)
	}
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return wal.SyncDirectory(dir)
}

// checkFatal runs under exclusive member ownership, before storage is opened.
// Explicit clearance archives the marker; it does not repair data or erase evidence.
func checkFatal(dir string, clear bool) error {
	path := filepath.Join(dir, "FATAL")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: inspect incident marker", ErrFatalState)
	}
	if !clear {
		return ErrFatalState
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("%w: marker is not a regular file", ErrFatalState)
	}
	archive, err := os.CreateTemp(dir, "FATAL.resolved.*")
	if err != nil {
		return err
	}
	name := archive.Name()
	if err = archive.Close(); err != nil {
		return err
	}
	// Reserve a unique name before replacing it. No original incident is removed.
	if err = os.Rename(path, name); err != nil {
		return err
	}
	return wal.SyncDirectory(dir)
}

func fatalStop(dir, reason string, index uint64) {
	err := writeFatal(dir, reason, index)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	logger.Error("fatal committed-state failure", "component", "storage", "reason", reason, "index", index, "marker_persisted", err == nil, "exit_code", FatalExitCode)
	os.Exit(FatalExitCode)
}

func (f *FSM) stop(reason string, index uint64) {
	if f.fatal != nil {
		f.fatal(reason, index)
	}
	// Standalone FSMs have no process/data-directory owner; never continue if a
	// supplied fatal handler unexpectedly returns.
	panic("raftnode: fatal committed-state failure: " + reason)
}
