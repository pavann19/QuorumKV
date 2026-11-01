package wal

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

type fullDisk struct {
	durableFile
	onSync bool
}

func (f fullDisk) Write(p []byte) (int, error) {
	if !f.onSync {
		return 0, syscall.ENOSPC
	}
	return f.durableFile.Write(p)
}
func (f fullDisk) Sync() error { return syscall.ENOSPC }
func TestDiskPressureNeverAcknowledgesOrContinues(t *testing.T) {
	for _, onSync := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "store.wal")
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Append(Record{Op: OpPut, Key: []byte("ack"), Value: []byte("keep")}); err != nil {
			t.Fatal(err)
		}
		file := w.f
		w.f = fullDisk{file, onSync}
		if err = w.Append(Record{Op: OpPut, Key: []byte("uncertain")}); !errors.Is(err, syscall.ENOSPC) {
			t.Fatalf("disk failure acknowledged: %v", err)
		}
		w.f = file
		if err = w.Append(Record{Op: OpPut, Key: []byte("must-not-ack")}); err == nil {
			t.Fatal("continued after disk uncertainty")
		}
		w.Close()
		records, err := Replay(path)
		if err != nil || len(records) < 1 || string(records[0].Key) != "ack" {
			t.Fatalf("ack prefix lost: %v %v", records, err)
		}
		// Sync failure may leave the complete unacknowledged record, which remains
		// uncertain. Never assert its absence as a fabricated durability guarantee.
		if len(records) > 2 {
			t.Fatal("write after poisoned WAL appeared")
		}
		if _, err = os.Stat(path); err != nil {
			t.Fatal(err)
		}
	}
}
