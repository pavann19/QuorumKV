package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestReopenRepairsTailBeforeAcknowledgingNewWrites(t *testing.T) {
	for _, tail := range [][]byte{{1, 2, 3}, {15, 39, 0, 0, 0, 0, 0, 0, 'x'}} {
		path := filepath.Join(t.TempDir(), "store.wal")
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(Record{Op: OpPut, Key: []byte("before")}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(tail); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		w, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Append(Record{Op: OpPut, Key: []byte("after")}); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		records, err := Replay(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 2 || string(records[1].Key) != "after" {
			t.Fatalf("acknowledged append lost after recovery: %+v", records)
		}
	}
}

func TestOversizedLengthFailsRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized.wal")
	header := make([]byte, headerLen)
	binary.LittleEndian.PutUint32(header, ^uint32(0))
	if err := os.WriteFile(path, header, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Replay(path); err == nil {
		t.Fatal("oversized length accepted by replay")
	}
	if w, err := Open(path); err == nil {
		w.Close()
		t.Fatal("oversized length accepted by open")
	}
	if info, err := os.Stat(path); err != nil || info.Size() != headerLen {
		t.Fatal("failed recovery modified the WAL")
	}
}

func TestIOFailurePoisonsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failed.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Op: OpPut, Key: []byte("failed")}); err == nil {
		t.Fatal("closed file write succeeded")
	}
	// Even if the descriptor becomes usable again, uncertain earlier I/O means
	// this WAL instance must not acknowledge more operations.
	w.f, err = os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Append(Record{Op: OpPut, Key: []byte("later")}); err == nil {
		t.Fatal("write accepted after I/O failure")
	}
	if err := w.Reset(nil); err == nil {
		t.Fatal("reset accepted after I/O failure")
	}
}
