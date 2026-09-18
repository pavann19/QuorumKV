package wal

import (
	"path/filepath"
	"testing"
)

func TestReset_ReplacesStaleRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "reset.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write some "stale" records, e.g. a key that a snapshot will no
	// longer contain.
	if err := w.Append(Record{Op: OpPut, Key: []byte("stale"), Value: []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Op: OpPut, Key: []byte("kept"), Value: []byte("old-value")}); err != nil {
		t.Fatal(err)
	}

	// A snapshot restore that no longer has "stale" and has a new value
	// for "kept".
	if err := w.Reset([]Record{
		{Op: OpPut, Key: []byte("kept"), Value: []byte("new-value")},
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Append(Record{Op: OpPut, Key: []byte("after-reset"), Value: []byte("v")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("expected exactly 2 records after reset (no stale entry resurrected), got %d: %+v", len(got), got)
	}
	if string(got[0].Key) != "kept" || string(got[0].Value) != "new-value" {
		t.Fatalf("expected kept=new-value, got %+v", got[0])
	}
	if string(got[1].Key) != "after-reset" {
		t.Fatalf("expected after-reset record to follow, got %+v", got[1])
	}
}
