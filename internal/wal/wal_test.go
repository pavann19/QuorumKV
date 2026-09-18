package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndReplay_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records := []Record{
		{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: OpPut, Key: []byte("b"), Value: []byte("2")},
		{Op: OpDelete, Key: []byte("a")},
	}
	for _, r := range records {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(got))
	}
	for i, r := range records {
		if got[i].Op != r.Op || string(got[i].Key) != string(r.Key) || string(got[i].Value) != string(r.Value) {
			t.Fatalf("record %d mismatch: got %+v, want %+v", i, got[i], r)
		}
	}
}

func TestReplay_MissingFileReturnsEmpty(t *testing.T) {
	got, err := Replay(filepath.Join(t.TempDir(), "does-not-exist.wal"))
	if err != nil {
		t.Fatalf("expected no error for a missing WAL file, got: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no records, got %d", len(got))
	}
}

func TestReplay_EmptyFileReturnsEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty.wal")
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := Replay(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no records, got %d", len(got))
	}
}

func TestReplay_StopsAtTornTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "torn.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	good := []Record{
		{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")},
		{Op: OpPut, Key: []byte("k2"), Value: []byte("v2")},
	}
	for _, r := range good {
		if err := w.Append(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-Append: a third record whose header claims a
	// body length far longer than what's actually on disk.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// header: bodyLen=9999 (torn), crc=0 (irrelevant, body never completes)
	torn := []byte{0x0F, 0x27, 0x00, 0x00, 0, 0, 0, 0, 'x', 'y'} // a few stray bytes, not a full 9999-byte body
	if _, err := f.Write(torn); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := Replay(path)
	if err != nil {
		t.Fatalf("expected a torn trailing record to be silently truncated, not an error: %v", err)
	}
	if len(got) != len(good) {
		t.Fatalf("expected exactly the %d good records before the torn one, got %d", len(good), len(got))
	}
}

func TestReplay_StopsAtCorruptedChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Op: OpPut, Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Op: OpPut, Key: []byte("k2"), Value: []byte("v2")}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Flip a byte inside the second record's body -- length and structure
	// stay intact, but the CRC no longer matches, exactly what a partial
	// disk-level corruption (not just a torn append) would look like.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := Replay(path)
	if err != nil {
		t.Fatalf("expected corruption to be silently truncated, not an error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 valid record before the corrupted one, got %d", len(got))
	}
}
