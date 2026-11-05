package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestStreamingReplayVisitsOnceAndAppendsAfterRepair(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"a", "b", "c"} {
		if err = w.Append(Record{Op: OpPut, Key: []byte(k), Value: []byte(k)}); err != nil {
			t.Fatal(err)
		}
	}
	w.Close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{1, 2})
	f.Close()
	var seen []Record
	w, err = OpenReplay(path, func(r Record) error { seen = append(seen, r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 {
		t.Fatalf("visits=%d", len(seen))
	}
	for i, k := range []string{"a", "b", "c"} {
		if string(seen[i].Key) != k || string(seen[i].Value) != k {
			t.Fatal("visitor record storage reused")
		}
	}
	if err = w.Append(Record{Op: OpPut, Key: []byte("d")}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	records, err := Replay(path)
	if err != nil || len(records) != 4 {
		t.Fatalf("append/repaired prefix: %v %v", records, err)
	}
}

func TestStreamingVisitorFailurePreservesEntireOriginal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Op: OpPut, Key: []byte("a")})
	w.Close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.Write([]byte{1, 2, 3})
	f.Close()
	original, _ := os.ReadFile(path)
	denied := errors.New("budget refused")
	if w, err = OpenReplay(path, func(Record) error { return denied }); !errors.Is(err, denied) || w != nil {
		if w != nil {
			w.Close()
		}
		t.Fatal("visitor error ignored", err)
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(original) {
		t.Fatal("failed replay repaired or changed original")
	}
}

func TestStreamingCorruptionNeverRepairsAcknowledgedRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Op: OpPut, Key: []byte("a")})
	w.Append(Record{Op: OpPut, Key: []byte("b")})
	w.Close()
	raw, _ := os.ReadFile(path)
	raw[len(raw)-1] ^= 0xff
	os.WriteFile(path, raw, 0600)
	visits := 0
	w, err = OpenReplay(path, func(Record) error { visits++; return nil })
	if w != nil || !errors.Is(err, ErrCorrupt) || visits != 1 {
		t.Fatalf("bad corruption outcome: %v visits=%d", err, visits)
	}
	after, _ := os.ReadFile(path)
	if string(raw) != string(after) {
		t.Fatal("corrupt evidence changed")
	}
}
