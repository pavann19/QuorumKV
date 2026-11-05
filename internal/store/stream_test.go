package store

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pavann19/quorumkv/internal/wal"
)

func TestStreamingRecoveryLongOverwriteHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]wal.Record, 2048)
	for i := range records {
		records[i] = wal.Record{Op: wal.OpPut, Key: []byte("same"), Value: bytes.Repeat([]byte{byte(i)}, 4096)}
	}
	if err = w.Reset(records); err != nil {
		t.Fatal(err)
	}
	w.Close()
	s, err := OpenWithLimits(path, Limits{MaxBytes: 4100, MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if u := s.Usage(); u.Bytes != 4100 || u.Entries != 1 {
		t.Fatalf("bad replay accounting %+v", u)
	}
	value, found := s.Get([]byte("same"))
	if !found || !bytes.Equal(value, records[len(records)-1].Value) {
		t.Fatal("latest overwrite lost")
	}
}

func TestStreamingRecoveryBudgetFailurePreservesOriginal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := wal.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(wal.Record{Op: wal.OpPut, Key: []byte("a"), Value: []byte("value")})
	w.Close()
	original, _ := os.ReadFile(path)
	if s, err := OpenWithLimits(path, Limits{MaxBytes: 2, MaxEntries: 1}); !errors.Is(err, ErrCapacity) || s != nil {
		if s != nil {
			s.Close()
		}
		t.Fatal("budget bypassed", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(original, after) {
		t.Fatal("budget failure modified original")
	}
}
