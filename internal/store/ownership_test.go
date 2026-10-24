package store

import (
	"path/filepath"
	"testing"
)

func TestValuesDoNotAliasCallerOrSnapshotMemory(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	value := []byte("original")
	if err := s.Put([]byte("k"), value); err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	got, _ := s.Get([]byte("k"))
	if string(got) != "original" {
		t.Fatal("Put retained caller memory")
	}
	got[0] = 'Y'
	snapshot := s.Dump()
	if string(snapshot[0].Value) != "original" {
		t.Fatal("Get exposed store memory")
	}
	snapshot[0].Value[0] = 'Z'
	got, _ = s.Get([]byte("k"))
	if string(got) != "original" {
		t.Fatal("Dump exposed store memory")
	}
	if err := s.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot[0].Value[0] = 'Q'
	got, _ = s.Get([]byte("k"))
	if string(got) != "Zriginal" {
		t.Fatal("Restore retained snapshot memory")
	}
}
