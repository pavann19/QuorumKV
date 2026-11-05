package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSnapshotCompactionPreservesLatestStateAndLaterWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	for i := 0; i < 100; i++ {
		if err = s.Put([]byte("same"), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	s.Put([]byte("deleted"), []byte("old"))
	s.Delete([]byte("deleted"))
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := s.CompactSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || string(entries[0].Value) != "99" {
		t.Fatalf("bad snapshot: %v", entries)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size() {
		t.Fatal("WAL history did not compact")
	}
	if err = s.Put([]byte("later"), []byte("ack")); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = nil
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for k, want := range map[string]string{"same": "99", "later": "ack"} {
		v, ok := reopened.Get([]byte(k))
		if !ok || string(v) != want {
			t.Fatalf("lost %s: %q", k, v)
		}
	}
	if _, found := reopened.Get([]byte("deleted")); found {
		t.Fatal("deleted key resurrected")
	}
}

func TestConcurrentCompactionNeverLosesAcknowledgedWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	group.Add(1)
	go func() {
		defer group.Done()
		for i := 0; i < 20; i++ {
			if _, err := s.CompactSnapshot(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for i := 0; i < 100; i++ {
		if err := s.Put([]byte(fmt.Sprint(i)), []byte("ack")); err != nil {
			t.Fatal(err)
		}
	}
	group.Wait()
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if len(s.Dump()) != 100 {
		t.Fatalf("acknowledged writes lost: %d", len(s.Dump()))
	}
}
