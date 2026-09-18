package store

import (
	"path/filepath"
	"testing"
)

func TestPutGet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, ok := s.Get([]byte("a"))
	if !ok || string(v) != "1" {
		t.Fatalf("expected a=1, got %q, ok=%v", v, ok)
	}
}

func TestGetMissingKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	_, ok := s.Get([]byte("missing"))
	if ok {
		t.Fatal("expected missing key to not be found")
	}
}

func TestDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	_, ok := s.Get([]byte("a"))
	if ok {
		t.Fatal("expected deleted key to not be found")
	}
}

func TestDeleteMissingKeyIsNotAnError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Delete([]byte("never-existed")); err != nil {
		t.Fatalf("expected deleting an absent key to succeed, got: %v", err)
	}
}

func TestReopen_ReplaysStateFromWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s1, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	if _, ok := s2.Get([]byte("a")); ok {
		t.Fatal("expected deleted key 'a' to stay deleted after reopen")
	}
	v, ok := s2.Get([]byte("b"))
	if !ok || string(v) != "2" {
		t.Fatalf("expected b=2 after reopen, got %q, ok=%v", v, ok)
	}
}

func TestPutOverwritesExistingKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := s.Put([]byte("a"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	v, ok := s.Get([]byte("a"))
	if !ok || string(v) != "2" {
		t.Fatalf("expected a=2 after overwrite, got %q, ok=%v", v, ok)
	}
}
