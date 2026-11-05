package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestDirectoryFailureRefusesOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	denied := errors.New("directory sync denied")
	w, err := openWithSync(path, func(string) error { return denied })
	if w != nil || !errors.Is(err, denied) {
		t.Fatalf("open acknowledged directory failure: %v", err)
	}
}

func TestResetDirectoryFailurePoisonsWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	denied := errors.New("directory sync denied")
	w.syncParent = func(string) error { return denied }
	if err := w.Reset([]Record{{Op: OpPut, Key: []byte("replacement")}}); !errors.Is(err, denied) {
		t.Fatalf("reset acknowledged: %v", err)
	}
	if err := w.Append(Record{Op: OpPut, Key: []byte("later")}); err == nil {
		t.Fatal("append after uncertain rename")
	}
}

func TestResetPreservesUnownedTemporaryFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	sentinel := path + ".tmp"
	if err := os.WriteFile(sentinel, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if err := w.Reset([]Record{{Op: OpPut, Key: []byte("new")}}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(sentinel)
	if err != nil || string(data) != "preserve" {
		t.Fatalf("unowned temp overwritten: %q %v", data, err)
	}
	matches, _ := filepath.Glob(path + ".*.tmp")
	if len(matches) != 0 {
		t.Fatalf("owned temp leaked: %v", matches)
	}
}
