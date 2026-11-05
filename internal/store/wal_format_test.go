package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/pavann19/quorumkv/internal/wal"
)

func TestVersionedStoreCompactionRestoreAndCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "versioned.wal")
	format := wal.Format{Version: 1, ClusterID: "store-format-test"}
	limits := Limits{MaxBytes: 32, MaxEntries: 2}
	s, err := OpenWithWALFormat(path, limits, format)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []wal.Record{{Op: wal.OpPut, Key: []byte("keep"), Value: []byte("old")}, {Op: wal.OpPut, Key: []byte("remove"), Value: []byte("old")}, {Op: wal.OpDelete, Key: []byte("remove")}, {Op: wal.OpPut, Key: []byte("keep"), Value: []byte("latest")}} {
		if _, err = s.ApplyBatch([]wal.Record{record}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.CompactSnapshot(); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s, err = OpenWithWALFormat(path, limits, format)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if value, ok := s.Get([]byte("keep")); !ok || string(value) != "latest" {
		t.Fatal("compaction lost current value")
	}
	if _, ok := s.Get([]byte("remove")); ok {
		t.Fatal("compaction resurrected deletion")
	}
	if err = s.Restore([]Entry{{Key: []byte("only"), Value: []byte("snapshot")}}); err != nil {
		t.Fatal(err)
	}
	if err = s.Put([]byte("second"), make([]byte, 32)); !errors.Is(err, ErrCapacity) {
		t.Fatal("versioned WAL bypassed capacity", err)
	}
	if _, ok := s.Get([]byte("keep")); ok {
		t.Fatal("restore retained stale key")
	}
	valid, err := wal.InspectFormat(path, format, nil)
	info, _ := os.Stat(path)
	if err != nil || valid != info.Size() {
		t.Fatal("restore lost header", err)
	}
}
