package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestCapacityAccountingAndRejectionPreserveWAL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s, err := OpenWithLimits(path, Limits{MaxBytes: 8, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Put([]byte("a"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	if err = s.Put([]byte("b"), []byte("456")); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if !errors.Is(s.Put([]byte("c"), nil), ErrCapacity) {
		t.Fatal("entry limit bypassed")
	}
	if !errors.Is(s.Put([]byte("a"), []byte("1234")), ErrCapacity) {
		t.Fatal("byte limit bypassed")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("rejection mutated WAL")
	}
	if err = s.Put([]byte("a"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if u := s.Usage(); u.Bytes != 6 || u.Entries != 2 {
		t.Fatalf("incorrect shrink accounting %+v", u)
	}
	if err = s.Delete([]byte("b")); err != nil {
		t.Fatal(err)
	}
	if err = s.Put([]byte("c"), []byte("12345")); err != nil {
		t.Fatal(err)
	}
	if u := s.Usage(); u.Bytes != 8 || u.Entries != 2 {
		t.Fatalf("incorrect delete accounting %+v", u)
	}
}

func TestReservationsBoundPendingAndReleaseIdempotently(t *testing.T) {
	s, err := OpenWithLimits(filepath.Join(t.TempDir(), "store.wal"), Limits{MaxBytes: 4, MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	release, err := s.ReservePut([]byte("a"), []byte("123"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.ReservePut([]byte("b"), nil); !errors.Is(err, ErrCapacity) {
		t.Fatal("pending capacity bypassed")
	}
	release()
	release()
	if u := s.Usage(); u.ReservedBytes != 0 || u.ReservedEntries != 0 {
		t.Fatalf("reservation leaked %+v", u)
	}
	if err = s.Put([]byte("a"), []byte("123")); err != nil {
		t.Fatal(err)
	}
	release, err = s.ReservePut([]byte("a"), []byte("x"))
	if err != nil {
		t.Fatal("shrink at full capacity rejected", err)
	}
	release()
}

func TestOversizedRestoreRefusedWithoutChangingState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s, err := OpenWithLimits(path, Limits{MaxBytes: 4, MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Put([]byte("a"), []byte("old"))
	before, _ := os.ReadFile(path)
	if !errors.Is(s.Restore([]Entry{{Key: []byte("large"), Value: []byte("snapshot")}}), ErrCapacity) {
		t.Fatal("restore cap bypassed")
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("rejected restore changed original")
	}
	if value, _ := s.Get([]byte("a")); string(value) != "old" {
		t.Fatal("rejected restore changed map")
	}
}
