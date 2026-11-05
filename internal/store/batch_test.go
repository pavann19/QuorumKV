package store

import (
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/wal"
)

func TestBatchCapacityDecisionsOrderAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s, err := OpenWithLimits(path, Limits{MaxBytes: 6, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	records := []wal.Record{{Op: wal.OpPut, Key: []byte("a"), Value: []byte("12345")}, {Op: wal.OpPut, Key: []byte("b"), Value: []byte("no")}, {Op: wal.OpPut, Key: []byte("a"), Value: []byte("x")}, {Op: wal.OpPut, Key: []byte("b"), Value: []byte("yes")}, {Op: wal.OpDelete, Key: []byte("a")}, {Op: wal.OpPut, Key: []byte("c"), Value: []byte("!")}}
	results, err := s.ApplyBatch(records)
	if err != nil {
		t.Fatal(err)
	}
	for i, result := range results {
		if (i == 1) != errors.Is(result, ErrCapacity) {
			t.Fatalf("response %d: %v", i, result)
		}
	}
	if stats := s.WALStats(); stats.Syncs != 1 || stats.Appends != 5 {
		t.Fatal("batch sync count", stats)
	}
	expected := map[string]string{"b": "yes", "c": "!"}
	check := func(s *Store) {
		t.Helper()
		actual := map[string]string{}
		for _, entry := range s.Dump() {
			actual[string(entry.Key)] = string(entry.Value)
		}
		if !reflect.DeepEqual(actual, expected) || s.Usage().Bytes != 6 || s.Usage().Entries != 2 {
			t.Fatal("ordered state mismatch", actual, s.Usage())
		}
	}
	check(s)
	s.Close()
	s, err = OpenWithLimits(path, Limits{MaxBytes: 6, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check(s)
	// A wholly rejected batch does not introduce a disk sync.
	before := s.WALStats()
	results, err = s.ApplyBatch([]wal.Record{{Op: wal.OpPut, Key: []byte("overflow")}})
	if err != nil || !errors.Is(results[0], ErrCapacity) || s.WALStats() != before {
		t.Fatal("rejected batch affected disk", results, err)
	}
}

func storeGateRelease(channel chan struct{}) func() {
	var once sync.Once
	return func() { once.Do(func() { close(channel) }) }
}

type gatedJournal struct {
	journal
	entered, release chan struct{}
	failure          error
}

func (j *gatedJournal) AppendBatch(records []wal.Record) error {
	close(j.entered)
	<-j.release
	if j.failure != nil {
		return j.failure
	}
	return j.journal.AppendBatch(records)
}
func TestBatchReadersObserveOldDurableMapUntilPublication(t *testing.T) {
	for _, failure := range []error{nil, syscall.ENOSPC} {
		s, err := OpenWithLimits(filepath.Join(t.TempDir(), "store.wal"), Limits{MaxBytes: 12, MaxEntries: 2})
		if err != nil {
			t.Fatal(err)
		}
		if err = s.Put([]byte("key"), []byte("original")); err != nil {
			t.Fatal(err)
		}
		gate := &gatedJournal{journal: s.wal, entered: make(chan struct{}), release: make(chan struct{}), failure: failure}
		s.wal = gate
		unblock := storeGateRelease(gate.release)
		defer unblock()
		done := make(chan error, 1)
		go func() {
			_, err := s.ApplyBatch([]wal.Record{{Op: wal.OpPut, Key: []byte("key"), Value: []byte("new")}})
			done <- err
		}()
		select {
		case <-gate.entered:
		case <-time.After(time.Second):
			t.Fatal("journal not entered")
		}
		read := make(chan string, 1)
		go func() { value, _ := s.Get([]byte("key")); read <- string(value) }()
		select {
		case value := <-read:
			if value != "original" {
				t.Fatal("unpublished value escaped", value)
			}
		case <-time.After(time.Second):
			unblock()
			t.Fatal("reader blocked on journal I/O")
		}
		if _, err = s.ReservePut([]byte("key"), []byte("x")); !errors.Is(err, ErrCapacity) {
			unblock()
			t.Fatal("reservation borrowed unfinished overwrite credit", err)
		}
		unblock()
		err = <-done
		if failure == nil {
			if err != nil {
				t.Fatal(err)
			}
			value, _ := s.Get([]byte("key"))
			if string(value) != "new" {
				t.Fatal("durable batch not published")
			}
		} else {
			if !errors.Is(err, failure) {
				t.Fatal("journal failure acknowledged", err)
			}
			value, _ := s.Get([]byte("key"))
			if string(value) != "original" {
				t.Fatal("failed batch published")
			}
			if err = s.Put([]byte("later"), nil); err == nil {
				t.Fatal("continued after uncertain journal failure")
			}
		}
		if usage := s.Usage(); usage.ReservedBytes != 0 || usage.ReservedEntries != 0 {
			t.Fatal("publication reservation leaked", usage)
		}
		s.Close()
	}
}

func TestBatchOwnsBuffersBeforeJournalIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	gate := &gatedJournal{journal: s.wal, entered: make(chan struct{}), release: make(chan struct{})}
	s.wal = gate
	unblock := storeGateRelease(gate.release)
	defer unblock()
	key, value := []byte("key"), []byte("owned")
	done := make(chan error, 1)
	go func() { _, err := s.ApplyBatch([]wal.Record{{Op: wal.OpPut, Key: key, Value: value}}); done <- err }()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("journal not entered")
	}
	key[0] = 'X'
	value[0] = 'X'
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, found := s.Get([]byte("key"))
	if !found || string(got) != "owned" {
		t.Fatal("batch retained caller memory", got)
	}
	s.Close()
	replay, err := wal.Replay(path)
	if err != nil || len(replay) != 1 || string(replay[0].Key) != "key" || string(replay[0].Value) != "owned" {
		t.Fatal("batch WAL retained caller memory", replay, err)
	}
}

type gatedResetJournal struct {
	journal
	entered, release chan struct{}
}

func (j *gatedResetJournal) Reset(records []wal.Record) error {
	close(j.entered)
	<-j.release
	return j.journal.Reset(records)
}
func TestRestorePublishesOnlyAfterDurableResetWithoutBlockingReaders(t *testing.T) {
	s, err := OpenWithLimits(filepath.Join(t.TempDir(), "store.wal"), Limits{MaxBytes: 12, MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Put([]byte("old"), []byte("original")); err != nil {
		t.Fatal(err)
	}
	gate := &gatedResetJournal{journal: s.wal, entered: make(chan struct{}), release: make(chan struct{})}
	s.wal = gate
	unblock := storeGateRelease(gate.release)
	defer unblock()
	done := make(chan error, 1)
	go func() { done <- s.Restore([]Entry{{Key: []byte("new"), Value: []byte("ack")}}) }()
	select {
	case <-gate.entered:
	case <-time.After(time.Second):
		t.Fatal("reset not entered")
	}
	read := make(chan bool, 1)
	go func() { value, found := s.Get([]byte("old")); read <- found && string(value) == "original" }()
	select {
	case correct := <-read:
		if !correct {
			unblock()
			t.Fatal("restore published before reset")
		}
	case <-time.After(time.Second):
		unblock()
		t.Fatal("reader blocked on reset I/O")
	}
	if _, err := s.ReservePut([]byte("old"), []byte("x")); !errors.Is(err, ErrCapacity) {
		unblock()
		t.Fatal("reservation used unfinished restore credit", err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, found := s.Get([]byte("old")); found {
		t.Fatal("stale key retained after durable reset")
	}
	if value, found := s.Get([]byte("new")); !found || string(value) != "ack" {
		t.Fatal("durable restore missing")
	}
}
