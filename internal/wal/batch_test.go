package wal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type gatedBatchFile struct {
	durableFile
	entered, release chan struct{}
	writes, syncs    int
}

func (f *gatedBatchFile) Write(data []byte) (int, error) {
	f.writes++
	return f.durableFile.Write(data)
}
func (f *gatedBatchFile) Sync() error {
	f.syncs++
	close(f.entered)
	<-f.release
	return f.durableFile.Sync()
}
func TestAppendBatchAcknowledgesOnlyAfterSharedSync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	file := &gatedBatchFile{durableFile: w.f, entered: make(chan struct{}), release: make(chan struct{})}
	w.f = file
	released := false
	defer func() {
		if !released {
			close(file.release)
		}
	}()
	records := []Record{{Op: OpPut, Key: []byte("a"), Value: []byte("1")}, {Op: OpPut, Key: []byte("b"), Value: []byte("2")}, {Op: OpDelete, Key: []byte("a")}}
	done := make(chan error, 1)
	go func() { done <- w.AppendBatch(records) }()
	select {
	case <-file.entered:
	case <-time.After(time.Second):
		t.Fatal("batch did not reach sync")
	}
	select {
	case err := <-done:
		t.Fatal("batch acknowledged before sync", err)
	default:
	}
	if stats := w.Stats(); stats.Syncs != 0 || stats.Appends != 0 {
		t.Fatal("premature durable counters", stats)
	}
	close(file.release)
	released = true
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if file.writes != 1 || file.syncs != 1 || w.Stats().Appends != 3 || w.Stats().Syncs != 1 {
		t.Fatal("batch was not one write and sync", file.writes, file.syncs, w.Stats())
	}
	replay, err := Replay(path)
	if err != nil || len(replay) != len(records) {
		t.Fatal("batch replay length", replay, err)
	}
	for i, record := range records {
		if replay[i].Op != record.Op || !bytes.Equal(replay[i].Key, record.Key) || !bytes.Equal(replay[i].Value, record.Value) {
			t.Fatal("batch replay order changed", replay, err)
		}
	}
}
func TestInvalidBatchHasNoFileEffectAndDoesNotPoison(t *testing.T) {
	huge := make([]byte, MaxRecordSize-9)
	cases := [][]Record{{{Op: 99}}, make([]Record, MaxBatchRecords+1), {{Op: OpPut, Value: huge}, {Op: OpPut, Value: huge}}}
	for i, records := range cases {
		path := filepath.Join(t.TempDir(), "store.wal")
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.AppendBatch(records); err == nil {
			t.Fatal("invalid batch accepted", i)
		}
		data, _ := os.ReadFile(path)
		if len(data) != 0 || w.Stats().Failures != 0 {
			t.Fatal("validation changed WAL", i, w.Stats())
		}
		if err = w.Append(Record{Op: OpPut, Key: []byte("after-refusal")}); err != nil {
			t.Fatal("validation poisoned WAL", err)
		}
		w.Close()
	}
}

type partialBatchFile struct {
	durableFile
	prefix    int
	syncError bool
}

func (f partialBatchFile) Write(p []byte) (int, error) {
	if f.syncError {
		return f.durableFile.Write(p)
	}
	n := min(f.prefix, len(p))
	written, err := f.durableFile.Write(p[:n])
	if err != nil {
		return written, err
	}
	return written, io.ErrShortWrite
}
func (f partialBatchFile) Sync() error { return syscall.ENOSPC }
func TestBatchIOFailurePreservesAcknowledgedPrefixAndPoisons(t *testing.T) {
	for _, scenario := range []struct {
		prefix    int
		syncError bool
	}{{0, false}, {1, false}, {20, false}, {37, false}, {0, true}} {
		path := filepath.Join(t.TempDir(), "store.wal")
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Append(Record{Op: OpPut, Key: []byte("ack"), Value: []byte("keep")}); err != nil {
			t.Fatal(err)
		}
		original := w.f
		w.f = partialBatchFile{original, scenario.prefix, scenario.syncError}
		batch := []Record{{Op: OpPut, Key: []byte("u1"), Value: []byte("x")}, {Op: OpDelete, Key: []byte("u2")}, {Op: OpPut, Key: []byte("u3")}}
		if err = w.AppendBatch(batch); err == nil {
			t.Fatal("failed batch acknowledged")
		}
		if scenario.syncError && !errors.Is(err, syscall.ENOSPC) {
			t.Fatal(err)
		}
		w.f = original
		if err = w.Append(Record{Op: OpPut, Key: []byte("must-not-ack")}); err == nil {
			t.Fatal("continued after uncertainty")
		}
		if stats := w.Stats(); stats.Appends != 1 || stats.Syncs != 1 || stats.Failures != 1 {
			t.Fatal("failed batch counted durable", stats)
		}
		w.Close()
		replay, err := Replay(path)
		if err != nil || len(replay) < 1 || string(replay[0].Key) != "ack" || string(replay[0].Value) != "keep" {
			t.Fatal("acknowledged prefix lost", replay, err)
		}
		for _, record := range replay {
			if string(record.Key) == "must-not-ack" {
				t.Fatal("poisoned append reached disk")
			}
		}
		// Complete unacknowledged batch records are allowed to survive. Opening
		// repairs only an incomplete suffix, then accepts a new independently synced record.
		reopened, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = reopened.Append(Record{Op: OpPut, Key: []byte("new-ack")}); err != nil {
			t.Fatal(err)
		}
		reopened.Close()
		after, err := Replay(path)
		if err != nil || len(after) != len(replay)+1 {
			t.Fatal("repair lost retained prefix", after, err)
		}
	}
}
