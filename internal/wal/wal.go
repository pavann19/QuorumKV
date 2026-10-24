// Package wal implements a write-ahead log: an append-only, fsync'd record
// stream that is the durability primitive the rest of QuorumKV builds on.
// Nothing else in this project (the in-memory store, and later Raft log
// replication) can be trusted until the WAL itself is proven to survive a
// hard crash without losing an acknowledged write or fabricating one that
// was never acknowledged -- see internal/wal's crash-recovery test and
// test/crashrecovery for that proof.
package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// OpType identifies what a Record does to the keyspace.
type OpType byte

const (
	OpPut    OpType = 1
	OpDelete OpType = 2
)

// Record is one durable operation in the log.
type Record struct {
	Op    OpType
	Key   []byte
	Value []byte // unused for OpDelete
}

// Record wire format, one after another with no separators other than their
// own declared length:
//
//	[4 bytes: recordLen (length of everything below, little-endian uint32)]
//	[4 bytes: CRC32 (IEEE) of the op+key+value bytes below]
//	[1 byte:  Op]
//	[4 bytes: len(Key)]
//	[len(Key) bytes: Key]
//	[4 bytes: len(Value)]
//	[len(Value) bytes: Value]
//
// The length+CRC framing is what lets Replay distinguish a real record from
// a torn write left by a crash mid-append: a torn write is either too short
// to contain its own declared length, or its CRC won't match, and Replay
// stops at the first record it can't fully validate rather than treating
// that as a fatal error.
const headerLen = 4 + 4 // recordLen + CRC32

// MaxRecordSize bounds allocations when reading untrusted on-disk lengths.
const MaxRecordSize = 16 << 20

// WAL is an append-only log file. All writes go through Append, which
// fsyncs before returning -- the entire point of a WAL is that Append
// returning nil is a durability promise a caller (e.g. a KV store's Put)
// can rely on even across a hard crash immediately afterward.
type WAL struct {
	path   string
	f      *os.File
	failed error
}

// Open opens (creating if necessary) the WAL file at path for appending.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: opening %s: %w", path, err)
	}
	// Remove an incomplete suffix before accepting any new acknowledged writes.
	reader := bufio.NewReader(f)
	var valid int64
	for {
		rec, ok, scanErr := readOneRecord(reader)
		if scanErr != nil {
			f.Close()
			return nil, scanErr
		}
		if !ok {
			break
		}
		valid += int64(headerLen + 9 + len(rec.Key) + len(rec.Value))
	}
	info, err := f.Stat()
	if err == nil && info.Size() != valid {
		err = f.Truncate(valid)
		if err == nil {
			err = f.Sync()
		}
	}
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: repairing tail: %w", err)
	}
	if _, err := f.Seek(valid, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return &WAL{path: path, f: f}, nil
}

// Close closes the underlying file. It does not fsync -- callers that need
// a final durability guarantee should have already gotten it from Append.
func (w *WAL) Close() error {
	return w.f.Close()
}

// Append writes rec to the log and fsyncs before returning. A nil error is
// a durability guarantee: rec will be present after Replay even if the
// process is killed the instant Append returns.
func (w *WAL) Append(rec Record) (err error) {
	if w.failed != nil {
		return fmt.Errorf("wal: unavailable after I/O failure: %w", w.failed)
	}
	if err := validateRecord(rec); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	body := encodeBody(rec)
	sum := crc32.ChecksumIEEE(body)

	buf := make([]byte, headerLen+len(body))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(buf[4:8], sum)
	copy(buf[headerLen:], body)

	if n, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("wal: writing record: %w", err)
	} else if n != len(buf) {
		return io.ErrShortWrite
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync: %w", err)
	}
	return nil
}

// Reset atomically replaces the WAL's entire contents with records: it
// writes a fresh file (fsynced) and renames it over the original path,
// then reopens for appending. Used when a Raft snapshot install replaces
// local state wholesale (internal/store.Store.Restore) -- appending the
// snapshot's entries to the *existing* log would leave stale pre-snapshot
// records behind, which Replay would then incorrectly resurrect on the
// next restart (a record for a key the snapshot no longer has would still
// replay as if it were current).
func (w *WAL) Reset(records []Record) (err error) {
	if w.failed != nil {
		return fmt.Errorf("wal: unavailable after I/O failure: %w", w.failed)
	}
	for _, rec := range records {
		if err := validateRecord(rec); err != nil {
			return err
		}
	}
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	tmpPath := w.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: creating temp file for reset: %w", err)
	}
	for _, rec := range records {
		body := encodeBody(rec)
		sum := crc32.ChecksumIEEE(body)
		buf := make([]byte, headerLen+len(body))
		binary.LittleEndian.PutUint32(buf[0:4], uint32(len(body)))
		binary.LittleEndian.PutUint32(buf[4:8], sum)
		copy(buf[headerLen:], body)
		if n, err := tmp.Write(buf); err != nil || n != len(buf) {
			if err == nil {
				err = io.ErrShortWrite
			}
			tmp.Close()
			os.Remove(tmpPath)
			return fmt.Errorf("wal: writing record during reset: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("wal: fsync during reset: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("wal: closing temp file during reset: %w", err)
	}

	if err := w.f.Close(); err != nil {
		return fmt.Errorf("wal: closing old file during reset: %w", err)
	}
	if err := os.Rename(tmpPath, w.path); err != nil {
		return fmt.Errorf("wal: renaming reset file into place: %w", err)
	}

	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("wal: reopening after reset: %w", err)
	}
	w.f = f
	return nil
}

func encodeBody(rec Record) []byte {
	buf := make([]byte, 0, 1+4+len(rec.Key)+4+len(rec.Value))
	buf = append(buf, byte(rec.Op))
	buf = appendLenPrefixed(buf, rec.Key)
	buf = appendLenPrefixed(buf, rec.Value)
	return buf
}

func validateRecord(rec Record) error {
	if rec.Op != OpPut && rec.Op != OpDelete {
		return fmt.Errorf("wal: unknown operation %d", rec.Op)
	}
	if len(rec.Key) > MaxRecordSize-9 || len(rec.Value) > MaxRecordSize-9-len(rec.Key) {
		return fmt.Errorf("wal: record exceeds %d bytes", MaxRecordSize)
	}
	return nil
}

func appendLenPrefixed(buf, data []byte) []byte {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	buf = append(buf, lenBuf[:]...)
	return append(buf, data...)
}

// Replay reads every valid record from the WAL file at path, in order. It
// stops -- without returning an error -- at the first record that is
// incomplete or fails its CRC check, since that is exactly the shape a
// crash mid-Append leaves behind: a torn trailing write, not corruption
// that should halt recovery. Any fully-written, CRC-valid record before
// that point is returned; nothing after it is trusted.
func Replay(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: opening %s for replay: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var records []Record
	for {
		rec, ok, err := readOneRecord(r)
		if err != nil {
			return nil, fmt.Errorf("wal: replaying %s: %w", path, err)
		}
		if !ok {
			break
		}
		records = append(records, rec)
	}
	return records, nil
}

// readOneRecord reads one record from r. ok is false (with a nil error)
// when the stream ends cleanly at a record boundary, OR when what follows
// doesn't form a complete, CRC-valid record (the torn-tail case) -- both
// are treated as "no more valid records," never as errors, since Replay's
// whole job is to recover exactly what was durably written and stop there.
func readOneRecord(r *bufio.Reader) (rec Record, ok bool, err error) {
	header := make([]byte, headerLen)
	n, err := io.ReadFull(r, header)
	if err == io.EOF && n == 0 {
		return Record{}, false, nil // clean end of file
	}
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, false, nil
		}
		return Record{}, false, err
	}

	bodyLen := binary.LittleEndian.Uint32(header[0:4])
	if bodyLen > MaxRecordSize {
		return Record{}, false, fmt.Errorf("wal: record length %d exceeds limit", bodyLen)
	}
	wantSum := binary.LittleEndian.Uint32(header[4:8])

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, false, nil
		}
		return Record{}, false, err
	}

	if crc32.ChecksumIEEE(body) != wantSum {
		return Record{}, false, nil // torn/corrupt: bytes present but don't match their own checksum
	}

	rec, err = decodeBody(body)
	if err != nil {
		return Record{}, false, nil // internally inconsistent despite a valid CRC; treat as torn
	}
	return rec, true, nil
}

func decodeBody(body []byte) (Record, error) {
	if len(body) < 1+4 {
		return Record{}, fmt.Errorf("body too short")
	}
	op := OpType(body[0])
	off := 1

	key, off, err := readLenPrefixed(body, off)
	if err != nil {
		return Record{}, err
	}
	value, end, err := readLenPrefixed(body, off)
	if err != nil {
		return Record{}, err
	}
	if end != len(body) {
		return Record{}, fmt.Errorf("trailing record data")
	}
	rec := Record{Op: op, Key: key, Value: value}
	if err := validateRecord(rec); err != nil {
		return Record{}, err
	}
	return rec, nil
}

func readLenPrefixed(body []byte, off int) (data []byte, newOff int, err error) {
	if off+4 > len(body) {
		return nil, 0, fmt.Errorf("truncated length prefix")
	}
	l := binary.LittleEndian.Uint32(body[off : off+4])
	off += 4
	if off+int(l) > len(body) {
		return nil, 0, fmt.Errorf("truncated data")
	}
	return body[off : off+int(l)], off + int(l), nil
}
