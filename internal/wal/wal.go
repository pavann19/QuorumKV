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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
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
// an incomplete trailing append. Complete records with invalid CRCs or
// structure fail recovery: they may contain acknowledged state.
const headerLen = 4 + 4 // recordLen + CRC32

// ErrCorrupt means recovery cannot safely infer the acknowledged state. Preserve
// the original file and recover from a verified backup; never silently truncate.
var ErrCorrupt = errors.New("wal: corrupt record")

// MaxRecordSize bounds allocations when reading untrusted on-disk lengths.
const MaxRecordSize = 16 << 20

// WAL is an append-only log file. All writes go through Append, which
// fsyncs before returning -- the entire point of a WAL is that Append
// returning nil is a durability promise a caller (e.g. a KV store's Put)
// can rely on even across a hard crash immediately afterward.
type WAL struct {
	path          string
	f             durableFile
	failed        error
	metrics       Counters
	syncParent    func(string) error
	fileHeader    []byte
	crcSeed       uint32
	formatVersion uint32
}

// The interface permits deterministic disk-failure fixtures without filling a
// real/shared volume. Production always uses an ordinary os.File.
type durableFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

// Open opens (creating if necessary) the WAL file at path for appending.
func Open(path string) (*WAL, error) {
	return openWithSync(path, syncDir)
}

// OpenReplay streams validated records once before accepting appends. A visitor
// error refuses open without tail repair; partial visitor state must be discarded.
// Each record owns its backing bytes and may be retained by the visitor.
func OpenReplay(path string, visit func(Record) error) (*WAL, error) {
	return openReplay(path, syncDir, visit)
}

func openWithSync(path string, syncParent func(string) error) (*WAL, error) {
	return openReplay(path, syncParent, nil)
}

func openReplay(path string, syncParent func(string) error, visit func(Record) error) (*WAL, error) {
	return openReplayFormat(path, syncParent, Format{}, visit)
}

func openReplayFormat(path string, syncParent func(string) error, format Format, visit func(Record) error) (*WAL, error) {
	if err := format.Validate(); err != nil {
		return nil, err
	}
	var f *os.File
	var err error
	var fileHeader []byte
	var seed uint32
	if format.Version == 0 {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	} else {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
		if errors.Is(err, os.ErrExist) {
			f, err = os.OpenFile(path, os.O_RDWR, 0)
			if err == nil {
				fileHeader, seed, err = readFileHeader(f, format)
			}
		} else if err == nil {
			fileHeader, seed, err = makeFileHeader(format)
			if err == nil {
				var n int
				n, err = f.Write(fileHeader)
				if err == nil && n != len(fileHeader) {
					err = io.ErrShortWrite
				}
			}
		}
	}
	if err != nil {
		if f != nil {
			f.Close()
		}
		return nil, fmt.Errorf("wal: opening %s: %w", path, err)
	}
	// Remove an incomplete suffix before accepting any new acknowledged writes.
	reader := bufio.NewReader(f)
	valid := int64(len(fileHeader))
	for {
		rec, ok, scanErr := readOneRecordVersion(reader, seed, format.Version)
		if scanErr != nil {
			f.Close()
			return nil, scanErr
		}
		if !ok {
			break
		}
		if visit != nil {
			if err := visit(rec); err != nil {
				f.Close()
				return nil, fmt.Errorf("wal: replay visitor: %w", err)
			}
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
	// Persist the file before its directory entry, including an empty new WAL.
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: syncing opened file: %w", err)
	}
	if err := syncParent(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: syncing parent directory: %w", err)
	}
	return &WAL{path: path, f: f, syncParent: syncParent, fileHeader: fileHeader, crcSeed: seed, formatVersion: format.Version}, nil
}

// Close closes the underlying file. It does not fsync -- callers that need
// a final durability guarantee should have already gotten it from Append.
func (w *WAL) Close() error {
	return w.f.Close()
}

// Append writes rec to the log and fsyncs before returning. A nil error is
// a durability guarantee: rec will be present after Replay even if the
// process is killed the instant Append returns.
func (w *WAL) Append(rec Record) error { return w.AppendBatch([]Record{rec}) }

// Batches are bounded independently of keyspace size and retain legacy record
// framing. A failed batch may leave an uncertain complete prefix, never an ack.
const MaxBatchRecords = 16
const MaxBatchBytes = 32 << 20

// EncodedRecordSize reports the legacy framed size of a validated record.
func EncodedRecordSize(record Record) int { return headerLen + 9 + len(record.Key) + len(record.Value) }

// ValidateBatch refuses invalid/oversized input before any file effect.
func ValidateBatch(records []Record) error {
	return validateBatchVersion(records, 0)
}
func validateBatchVersion(records []Record, version uint32) error {
	if len(records) > MaxBatchRecords {
		return fmt.Errorf("wal: batch exceeds %d records", MaxBatchRecords)
	}
	total := 0
	for _, rec := range records {
		if err := validateRecordVersion(rec, version); err != nil {
			return err
		}
		size := EncodedRecordSize(rec)
		if size > MaxBatchBytes-total {
			return fmt.Errorf("wal: batch exceeds %d bytes", MaxBatchBytes)
		}
		total += size
	}
	return nil
}

// AppendBatch writes every accepted frame once, then fsyncs once. No caller may
// acknowledge any member before this method returns nil. This is not a multi-key
// atomicity/rollback format: recovery can retain an unacknowledged prefix.
func (w *WAL) AppendBatch(records []Record) (err error) {
	if w.failed != nil {
		return fmt.Errorf("wal: unavailable after I/O failure: %w", w.failed)
	}
	if err := validateBatchVersion(records, w.formatVersion); err != nil {
		return err
	}
	if len(records) == 0 {
		return nil
	}
	defer func() {
		if err != nil {
			w.metrics.Failures.Add(1)
			w.failed = err
		}
	}()
	total := 0
	for _, rec := range records {
		total += EncodedRecordSize(rec)
	}
	buffer := make([]byte, total)
	offset := 0
	for _, rec := range records {
		body := encodeBody(rec)
		binary.LittleEndian.PutUint32(buffer[offset:offset+4], uint32(len(body)))
		binary.LittleEndian.PutUint32(buffer[offset+4:offset+8], recordChecksum(w.crcSeed, body))
		copy(buffer[offset+headerLen:], body)
		offset += headerLen + len(body)
	}
	if n, err := w.f.Write(buffer); err != nil {
		return fmt.Errorf("wal: writing batch: %w", err)
	} else if n != len(buffer) {
		return io.ErrShortWrite
	}
	start := time.Now()
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: fsync batch: %w", err)
	}
	w.metrics.SyncNanos.Add(uint64(time.Since(start)))
	w.metrics.Syncs.Add(1)
	w.metrics.Appends.Add(uint64(len(records)))
	w.metrics.Bytes.Add(uint64(len(buffer)))
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
		if err := validateRecordVersion(rec, w.formatVersion); err != nil {
			return err
		}
	}
	defer func() {
		if err != nil {
			w.failed = err
		}
	}()
	tmp, err := os.CreateTemp(filepath.Dir(w.path), filepath.Base(w.path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("wal: creating temp file for reset: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if len(w.fileHeader) > 0 {
		if n, err := tmp.Write(w.fileHeader); err != nil || n != len(w.fileHeader) {
			tmp.Close()
			if err == nil {
				err = io.ErrShortWrite
			}
			return err
		}
	}
	for _, rec := range records {
		body := encodeBody(rec)
		sum := recordChecksum(w.crcSeed, body)
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
	if err := replaceWAL(tmpPath, w.path); err != nil {
		return fmt.Errorf("wal: renaming reset file into place: %w", err)
	}

	if err := w.syncParent(filepath.Dir(w.path)); err != nil {
		return fmt.Errorf("wal: syncing reset directory: %w", err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_APPEND, 0o644)
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
	return validateRecordVersion(rec, 0)
}
func validateRecordVersion(rec Record, version uint32) error {
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
// stops at an incomplete trailing append. Complete records with checksum or
// structural damage return an error, rather than silently dropping potentially
// acknowledged state. Replay never modifies the file.
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
// when the stream ends cleanly or with an incomplete trailing append. Complete
// malformed records are corruption, not proof of an unacknowledged write.
func readOneRecord(r *bufio.Reader) (rec Record, ok bool, err error) {
	return readOneRecordSeed(r, 0)
}
func readOneRecordSeed(r *bufio.Reader, seed uint32) (rec Record, ok bool, err error) {
	return readOneRecordVersion(r, seed, 0)
}
func readOneRecordVersion(r *bufio.Reader, seed uint32, version uint32) (rec Record, ok bool, err error) {
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
	if bodyLen < 9 || bodyLen > MaxRecordSize {
		return Record{}, false, fmt.Errorf("%w: invalid record length %d", ErrCorrupt, bodyLen)
	}
	wantSum := binary.LittleEndian.Uint32(header[4:8])

	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return Record{}, false, nil
		}
		return Record{}, false, err
	}

	if recordChecksum(seed, body) != wantSum {
		return Record{}, false, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}

	rec, err = decodeBodyVersion(body, version)
	if err != nil {
		return Record{}, false, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	return rec, true, nil
}

func decodeBody(body []byte) (Record, error) {
	return decodeBodyVersion(body, 0)
}
func decodeBodyVersion(body []byte, version uint32) (Record, error) {
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
	if err := validateRecordVersion(rec, version); err != nil {
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

// Inspect streams an existing WAL without creating it, repairing its tail or
// acquiring an append handle. A visitor must discard partial state on error.
// validBytes excludes an incomplete suffix; callers may compare the file size.
func Inspect(path string, visit func(Record) error) (validBytes int64, err error) {
	return InspectFormat(path, Format{}, visit)
}

// InspectFormat is read-only and includes the validated file header in validBytes.
func InspectFormat(path string, format Format, visit func(Record) error) (validBytes int64, err error) {
	if err := format.Validate(); err != nil {
		return 0, err
	}
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	header, seed, err := readFileHeader(reader, format)
	if err != nil {
		return 0, err
	}
	validBytes = int64(len(header))
	for {
		rec, ok, err := readOneRecordVersion(reader, seed, format.Version)
		if err != nil {
			return validBytes, err
		}
		if !ok {
			return validBytes, nil
		}
		if visit != nil {
			if err := visit(rec); err != nil {
				return validBytes, err
			}
		}
		validBytes += int64(headerLen + 9 + len(rec.Key) + len(rec.Value))
	}
}
