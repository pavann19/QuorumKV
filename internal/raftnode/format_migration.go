package raftnode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/memberlock"
	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/wal"
	storagepb "github.com/pavann19/quorumkv/proto/storagepb"
	"go.etcd.io/bbolt"
)

const migrationIncomplete = "MIGRATION_INCOMPLETE"
const migrationReady = "MIGRATION_READY.json"

type StorageProfile struct {
	Snapshot uint32 `json:"snapshot"`
	WAL      uint32 `json:"wal"`
	Command  uint32 `json:"command"`
}
type migrationProof struct {
	WALRecords, Commands, Logs, Snapshots uint64
	WALSHA256, LogSHA256                  string
	SnapshotSHA256                        []string
}
type FormatMigration struct {
	Version               int    `json:"version"`
	Status                string `json:"status"`
	Source, Destination   string
	ApprovalSHA256        string          `json:"approval_sha256"`
	SourceManifest        clusterManifest `json:"source_manifest"`
	TargetManifest        clusterManifest `json:"target_manifest"`
	SourceFiles           []MigrationFile `json:"source_files"`
	TargetFiles           []MigrationFile `json:"target_files,omitempty"`
	Proof                 migrationProof  `json:"proof"`
	RequiredHeadroomBytes uint64          `json:"required_headroom_bytes"`
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

type contextWriter struct {
	ctx context.Context
	w   io.Writer
}

func (w contextWriter) Write(p []byte) (int, error) {
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.w.Write(p)
}
func migrationHeadroom(files []MigrationFile, commands uint64) uint64 {
	var bytes uint64
	for _, file := range files {
		bytes += uint64(file.Bytes)
	}
	return 3*bytes + commands*1024 + (64 << 20)
}
func checkMigrationHeadroom(space diskhealth.Space, probeErr error, required uint64) error {
	if probeErr != nil || space.Total == 0 || space.Available > space.Total {
		return diskhealth.ErrUnknown
	}
	if space.Available < required {
		return diskhealth.ErrPressure
	}
	return nil
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func profileOptions(m clusterManifest) StartOptions {
	return StartOptions{ClusterID: m.ClusterID, StoreLimits: m.StoreLimits, SnapshotFormatVersion: m.SnapshotFormatVersion, WALFormatVersion: m.WALFormatVersion, CommandFormatVersion: m.CommandFormatVersion}
}
func manifestSelf(m clusterManifest) (Peer, error) {
	for _, p := range m.Peers {
		if p.ID == m.NodeID {
			return p, nil
		}
	}
	return Peer{}, errors.New("manifest member is absent from peers")
}
func readMigrationManifest(dir string) (clusterManifest, error) {
	var m clusterManifest
	f, err := os.Open(filepath.Join(dir, "cluster.json"))
	if err != nil {
		return m, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 65537))
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		return m, err
	}
	if d.Decode(new(any)) != io.EOF {
		return m, errors.New("invalid manifest framing")
	}
	self, err := manifestSelf(m)
	if err != nil {
		return m, err
	}
	o, err := validateStart(self, m.Peers, []StartOptions{profileOptions(m)})
	if err != nil {
		return m, err
	}
	if !reflect.DeepEqual(m, desiredManifest(self, m.Peers, o)) {
		return m, errors.New("unsupported/noncanonical manifest")
	}
	return m, nil
}
func selectedWAL(m clusterManifest) wal.Format {
	f := wal.Format{Version: m.WALFormatVersion}
	if f.Version != 0 {
		f.ClusterID = m.ClusterID
	}
	return f
}

func hashRecord(h hash.Hash, r wal.Record) {
	var b [8]byte
	h.Write([]byte{byte(r.Op)})
	binary.LittleEndian.PutUint64(b[:], uint64(len(r.Key)))
	h.Write(b[:])
	h.Write(r.Key)
	binary.LittleEndian.PutUint64(b[:], uint64(len(r.Value)))
	h.Write(b[:])
	h.Write(r.Value)
}

func inspectMigrationWAL(ctx context.Context, dir string, m clusterManifest) (uint64, string, error) {
	path := filepath.Join(dir, "store.wal")
	sizes := map[string]int{}
	var used int64
	var count uint64
	h := sha256.New()
	valid, err := wal.InspectFormat(path, selectedWAL(m), func(r wal.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		key := string(r.Key)
		if previous, ok := sizes[key]; ok {
			used -= int64(len(r.Key) + previous)
		}
		if r.Op == wal.OpPut {
			sizes[key] = len(r.Value)
			used += int64(len(r.Key) + len(r.Value))
		} else {
			delete(sizes, key)
		}
		if used > m.StoreLimits.MaxBytes || used > migrationMaxLogicalBytes || int64(len(sizes)) > m.StoreLimits.MaxEntries || len(sizes) > migrationMaxEntries {
			return errors.New("WAL state exceeds migration capacity ceiling")
		}
		count++
		hashRecord(h, r)
		return nil
	})
	if err != nil {
		return 0, "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, "", err
	}
	if valid != info.Size() {
		return 0, "", errors.New("incomplete WAL tail requires recovery on a separate copy")
	}
	return count, hex.EncodeToString(h.Sum(nil)), nil
}

func normalizedMigrationCommand(log *raft.Log, from, to clusterManifest) ([]byte, error) {
	if len(log.Data) > wal.MaxRecordSize {
		return nil, errors.New("committed command exceeds conversion ceiling")
	}
	c, err := decodeCommandFormat(log.Data, from.ClusterID, from.CommandFormatVersion)
	if err != nil {
		return nil, err
	}
	if c.Limits == (store.Limits{}) {
		c.Limits = store.DefaultLimits()
	}
	if c.Limits != from.StoreLimits {
		return nil, errors.New("command capacity differs from manifest")
	}
	if c.Op != opPut && c.Op != opDelete {
		return nil, errors.New("unknown committed operation")
	}
	if c.Op == opDelete && len(c.Value) != 0 {
		return nil, errors.New("legacy delete payload requires manual review")
	}
	return encodeCommandFormat(c, to.ClusterID, to.CommandFormatVersion)
}

func inspectMigrationLogs(ctx context.Context, dir string, from, to clusterManifest, rewrite bool) (uint64, uint64, string, error) {
	path := filepath.Join(dir, "raft-log.bolt")
	if err := inspectBoltLayout(path); err != nil {
		return 0, 0, "", err
	}
	// Bound raw records before the adopted MessagePack decoder can allocate.
	db, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return 0, 0, "", err
	}
	err = db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte("logs")).ForEach(func(k, v []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if len(k) != 8 || binary.BigEndian.Uint64(k) == 0 || v == nil || len(v) > wal.MaxRecordSize+(64<<10) {
				return errors.New("invalid/oversized Raft log record")
			}
			return nil
		})
	})
	closeErr := db.Close()
	if err = errors.Join(err, closeErr); err != nil {
		return 0, 0, "", err
	}
	logs, err := raftboltdb.New(raftboltdb.Options{Path: path, BoltOptions: &bbolt.Options{ReadOnly: !rewrite, Timeout: time.Second}})
	if err != nil {
		return 0, 0, "", err
	}
	defer logs.Close()
	first, err := logs.FirstIndex()
	if err != nil {
		return 0, 0, "", err
	}
	last, err := logs.LastIndex()
	if err != nil {
		return 0, 0, "", err
	}
	h := sha256.New()
	var count, commands uint64
	for index := first; index != 0 && index <= last; index++ {
		if err := ctx.Err(); err != nil {
			return 0, 0, "", err
		}
		var entry raft.Log
		if err = logs.GetLog(index, &entry); err != nil {
			return 0, 0, "", err
		}
		if entry.Index != index {
			return 0, 0, "", errors.New("log index mismatch")
		}
		if entry.Type == raft.LogCommand {
			entry.Data, err = normalizedMigrationCommand(&entry, from, to)
			if err != nil {
				return 0, 0, "", err
			}
			commands++
			if rewrite {
				if err = logs.StoreLog(&entry); err != nil {
					return 0, 0, "", err
				}
			}
		}
		if entry.Type == raft.LogCommand {
			c, e := decodeCommandFormat(entry.Data, to.ClusterID, to.CommandFormatVersion)
			if e != nil {
				return 0, 0, "", e
			}
			if len(c.Value) == 0 {
				c.Value = nil
			}
			if len(c.Key) == 0 {
				c.Key = nil
			}
			entry.Data, e = json.Marshal(c)
			if e != nil {
				return 0, 0, "", e
			}
		}
		entry.AppendedAt = entry.AppendedAt.UTC()
		raw, err := json.Marshal(entry)
		if err != nil {
			return 0, 0, "", err
		}
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], uint64(len(raw)))
		h.Write(b[:])
		h.Write(raw)
		count++
		if index == last {
			break
		}
	}
	return count, commands, hex.EncodeToString(h.Sum(nil)), nil
}

type migrationSnapshotMeta struct {
	raft.SnapshotMeta
	CRC []byte
}

func snapshotForMigration(ctx context.Context, dir string, meta *raft.SnapshotMeta, from clusterManifest) ([]store.Entry, uint64, error) {
	if meta.Size < 0 || meta.Size > migrationMaxSnapshotBytes {
		return nil, 0, errors.New("snapshot exceeds conversion ceiling")
	}
	f, err := os.Open(filepath.Join(dir, "snapshots", meta.ID, "state.bin"))
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	var entries []store.Entry
	var index uint64
	if from.SnapshotFormatVersion == 1 {
		var magic [8]byte
		if _, err = io.ReadFull(contextReader{ctx, f}, magic[:]); err != nil {
			return nil, 0, err
		}
		if !bytes.Equal(magic[:], snapshotMagic) {
			return nil, 0, ErrSnapshotFormat
		}
		var header storagepb.SnapshotHeader
		if err = readSnapshotFrame(contextReader{ctx, f}, &header, maxSnapshotHeader); err != nil {
			return nil, 0, err
		}
		if header.EntryCount > migrationMaxEntries || header.LogicalBytes > migrationMaxLogicalBytes {
			return nil, 0, errors.New("snapshot header exceeds migration ceiling")
		}
		if _, err = f.Seek(0, io.SeekStart); err != nil {
			return nil, 0, err
		}
		entries, index, err = decodeSnapshotV1(contextReader{ctx, f}, from.ClusterID, from.StoreLimits)
	} else {
		entries, err = decodeMigrationLegacySnapshot(contextReader{ctx, f}, meta.Size)
	}
	if err != nil {
		return nil, 0, err
	}
	if index > meta.Index {
		return nil, 0, errors.New("snapshot command index exceeds Raft snapshot index")
	}
	seen := map[string]bool{}
	var used int64
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
		key := string(entry.Key)
		if seen[key] {
			return nil, 0, errors.New("duplicate snapshot key")
		}
		seen[key] = true
		if len(entry.Key)+len(entry.Value) > wal.MaxRecordSize-9 {
			return nil, 0, errors.New("snapshot entry exceeds WAL bound")
		}
		used += int64(len(entry.Key) + len(entry.Value))
		if used > from.StoreLimits.MaxBytes || used > migrationMaxLogicalBytes || len(seen) > migrationMaxEntries || int64(len(seen)) > from.StoreLimits.MaxEntries {
			return nil, 0, errors.New("snapshot exceeds migration state ceiling")
		}
	}
	return entries, index, nil
}

func inspectMigrationSnapshots(ctx context.Context, source, destination string, from, to clusterManifest) ([]string, error) {
	snapshots, err := inspectSnapshotsContext(ctx, source)
	if err != nil {
		return nil, err
	}
	var proofs []string
	for _, meta := range snapshots.metas {
		entries, index, err := snapshotForMigration(ctx, source, meta, from)
		if err != nil {
			return nil, err
		}
		if to.CommandFormatVersion == 1 {
			for _, entry := range entries {
				if !validCommandV1(command{Op: opPut, Key: entry.Key, Value: entry.Value, Limits: to.StoreLimits}, to.ClusterID) {
					return nil, errors.New("snapshot state is outside versioned command payload bounds; operator review required")
				}
			}
		}
		if to.SnapshotFormatVersion == 0 {
			index = 0
		}
		h := sha256.New()
		for _, entry := range entries {
			hashRecord(h, wal.Record{Op: wal.OpPut, Key: entry.Key, Value: entry.Value})
		}
		proofs = append(proofs, fmt.Sprintf("%s:%d:%d:%x", meta.ID, meta.Index, index, h.Sum(nil)))
		// Even unchanged snapshots have their metadata validated strictly, so
		// future fields are never silently erased by an older converter.
		metadata, err := os.Open(filepath.Join(source, "snapshots", meta.ID, "meta.json"))
		if err != nil {
			return nil, err
		}
		var full migrationSnapshotMeta
		decoder := json.NewDecoder(io.LimitReader(metadata, 1<<20))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(&full)
		if err == nil && decoder.Decode(new(any)) != io.EOF {
			err = errors.New("invalid snapshot metadata")
		}
		metadata.Close()
		if err != nil {
			return nil, err
		}
		if destination != "" {
			base := filepath.Join(destination, "snapshots", meta.ID)
			if err = wal.MakeDurableDir(base, 0700); err != nil {
				return nil, err
			}
			state, err := os.OpenFile(filepath.Join(base, "state.bin"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				return nil, err
			}
			crc := crc64.New(crc64.MakeTable(crc64.ECMA))
			writer := contextWriter{ctx, io.MultiWriter(state, crc)}
			if to.SnapshotFormatVersion == 1 {
				err = encodeSnapshotV1(writer, to.ClusterID, to.StoreLimits, index, entries)
			} else {
				err = gob.NewEncoder(writer).Encode(entries)
			}
			if err == nil {
				err = state.Sync()
			}
			info, statErr := state.Stat()
			err = errors.Join(err, statErr, state.Close())
			if err != nil {
				return nil, err
			}
			full.Size = info.Size()
			full.CRC = crc.Sum(nil)
			raw, err := json.Marshal(full)
			if err != nil {
				return nil, err
			}
			if err = writeMigrationFile(filepath.Join(base, "meta.json"), raw); err != nil {
				return nil, err
			}
			if err = wal.SyncDirectory(base); err != nil {
				return nil, err
			}
		}
	}
	return proofs, nil
}

func inspectMemberConversion(ctx context.Context, dir string, from, to clusterManifest) (migrationProof, error) {
	var p migrationProof
	var err error
	p.WALRecords, p.WALSHA256, err = inspectMigrationWAL(ctx, dir, from)
	if err != nil {
		return p, err
	}
	p.Logs, p.Commands, p.LogSHA256, err = inspectMigrationLogs(ctx, dir, from, to, false)
	if err != nil {
		return p, err
	}
	p.SnapshotSHA256, err = inspectMigrationSnapshots(ctx, dir, "", from, to)
	p.Snapshots = uint64(len(p.SnapshotSHA256))
	if err != nil {
		return p, err
	}
	logs, err := raftboltdb.New(raftboltdb.Options{Path: filepath.Join(dir, "raft-log.bolt"), BoltOptions: &bbolt.Options{ReadOnly: true, Timeout: time.Second}})
	if err != nil {
		return p, err
	}
	defer logs.Close()
	snaps, err := inspectSnapshotsContext(ctx, dir)
	if err != nil {
		return p, err
	}
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(from.NodeID)
	cfg.LogOutput = io.Discard
	_, transport := raft.NewInmemTransport("conversion-inspection")
	defer transport.Close()
	actual, err := inspectConfiguration(cfg, logs, logs, snaps, transport)
	if err != nil {
		return p, err
	}
	if !sameConfiguration(actual, from.Peers) {
		return p, errors.New("persisted membership differs from manifest")
	}
	return p, nil
}

func writeMigrationFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}
func underPath(child, parent string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && (rel == "." || rel != ".." && !strings.HasPrefix(rel, ".."+string(os.PathSeparator)))
}

// ConvertMemberFormat never edits the source's data or overwrites a destination.
// Empty approval inspects only. Approved conversion reserves a new directory,
// holds both member locks and leaves an incomplete marker on every failure.
func ConvertMemberFormat(ctx context.Context, source, destination string, target StorageProfile, approval string) (FormatMigration, error) {
	return convertMemberFormat(ctx, source, destination, target, approval, nil)
}
func convertMemberFormat(ctx context.Context, source, destination string, target StorageProfile, approval string, beforePublish func() error) (report FormatMigration, resultErr error) {
	defer func() {
		if recover() != nil {
			resultErr = errors.New("offline conversion rejected malformed persisted state")
		}
		if resultErr != nil && report.Status == "CONVERTED_COPY_VERIFIED_SOURCE_UNCHANGED" {
			report.Status = "INCOMPLETE_OR_PUBLICATION_UNCERTAIN"
		}
	}()
	if err := ctx.Err(); err != nil {
		return report, err
	}
	source, err := filepath.Abs(source)
	if err != nil {
		return report, err
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return report, err
	}
	info, err := os.Lstat(source)
	if err != nil {
		return report, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return report, errors.New("source must be an ordinary offline member directory")
	}
	source, err = filepath.EvalSymlinks(source)
	if err != nil {
		return report, err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(destination))
	if err != nil {
		return report, err
	}
	destination = filepath.Join(parent, filepath.Base(destination))
	if underPath(destination, source) || underPath(source, destination) {
		return report, errors.New("source and destination must be disjoint")
	}
	if _, err = os.Lstat(destination); !errors.Is(err, os.ErrNotExist) {
		return report, errors.New("destination must not exist")
	}
	sourceLock, err := memberlock.Acquire(source)
	if err != nil {
		return report, err
	}
	defer sourceLock.Close()
	if _, err = os.Lstat(filepath.Join(source, migrationIncomplete)); !errors.Is(err, os.ErrNotExist) {
		return report, errors.New("source is incomplete or cannot be inspected")
	}
	if err = checkFatal(source, false); err != nil {
		return report, err
	}
	from, err := readMigrationManifest(source)
	if err != nil {
		return report, err
	}
	self, err := manifestSelf(from)
	if err != nil {
		return report, err
	}
	options := profileOptions(from)
	options.SnapshotFormatVersion = target.Snapshot
	options.WALFormatVersion = target.WAL
	options.CommandFormatVersion = target.Command
	options, err = validateStart(self, from.Peers, []StartOptions{options})
	if err != nil {
		return report, err
	}
	to := desiredManifest(self, from.Peers, options)
	if reflect.DeepEqual(from, to) {
		return report, errors.New("source already has requested format")
	}
	files, err := migrationFilesContext(ctx, source)
	if err != nil {
		return report, err
	}
	for _, file := range files {
		if strings.HasPrefix(file.Path, "snapshots/") && strings.HasSuffix(file.Path, "/state.bin") && file.Bytes > migrationMaxSnapshotBytes {
			return report, errors.New("snapshot file exceeds migration ceiling")
		}
	}
	proof, err := inspectMemberConversion(ctx, source, from, to)
	if err != nil {
		return report, err
	}
	report = FormatMigration{Version: 1, Status: "INSPECTED_DESTINATION_NOT_CREATED", Source: source, Destination: destination, SourceManifest: from, TargetManifest: to, SourceFiles: files, Proof: proof}
	report.RequiredHeadroomBytes = migrationHeadroom(files, proof.Commands)
	binding, err := json.Marshal(report)
	if err != nil {
		return report, err
	}
	digest := sha256.Sum256(binding)
	report.ApprovalSHA256 = hex.EncodeToString(digest[:])
	if approval == "" {
		return report, nil
	}
	if approval != report.ApprovalSHA256 {
		return report, errors.New("approval does not match source, destination and format plan")
	}
	if err = ctx.Err(); err != nil {
		return report, err
	}
	space, probeErr := diskhealth.Probe(parent)
	if err = checkMigrationHeadroom(space, probeErr, report.RequiredHeadroomBytes); err != nil {
		return report, err
	}
	if err = os.Mkdir(destination, 0700); err != nil {
		return report, err
	}
	report.Status = "INCOMPLETE_DO_NOT_START"
	targetLock, err := memberlock.Acquire(destination)
	if err != nil {
		return report, err
	}
	defer targetLock.Close()
	if err = writeMigrationFile(filepath.Join(destination, migrationIncomplete), []byte(report.ApprovalSHA256+"\n")); err != nil {
		return report, err
	}
	if err = writeMigrationFile(filepath.Join(destination, "cluster.json"), []byte(`{"version":2147483647}`)); err != nil {
		return report, err
	}
	if err = wal.SyncDirectory(destination); err != nil {
		return report, err
	}
	for _, file := range files {
		if err = ctx.Err(); err != nil {
			return report, err
		}
		if file.Path == "cluster.json" || file.Path == "store.wal" || strings.HasPrefix(file.Path, "snapshots/") && (strings.HasSuffix(file.Path, "/state.bin") || strings.HasSuffix(file.Path, "/meta.json")) {
			continue
		}
		name := file.Path
		if name == migrationReady {
			name = "MIGRATION_HISTORY." + file.SHA256 + ".json"
		}
		path := filepath.Join(destination, filepath.FromSlash(name))
		if err = wal.MakeDurableDir(filepath.Dir(path), 0700); err != nil {
			return report, err
		}
		input, err := os.Open(filepath.Join(source, filepath.FromSlash(file.Path)))
		if err != nil {
			return report, err
		}
		output, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			input.Close()
			return report, err
		}
		_, err = io.Copy(output, contextReader{ctx, input})
		if err == nil {
			err = output.Sync()
		}
		err = errors.Join(err, input.Close(), output.Close())
		if err != nil {
			return report, err
		}
		if err = wal.SyncDirectory(filepath.Dir(path)); err != nil {
			return report, err
		}
	}
	w, err := wal.OpenReplayFormat(filepath.Join(destination, "store.wal"), selectedWAL(to), nil)
	if err != nil {
		return report, err
	}
	batch := []wal.Record{}
	size := 0
	flush := func() error { err := w.AppendBatch(batch); batch = nil; size = 0; return err }
	_, err = wal.InspectFormat(filepath.Join(source, "store.wal"), selectedWAL(from), func(r wal.Record) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n := wal.EncodedRecordSize(r)
		if len(batch) == wal.MaxBatchRecords || n > wal.MaxBatchBytes-size {
			if err := flush(); err != nil {
				return err
			}
		}
		batch = append(batch, r)
		size += n
		return nil
	})
	if err == nil {
		err = flush()
	}
	err = errors.Join(err, w.Close())
	if err != nil {
		return report, err
	}
	if _, _, _, err = inspectMigrationLogs(ctx, destination, from, to, true); err != nil {
		return report, err
	}
	if _, err = inspectMigrationSnapshots(ctx, source, destination, from, to); err != nil {
		return report, err
	}
	manifestBytes, err := json.Marshal(to)
	if err != nil {
		return report, err
	}
	verified, err := inspectMemberConversion(ctx, destination, to, to)
	if err != nil {
		return report, err
	}
	if !reflect.DeepEqual(proof, verified) {
		return report, errors.New("converted semantic proof differs from inspected source")
	}
	if beforePublish != nil {
		if err = beforePublish(); err != nil {
			return report, err
		}
	}
	again, err := migrationFilesContext(ctx, source)
	if err != nil {
		return report, err
	}
	if !reflect.DeepEqual(files, again) {
		return report, errors.New("source changed during conversion")
	}
	outputFiles, err := migrationFilesContext(ctx, destination)
	if err != nil {
		return report, err
	}
	for _, file := range outputFiles {
		if file.Path != migrationIncomplete {
			if file.Path == "cluster.json" {
				file.Bytes = int64(len(manifestBytes))
				file.SHA256 = fmt.Sprintf("%x", sha256.Sum256(manifestBytes))
			}
			report.TargetFiles = append(report.TargetFiles, file)
		}
	}
	report.Status = "CONVERTED_COPY_VERIFIED_SOURCE_UNCHANGED"
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, err
	}
	if err = writeMigrationFile(filepath.Join(destination, migrationReady), raw); err != nil {
		return report, err
	}
	if err = wal.SyncDirectory(destination); err != nil {
		return report, err
	}
	if err = ctx.Err(); err != nil {
		return report, err
	}
	temporary, err := os.CreateTemp(destination, "manifest.*.tmp")
	if err != nil {
		return report, err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	n, err := temporary.Write(manifestBytes)
	if err == nil && n != len(manifestBytes) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = temporary.Sync()
	}
	err = errors.Join(err, temporary.Close())
	if err != nil {
		return report, err
	}
	if err = ctx.Err(); err != nil {
		return report, err
	}
	if err = os.Rename(temporaryName, filepath.Join(destination, "cluster.json")); err != nil {
		return report, err
	}
	if err = wal.SyncDirectory(destination); err != nil {
		return report, err
	}
	if err = ctx.Err(); err != nil {
		return report, err
	}
	if err = os.Remove(filepath.Join(destination, migrationIncomplete)); err != nil {
		return report, err
	}
	if err = wal.SyncDirectory(destination); err != nil {
		return report, err
	}
	if err = wal.SyncDirectory(parent); err != nil {
		return report, err
	}
	return report, nil
}
