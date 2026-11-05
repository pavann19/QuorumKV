package raftnode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/pavann19/quorumkv/internal/memberlock"
	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/wal"
	"go.etcd.io/bbolt"
)

type MigrationFile struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}
type IdentityMigration struct {
	Version      int             `json:"version"`
	Status       string          `json:"status"`
	SourceSHA256 string          `json:"source_sha256"`
	Files        []MigrationFile `json:"files"`
	Manifest     clusterManifest `json:"manifest"`
}

// LegacyIdentityMigration only assigns identity to an offline, manifest-free
// existing member. Empty approval performs inspection; applying requires the
// exact fingerprint of both the source evidence and desired identity/config.
// It starts no consensus worker, opens Bolt read-only and preserves data files.
func LegacyIdentityMigration(self Peer, peers []Peer, dir string, options StartOptions, approveSHA256 string) (IdentityMigration, error) {
	var report IdentityMigration
	options, err := validateStart(self, peers, []StartOptions{options})
	if err != nil {
		return report, err
	}
	if options.Bootstrap || options.ClearFatal {
		return report, errors.New("migration cannot bootstrap or clear fatal incidents")
	}
	if options.WALFormatVersion != 0 || options.CommandFormatVersion != 0 {
		return report, errors.New("legacy identity migration cannot authorize a WAL format upgrade")
	}
	if options.StoreLimits != store.DefaultLimits() {
		return report, errors.New("legacy identity migration requires default legacy command limits")
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return report, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return report, errors.New("migration requires an existing ordinary member directory")
	}
	lock, err := memberlock.Acquire(dir)
	if err != nil {
		return report, err
	}
	defer lock.Close()
	if err = checkFatal(dir, false); err != nil {
		return report, err
	}
	if _, err = os.Lstat(filepath.Join(dir, "cluster.json")); err == nil {
		return report, errors.New("manifest already exists; identity reassignment is forbidden")
	} else if !errors.Is(err, os.ErrNotExist) {
		return report, err
	}
	files, err := migrationFiles(dir)
	if err != nil {
		return report, err
	}
	walPath := filepath.Join(dir, "store.wal")
	valid, err := wal.Inspect(walPath, nil)
	if err != nil {
		return report, fmt.Errorf("migration WAL integrity: %w", err)
	}
	walInfo, err := os.Stat(walPath)
	if err != nil {
		return report, err
	}
	if valid != walInfo.Size() {
		return report, errors.New("incomplete WAL tail: preserve and resolve on a recovery copy before migration")
	}
	logPath := filepath.Join(dir, "raft-log.bolt")
	if _, err = os.Stat(logPath); err != nil {
		return report, err
	}
	if err = inspectBoltLayout(logPath); err != nil {
		return report, err
	}
	logs, err := raftboltdb.New(raftboltdb.Options{Path: logPath, BoltOptions: &bbolt.Options{ReadOnly: true, Timeout: time.Second}})
	if err != nil {
		return report, err
	}
	defer logs.Close()
	snaps, err := inspectSnapshots(dir)
	if err != nil {
		return report, err
	}
	if options.SnapshotFormatVersion != 0 {
		return report, errors.New("legacy identity migration cannot authorize a snapshot format upgrade")
	}
	for _, meta := range snaps.metas {
		file, err := os.Open(filepath.Join(dir, "snapshots", meta.ID, "state.bin"))
		if err != nil {
			return report, err
		}
		prefix := make([]byte, len(snapshotMagic))
		n, readErr := io.ReadFull(file, prefix)
		file.Close()
		if readErr != nil && readErr != io.EOF && readErr != io.ErrUnexpectedEOF {
			return report, readErr
		}
		if n >= 6 && bytes.Equal(prefix[:6], snapshotMagic[:6]) {
			return report, errors.New("versioned snapshot is not eligible for legacy identity assignment")
		}
	}
	hasState, err := raft.HasExistingState(logs, logs, snaps)
	if err != nil {
		return report, err
	}
	if !hasState {
		return report, errors.New("no existing Raft state; migration does not initialize clusters")
	}
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(self.ID)
	cfg.NoSnapshotRestoreOnStart = true
	cfg.LogOutput = io.Discard
	_, transport := raft.NewInmemTransport(raft.ServerAddress(self.Addr))
	defer transport.Close()
	actual, err := inspectConfiguration(cfg, logs, logs, snaps, transport)
	if err != nil {
		return report, err
	}
	if !sameConfiguration(actual, peers) {
		return report, errors.New("migration peers disagree with persisted Raft membership")
	}
	report = IdentityMigration{Version: 1, Status: "INSPECTED_NOT_APPLIED", Files: files, Manifest: desiredManifest(self, peers, options)}
	raw, err := json.Marshal(struct {
		Files    []MigrationFile
		Manifest clusterManifest
	}{files, report.Manifest})
	if err != nil {
		return report, err
	}
	digest := sha256.Sum256(raw)
	report.SourceSHA256 = hex.EncodeToString(digest[:])
	if approveSHA256 == "" {
		return report, nil
	}
	if approveSHA256 != report.SourceSHA256 {
		return report, errors.New("migration approval fingerprint does not match source and requested identity")
	}
	again, err := migrationFiles(dir)
	if err != nil {
		return report, err
	}
	a, _ := json.Marshal(files)
	b, _ := json.Marshal(again)
	if !bytes.Equal(a, b) {
		return report, errors.New("source changed during migration inspection")
	}
	// The existing-state guard remains intact in ordinary startup. Only this
	// separately fingerprint-approved offline operation may create its manifest.
	if err = persistManifest(dir, self, peers, options, false); err != nil {
		return report, err
	}
	report.Status = "APPLIED_IDENTITY_ONLY"
	return report, nil
}

func migrationFiles(dir string) ([]MigrationFile, error) {
	return migrationFilesContext(context.Background(), dir)
}
func migrationFilesContext(ctx context.Context, dir string) ([]MigrationFile, error) {
	var files []MigrationFile
	var total int64
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		if path == dir {
			return nil
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "member.lock" {
			return nil
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("migration refuses symlinks/reparse links")
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("migration requires regular evidence files")
		}
		if len(files) >= 10000 || info.Size() > 64<<30-total {
			return errors.New("migration evidence exceeds 10000 files or 64 GiB")
		}
		total += info.Size()
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		h := sha256.New()
		n, copyErr := io.Copy(h, contextReader{ctx, f})
		closeErr := f.Close()
		if err = errors.Join(copyErr, closeErr); err != nil {
			return err
		}
		if n != info.Size() {
			return errors.New("source file changed while hashing")
		}
		files = append(files, MigrationFile{filepath.ToSlash(rel), n, hex.EncodeToString(h.Sum(nil))})
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, err
}

type offlineSnapshots struct{ metas []*raft.SnapshotMeta }

func (s *offlineSnapshots) Create(raft.SnapshotVersion, uint64, uint64, raft.Configuration, uint64, raft.Transport) (raft.SnapshotSink, error) {
	return nil, errors.New("offline inspection cannot create snapshots")
}
func (s *offlineSnapshots) List() ([]*raft.SnapshotMeta, error) { return s.metas, nil }
func (s *offlineSnapshots) Open(string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	return nil, nil, errors.New("offline membership inspection does not restore snapshots")
}

// The ordinary FileSnapshotStore constructor writes a permissions-test file.
// Offline inspection instead validates the adopted v1 snapshot metadata/CRC
// directly without opening that constructor or rewriting snapshot state.
func inspectSnapshots(dir string) (*offlineSnapshots, error) {
	return inspectSnapshotsContext(context.Background(), dir)
}
func inspectSnapshotsContext(ctx context.Context, dir string) (*offlineSnapshots, error) {
	result := &offlineSnapshots{}
	entries, err := os.ReadDir(filepath.Join(dir, "snapshots"))
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".tmp") {
			return nil, errors.New("incomplete snapshot directory requires recovery review")
		}
		base := filepath.Join(dir, "snapshots", entry.Name())
		f, err := os.Open(filepath.Join(base, "meta.json"))
		if err != nil {
			return nil, err
		}
		var meta struct {
			raft.SnapshotMeta
			CRC []byte
		}
		d := json.NewDecoder(io.LimitReader(f, 1<<20))
		err = d.Decode(&meta)
		if err == nil && d.Decode(new(any)) != io.EOF {
			err = errors.New("invalid snapshot metadata")
		}
		f.Close()
		if err != nil {
			return nil, err
		}
		if meta.Version != 1 || meta.ID != entry.Name() || meta.Size < 0 {
			return nil, errors.New("unsupported or mismatched snapshot metadata")
		}
		state, err := os.Open(filepath.Join(base, "state.bin"))
		if err != nil {
			return nil, err
		}
		crc := crc64.New(crc64.MakeTable(crc64.ECMA))
		size, copyErr := io.Copy(crc, contextReader{ctx, state})
		state.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if size != meta.Size || !bytes.Equal(crc.Sum(nil), meta.CRC) {
			return nil, errors.New("snapshot integrity check failed")
		}
		snapshot := meta.SnapshotMeta
		result.metas = append(result.metas, &snapshot)
	}
	sort.Slice(result.metas, func(i, j int) bool { return result.metas[i].Index > result.metas[j].Index })
	return result, nil
}

// NewRaft (also used by GetConfiguration) calls setCurrentTerm during offline
// initialization. Keep all such stable-store writes private to inspection.
// The underlying Bolt handle remains read-only as an independent backstop.
type inspectionStable struct {
	raft.StableStore
	bytes    map[string][]byte
	integers map[string]uint64
}

func (s *inspectionStable) Set(key, value []byte) error {
	s.bytes[string(key)] = append([]byte(nil), value...)
	return nil
}
func (s *inspectionStable) Get(key []byte) ([]byte, error) {
	if value, found := s.bytes[string(key)]; found {
		return append([]byte(nil), value...), nil
	}
	return s.StableStore.Get(key)
}
func (s *inspectionStable) SetUint64(key []byte, value uint64) error {
	s.integers[string(key)] = value
	return nil
}
func (s *inspectionStable) GetUint64(key []byte) (uint64, error) {
	if value, found := s.integers[string(key)]; found {
		return value, nil
	}
	return s.StableStore.GetUint64(key)
}

func inspectBoltLayout(path string) error {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.View(func(tx *bbolt.Tx) error {
		if tx.Bucket([]byte("logs")) == nil || tx.Bucket([]byte("conf")) == nil {
			return errors.New("legacy Raft database lacks required buckets")
		}
		return nil
	})
}

// The adopted reader panics for some damaged log layouts. Only this offline,
// no-background-work inspection boundary converts that rejection to an error;
// ordinary committed FSM failures retain their fail-stop behavior.
func inspectConfiguration(cfg *raft.Config, logs raft.LogStore, stable raft.StableStore, snaps raft.SnapshotStore, transport raft.Transport) (configuration raft.Configuration, err error) {
	defer func() {
		if recover() != nil {
			configuration = raft.Configuration{}
			err = errors.New("persisted Raft configuration cannot be inspected safely; preserve evidence and recover on a copy")
		}
	}()
	clone := *cfg
	clone.NoSnapshotRestoreOnStart = true
	return raft.GetConfiguration(&clone, nil, logs, &inspectionStable{StableStore: stable, bytes: map[string][]byte{}, integers: map[string]uint64{}}, snaps, transport)
}
