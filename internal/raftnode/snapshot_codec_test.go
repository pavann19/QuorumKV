package raftnode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pavann19/quorumkv/internal/store"
	storagepb "github.com/pavann19/quorumkv/proto/storagepb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func snapshotBytes(t *testing.T, header *storagepb.SnapshotHeader, entries ...*storagepb.SnapshotEntry) []byte {
	t.Helper()
	var out bytes.Buffer
	out.Write(snapshotMagic)
	if err := writeSnapshotFrame(&out, header, maxSnapshotHeader); err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if err := writeSnapshotFrame(&out, entry, maxSnapshotEntry); err != nil {
			t.Fatal(err)
		}
	}
	return out.Bytes()
}
func TestVersionedSnapshotRoundTripAndNoStaleState(t *testing.T) {
	limits := store.Limits{MaxBytes: 64, MaxEntries: 4}
	entries := []store.Entry{{Key: []byte("retained"), Value: []byte("ack")}, {Key: []byte("empty"), Value: nil}}
	var out bytes.Buffer
	if err := encodeSnapshotV1(&out, "codec", limits, 42, entries); err != nil {
		t.Fatal(err)
	}
	decoded, index, err := decodeSnapshotV1(bytes.NewReader(out.Bytes()), "codec", limits)
	if err != nil || index != 42 {
		t.Fatal(index, err)
	}
	if len(decoded) != 2 || string(decoded[0].Key) != "retained" || string(decoded[0].Value) != "ack" {
		t.Fatal(decoded)
	}
	path := filepath.Join(t.TempDir(), "store.wal")
	st, err := store.OpenWithLimits(path, limits)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	st.Put([]byte("stale"), []byte("must-disappear"))
	f := &FSM{Store: st, clusterID: "codec", snapshotFormatVersion: 1}
	if err = f.Restore(io.NopCloser(bytes.NewReader(out.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if _, found := st.Get([]byte("stale")); found {
		t.Fatal("snapshot left stale state")
	}
	if f.lastCommandIndex.Load() != 42 {
		t.Fatal("snapshot completion index not restored")
	}
	snapshot, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &codecSink{}
	if err = snapshot.Persist(sink); err != nil {
		t.Fatal(err)
	}
	if !sink.closed || sink.canceled {
		t.Fatal("valid snapshot did not complete")
	}
	if _, _, err = decodeSnapshotV1(bytes.NewReader(sink.Bytes()), "codec", limits); err != nil {
		t.Fatal(err)
	}
}

type codecSink struct {
	bytes.Buffer
	closed, canceled bool
}

func (s *codecSink) ID() string    { return "owned-snapshot-fixture" }
func (s *codecSink) Close() error  { s.closed = true; return nil }
func (s *codecSink) Cancel() error { s.canceled = true; return nil }

func TestVersionedSnapshotRefusalPreservesStoreAndWAL(t *testing.T) {
	limits := store.Limits{MaxBytes: 64, MaxEntries: 4}
	base := &storagepb.SnapshotHeader{SchemaVersion: 1, ClusterId: "codec", MaxStoreBytes: 64, MaxStoreEntries: 4, EntryCount: 1, LogicalBytes: 4}
	valid := snapshotBytes(t, base, &storagepb.SnapshotEntry{Key: []byte("key"), Value: []byte("v")})
	cases := map[string][]byte{"truncated": valid[:len(valid)-1], "trailing": append(append([]byte(nil), valid...), 1), "legacy_in_v1": []byte("legacy is not accepted by bounded v1")}
	for name, mutate := range map[string]func(*storagepb.SnapshotHeader){"unknown_version": func(h *storagepb.SnapshotHeader) { h.SchemaVersion = 2 }, "wrong_cluster": func(h *storagepb.SnapshotHeader) { h.ClusterId = "other" }, "wrong_limits": func(h *storagepb.SnapshotHeader) { h.MaxStoreBytes = 65 }, "too_many_entries": func(h *storagepb.SnapshotHeader) { h.EntryCount = 5 }, "excess_declared_bytes": func(h *storagepb.SnapshotHeader) { h.LogicalBytes = 65 }, "wrong_logical_bytes": func(h *storagepb.SnapshotHeader) { h.LogicalBytes = 3 }} {
		header := proto.Clone(base).(*storagepb.SnapshotHeader)
		mutate(header)
		cases[name] = snapshotBytes(t, header, &storagepb.SnapshotEntry{Key: []byte("key"), Value: []byte("v")})
	}
	duplicate := proto.Clone(base).(*storagepb.SnapshotHeader)
	duplicate.EntryCount = 2
	duplicate.LogicalBytes = 8
	cases["duplicate_keys"] = snapshotBytes(t, duplicate, &storagepb.SnapshotEntry{Key: []byte("key"), Value: []byte("v")}, &storagepb.SnapshotEntry{Key: []byte("key"), Value: []byte("x")})
	bomb := append([]byte(nil), snapshotMagic...)
	bomb = append(bomb, 0xff, 0xff, 0xff, 0xff)
	cases["oversized_header_frame"] = bomb
	var badEntry bytes.Buffer
	badEntry.Write(snapshotBytes(t, base))
	var size [4]byte
	binary.LittleEndian.PutUint32(size[:], uint32(maxSnapshotEntry+1))
	badEntry.Write(size[:])
	cases["oversized_entry_frame"] = badEntry.Bytes()
	unknown := proto.Clone(base).(*storagepb.SnapshotHeader)
	unknown.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	cases["unknown_header_field"] = snapshotBytes(t, unknown)
	for name, payload := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.wal")
			st, err := store.OpenWithLimits(path, limits)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			st.Put([]byte("original"), []byte("preserve"))
			before, _ := os.ReadFile(path)
			beforeMap := st.Dump()
			f := &FSM{Store: st, clusterID: "codec", snapshotFormatVersion: 1}
			f.lastCommandIndex.Store(9)
			if err = f.Restore(io.NopCloser(bytes.NewReader(payload))); err == nil {
				t.Fatal("malformed snapshot installed")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(before, after) || !reflect.DeepEqual(beforeMap, st.Dump()) || f.lastCommandIndex.Load() != 9 {
				t.Fatal("refused snapshot changed existing state")
			}
		})
	}
}
func TestVersionedSnapshotHeaderRejectsBeforePayloadRead(t *testing.T) {
	limits := store.Limits{MaxBytes: 64, MaxEntries: 4}
	header := &storagepb.SnapshotHeader{SchemaVersion: 99, ClusterId: "codec", MaxStoreBytes: 64, MaxStoreEntries: 4}
	reader := io.MultiReader(bytes.NewReader(snapshotBytes(t, header)), payloadBomb{t})
	if _, _, err := decodeSnapshotV1(reader, "codec", limits); !errors.Is(err, ErrSnapshotFormat) {
		t.Fatal("unknown schema not rejected", err)
	}
}

type payloadBomb struct{ t *testing.T }

func (b payloadBomb) Read([]byte) (int, error) {
	b.t.Fatal("unsupported header consumed payload")
	return 0, io.EOF
}
func TestVersionedSnapshotPersistCancelsOnInvalidState(t *testing.T) {
	snapshot := &fsmSnapshot{format: 1, clusterID: "codec", limits: store.Limits{MaxBytes: 64, MaxEntries: 4}, data: []store.Entry{{Key: []byte("same")}, {Key: []byte("same")}}}
	sink := &codecSink{}
	if err := snapshot.Persist(sink); err == nil || !sink.canceled || sink.closed {
		t.Fatal("invalid snapshot was finalized", err)
	}
}

func TestSnapshotFormatManifestGuardsCompatibility(t *testing.T) {
	peer := Peer{ID: "one", Addr: "127.0.0.1:29979"}
	dir := t.TempDir()
	options := StartOptions{ClusterID: "format-fixture", Bootstrap: true, BootstrapExpect: 1, SnapshotFormatVersion: 1}
	n, err := New(peer, []Peer{peer}, dir, options)
	if err != nil {
		t.Fatal(err)
	}
	n.Shutdown()
	raw, err := os.ReadFile(filepath.Join(dir, "cluster.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest clusterManifest
	if err = json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || manifest.SnapshotFormatVersion != 1 {
		t.Fatal("new format lacks old-reader refusal marker", manifest)
	}
	original, _ := os.ReadFile(filepath.Join(dir, "store.wal"))
	options.Bootstrap = false
	options.SnapshotFormatVersion = 0
	if n, err = New(peer, []Peer{peer}, dir, options); err == nil {
		n.Shutdown()
		t.Fatal("format downgrade opened new-format member")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "store.wal"))
	if !bytes.Equal(original, after) {
		t.Fatal("refused downgrade modified member WAL")
	}
	options.SnapshotFormatVersion = 99
	if n, err = New(peer, []Peer{peer}, dir, options); err == nil {
		n.Shutdown()
		t.Fatal("unknown format opened")
	}
	// The old manifest schema's reader rejects Version=2 before storage open;
	// do not claim older unguarded historical executables are safe to run here.
	legacy := StartOptions{ClusterID: "format-fixture", StoreLimits: store.DefaultLimits()}
	if err = validateManifest(dir, peer, []Peer{peer}, legacy); err == nil {
		t.Fatal("legacy-mode manifest guard accepted v1 snapshot state")
	}
}

func TestVersionedSnapshotUnknownModeNeverFallsBack(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	f := &FSM{Store: st, snapshotFormatVersion: 99}
	if err := f.Restore(io.NopCloser(payloadBomb{t})); !errors.Is(err, ErrSnapshotFormat) {
		t.Fatal(err)
	}
	sink := &codecSink{}
	if err := (&fsmSnapshot{format: 99}).Persist(sink); !errors.Is(err, ErrSnapshotFormat) || !sink.canceled || sink.closed || sink.Len() != 0 {
		t.Fatal("unknown mode persisted", err)
	}
}
