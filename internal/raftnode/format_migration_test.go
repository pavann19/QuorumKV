package raftnode

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/store"
)

func TestMigrationHeadroomFailsClosed(t *testing.T) {
	required := migrationHeadroom([]MigrationFile{{Bytes: 1000}}, 2)
	if required != 64<<20+3000+2048 {
		t.Fatal("headroom estimate changed", required)
	}
	for _, space := range []diskhealth.Space{{}, {Available: 11, Total: 10}, {Available: required - 1, Total: required * 2}} {
		if err := checkMigrationHeadroom(space, nil, required); err == nil {
			t.Fatal("unknown/low headroom accepted", space)
		}
	}
	if err := checkMigrationHeadroom(diskhealth.Space{Available: required, Total: required * 2}, nil, required); err != nil {
		t.Fatal(err)
	}
	if err := checkMigrationHeadroom(diskhealth.Space{Available: required, Total: required * 2}, errors.New("probe failed"), required); !errors.Is(err, diskhealth.ErrUnknown) {
		t.Fatal(err)
	}
}

func TestFormatConversionRefusesActiveCorruptAndChangingSource(t *testing.T) {
	self, peers, source, options := conversionFixture(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := filepath.Join(t.TempDir(), "target")
	n, err := New(self, peers, source, options)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ConvertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, ""); err == nil {
		n.Shutdown()
		t.Fatal("active source accepted")
	}
	n.Shutdown()
	plan, err := ConvertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(t.TempDir(), "other")
	if _, err = ConvertMemberFormat(ctx, source, other, StorageProfile{1, 1, 1}, plan.ApprovalSHA256); err == nil {
		t.Fatal("approval reused for another destination")
	}
	if _, err = convertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, plan.ApprovalSHA256, func() error {
		return os.WriteFile(filepath.Join(source, "concurrent-synthetic-evidence"), []byte("preserve"), 0600)
	}); err == nil {
		t.Fatal("source drift published copy")
	}
	if _, err = os.Stat(filepath.Join(destination, migrationIncomplete)); err != nil {
		t.Fatal("source drift lost incomplete fence")
	}
	fresh := filepath.Join(t.TempDir(), "corrupt-refused")
	walPath := filepath.Join(source, "store.wal")
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{1, 2, 3})
	f.Close()
	before, _ := os.ReadFile(walPath)
	if _, err = ConvertMemberFormat(ctx, source, fresh, StorageProfile{1, 1, 1}, ""); err == nil {
		t.Fatal("partial source tail converted")
	}
	after, _ := os.ReadFile(walPath)
	if !bytes.Equal(before, after) {
		t.Fatal("inspection repaired partial source")
	}
	if _, err = os.Stat(fresh); !os.IsNotExist(err) {
		t.Fatal("partial-tail refusal created output")
	}
}

func TestLegacySnapshotMigrationGuard(t *testing.T) {
	entries := []store.Entry{{Key: []byte("retained"), Value: []byte("ack")}, {Key: []byte("empty"), Value: nil}}
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(entries); err != nil {
		t.Fatal(err)
	}
	raw := buffer.Bytes()
	got, err := decodeMigrationLegacySnapshot(bytes.NewReader(raw), int64(len(raw)))
	if err != nil || !reflect.DeepEqual(entries, got) {
		t.Fatal("guard changed valid legacy snapshot", err)
	}
	for _, bad := range [][]byte{raw[:len(raw)-1], append(bytes.Clone(raw), 0), append(bytes.Clone(raw), raw...), {0xff, 0xff}} {
		if _, err = decodeMigrationLegacySnapshot(bytes.NewReader(bad), int64(len(bad))); err == nil {
			t.Fatal("bad/trailing framing accepted")
		}
	}
	// Positive type ID 64, singleton delta zero, then a count over one million.
	// The guard rejects this before asking gob to allocate a destination slice.
	body := []byte{0xff, 0x80, 0, 0xfd, 0x0f, 0x42, 0x41}
	hostile := append([]byte{byte(len(body))}, body...)
	if _, err = guardedLegacySnapshot(bytes.NewReader(hostile), int64(len(hostile))); err == nil {
		t.Fatal("oversized declared slice accepted")
	}
}

func conversionFixture(t *testing.T, snapshot bool) (Peer, []Peer, string, StartOptions) {
	t.Helper()
	self, peers, dir, options := legacyFixture(t, snapshot)
	var err error
	options, err = validateStart(self, peers, []StartOptions{options})
	if err != nil {
		t.Fatal(err)
	}
	if err := persistManifest(dir, self, peers, options, false); err != nil {
		t.Fatal(err)
	}
	return self, peers, dir, options
}
func conversionLeader(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(7 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("converted node election timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := n.Raft.Barrier(time.Second).Error(); err != nil {
		t.Fatal(err)
	}
}
func assertConvertedValues(t *testing.T, n *Node) {
	t.Helper()
	if v, ok := n.Get([]byte("retained")); !ok || string(v) != "ack" {
		t.Fatal("conversion lost retained value")
	}
	if _, ok := n.Get([]byte("deleted")); ok {
		t.Fatal("conversion resurrected deletion")
	}
}

func TestOfflineFormatConversionForwardBackwardPreservesSource(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "logs", true: "snapshot"}[snapshot], func(t *testing.T) {
			self, peers, source, options := conversionFixture(t, snapshot)
			destination := filepath.Join(t.TempDir(), "versioned")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			before, _ := migrationFiles(source)
			plan, err := ConvertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(destination); !os.IsNotExist(err) {
				t.Fatal("inspection created destination")
			}
			if _, err = ConvertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, "wrong"); err == nil {
				t.Fatal("unapproved conversion applied")
			}
			report, err := ConvertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, plan.ApprovalSHA256)
			if err != nil {
				t.Fatal(err)
			}
			if report.Status != "CONVERTED_COPY_VERIFIED_SOURCE_UNCHANGED" {
				t.Fatal(report.Status)
			}
			after, _ := migrationFiles(source)
			if !reflect.DeepEqual(before, after) {
				t.Fatal("conversion changed original data")
			}
			if _, err = os.Stat(filepath.Join(destination, migrationIncomplete)); !os.IsNotExist(err) {
				t.Fatal("successful copy remains incomplete")
			}
			actual, _ := migrationFiles(destination)
			var inventory []MigrationFile
			for _, f := range actual {
				if f.Path != migrationReady {
					inventory = append(inventory, f)
				}
			}
			if !reflect.DeepEqual(inventory, report.TargetFiles) {
				t.Fatal("published target differs from ready inventory")
			}
			versioned := options
			versioned.SnapshotFormatVersion = 1
			versioned.WALFormatVersion = 1
			versioned.CommandFormatVersion = 1
			n, err := New(self, peers, destination, versioned)
			if err != nil {
				t.Fatal(err)
			}
			conversionLeader(t, n)
			assertConvertedValues(t, n)
			if err = n.Put([]byte("after-upgrade"), []byte("ack"), time.Second); err != nil {
				n.Shutdown()
				t.Fatal(err)
			}
			if snapshot {
				if err = n.Raft.Snapshot().Error(); err != nil {
					n.Shutdown()
					t.Fatal(err)
				}
			}
			if err = n.Put([]byte("tail"), []byte("after-snapshot"), time.Second); err != nil {
				n.Shutdown()
				t.Fatal(err)
			}
			n.Shutdown()
			back := filepath.Join(t.TempDir(), "legacy")
			plan, err = ConvertMemberFormat(ctx, destination, back, StorageProfile{}, "")
			if err != nil {
				t.Fatal(err)
			}
			if _, err = ConvertMemberFormat(ctx, destination, back, StorageProfile{}, plan.ApprovalSHA256); err != nil {
				t.Fatal(err)
			}
			n, err = New(self, peers, back, options)
			if err != nil {
				t.Fatal(err)
			}
			defer n.Shutdown()
			conversionLeader(t, n)
			assertConvertedValues(t, n)
			for _, key := range []string{"after-upgrade", "tail"} {
				if _, ok := n.Get([]byte(key)); !ok {
					t.Fatal("backward conversion lost later acknowledgement", key)
				}
			}
			if err = n.Put([]byte("after-downgrade"), []byte("ack"), time.Second); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestFormatConversionRefusalsAndIncompleteCopyFence(t *testing.T) {
	self, peers, source, options := conversionFixture(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	destination := filepath.Join(t.TempDir(), "failed-copy")
	plan, err := ConvertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	before, _ := migrationFiles(source)
	injected := errors.New("synthetic pre-publication interruption")
	if _, err = convertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, plan.ApprovalSHA256, func() error { return injected }); !errors.Is(err, injected) {
		t.Fatal("interruption was not retained", err)
	}
	if _, err = os.Stat(filepath.Join(destination, migrationIncomplete)); err != nil {
		t.Fatal("failed copy lost incomplete marker", err)
	}
	if n, err := New(self, peers, destination, options); err == nil {
		n.Shutdown()
		t.Fatal("incomplete conversion started")
	}
	after, _ := migrationFiles(source)
	if !reflect.DeepEqual(before, after) {
		t.Fatal("interruption changed source")
	}
	if _, err = ConvertMemberFormat(ctx, source, destination, StorageProfile{1, 1, 1}, plan.ApprovalSHA256); err == nil {
		t.Fatal("failed copy silently overwritten/resumed")
	}
	nested := filepath.Join(source, "nested")
	if _, err = ConvertMemberFormat(ctx, source, nested, StorageProfile{1, 1, 1}, ""); err == nil {
		t.Fatal("nested target accepted")
	}
	if _, err = os.Stat(nested); !os.IsNotExist(err) {
		t.Fatal("refusal created nested target")
	}
	fresh := filepath.Join(t.TempDir(), "fresh")
	plan, err = ConvertMemberFormat(ctx, source, fresh, StorageProfile{1, 1, 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(source, "retained-new-evidence"), []byte("preserve"), 0600)
	if _, err = ConvertMemberFormat(ctx, source, fresh, StorageProfile{1, 1, 1}, plan.ApprovalSHA256); err == nil {
		t.Fatal("stale source approval accepted")
	}
	if _, err = os.Stat(fresh); !os.IsNotExist(err) {
		t.Fatal("stale approval created target")
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err = ConvertMemberFormat(canceled, source, fresh, StorageProfile{1, 1, 1}, ""); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled conversion continued", err)
	}
}
