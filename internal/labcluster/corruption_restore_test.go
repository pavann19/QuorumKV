package labcluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/wal"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

type drillFile struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// These helpers operate only on paths owned by the disposable test fixture.
// Copy never overwrites a destination or follows evidence symlinks.
func copyDrillTree(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("fixture refuses symlinks")
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			return os.Mkdir(target, 0700)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return errors.New("fixture requires regular files")
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		if copyErr == nil {
			copyErr = output.Sync()
		}
		return errors.Join(copyErr, output.Close())
	})
}
func hashDrillTree(root string) (map[string]drillFile, error) {
	files := map[string]drillFile{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("fixture refuses symlinks")
		}
		if entry.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		hash := sha256.New()
		n, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if err = errors.Join(copyErr, closeErr); err != nil {
			return err
		}
		files[filepath.ToSlash(rel)] = drillFile{n, hex.EncodeToString(hash.Sum(nil))}
		return nil
	})
	return files, err
}
func exactDrillState(n *raftnode.Node) map[string]string {
	actual := map[string]string{}
	for _, entry := range n.Store.Dump() {
		actual[string(entry.Key)] = string(entry.Value)
	}
	return actual
}

func TestCorruptMemberVerifiedBackupRestoreDrill(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	report := map[string]any{"schema": 1, "scope": "synthetic single-host mTLS offline corruption/restore drill", "status": "INCOMPLETE", "acknowledged_mutations": 0, "production_rto_rpo": "UNVERIFIED", "source": "current candidate identified by enclosing runner"}
	artifact := ""
	if output := os.Getenv("QUORUMKV_RESULTS_DIR"); output != "" {
		if err = os.MkdirAll(output, 0700); err != nil {
			t.Fatal(err)
		}
		artifact, err = os.MkdirTemp(output, "corruption-restore-")
		if err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		if t.Failed() {
			report["status"] = "FAIL"
		}
		raw, _ := json.MarshalIndent(report, "", "  ")
		if artifact != "" {
			if err := os.WriteFile(filepath.Join(artifact, "drill.json"), append(raw, '\n'), 0600); err != nil {
				t.Error(err)
			}
		}
		t.Logf("corruption_restore_drill=%s", raw)
	}()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{}
	deleted := []string{}
	for i := 0; i < 16; i++ {
		key := fmt.Sprintf("drill-%02d", i)
		value := fmt.Sprintf("acknowledged-%02d", i)
		if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)}); err != nil {
			t.Fatal(err)
		}
		expected[key] = value
		report["acknowledged_mutations"] = i + 1
	}
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("drill-%02d", i)
		if _, err = c.Clients[leader].Delete(ctx, &pb.DeleteRequest{Key: []byte(key)}); err != nil {
			t.Fatal(err)
		}
		delete(expected, key)
		deleted = append(deleted, key)
		report["acknowledged_mutations"] = 17 + i
	}
	// Quiesce at exact state before backup; require every replica, not merely the
	// leader or the acknowledgement quorum, to contain the full expected map.
	deadline := time.Now().Add(10 * time.Second)
	for {
		same := true
		for _, node := range c.Nodes {
			if !reflect.DeepEqual(exactDrillState(node), expected) {
				same = false
			}
		}
		if same {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replicas did not reach backup keyspace")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, node := range c.Nodes {
		if err = node.Raft.Snapshot().Error(); err != nil {
			t.Fatal(err)
		}
	}
	for i := range c.Nodes {
		if err = c.Stop(i); err != nil {
			t.Fatal(err)
		}
	}
	originalRoot := c.Root
	backup := filepath.Join(originalRoot, "verified-backup")
	if err = os.Mkdir(backup, 0700); err != nil {
		t.Fatal(err)
	}
	for _, peer := range c.Peers {
		source := filepath.Join(originalRoot, peer.ID)
		destination := filepath.Join(backup, peer.ID)
		if err = copyDrillTree(source, destination); err != nil {
			t.Fatal(err)
		}
		before, err := hashDrillTree(source)
		if err != nil {
			t.Fatal(err)
		}
		after, err := hashDrillTree(destination)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(before, after) {
			t.Fatal("backup byte verification failed")
		}
	}
	backupHashes, err := hashDrillTree(backup)
	if err != nil {
		t.Fatal(err)
	}
	report["backup_files"] = backupHashes
	report["backup_verified"] = true
	report["retained_keys"] = len(expected)
	report["deleted_keys"] = len(deleted)
	if artifact != "" {
		if err = copyDrillTree(backup, filepath.Join(artifact, "verified-backup")); err != nil {
			t.Fatal(err)
		}
	}
	// Corrupt only the stopped synthetic member, leaving the verified backup
	// immutable and preserving the damaged bytes before any attempted restart.
	damagedPath := filepath.Join(originalRoot, c.Peers[leader].ID, "store.wal")
	damaged, err := os.ReadFile(damagedPath)
	if err != nil || len(damaged) < 9 {
		t.Fatal("missing populated fixture WAL", err)
	}
	damaged[len(damaged)-1] ^= 0xff
	if err = os.WriteFile(damagedPath, damaged, 0600); err != nil {
		t.Fatal(err)
	}
	damagedHash := sha256.Sum256(damaged)
	report["damaged_wal_sha256"] = fmt.Sprintf("%x", damagedHash)
	if artifact != "" {
		if err = os.WriteFile(filepath.Join(artifact, "corrupt-store.wal"), damaged, 0600); err != nil {
			t.Fatal(err)
		}
	}
	incidentFiles, err := hashDrillTree(filepath.Dir(damagedPath))
	if err != nil {
		t.Fatal(err)
	}
	report["incident_files_before_restart"] = incidentFiles
	err = c.Restart(leader)
	if !errors.Is(err, raftnode.ErrFatalState) || !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("corrupt member startup was not fatal: %v", err)
	}
	if c.Nodes[leader] != nil {
		t.Fatal("corrupt member became a serving node")
	}
	after, err := os.ReadFile(damagedPath)
	if err != nil || !bytes.Equal(after, damaged) {
		t.Fatal("startup changed corrupt evidence", err)
	}
	marker, err := os.ReadFile(filepath.Join(originalRoot, c.Peers[leader].ID, "FATAL"))
	if err != nil {
		t.Fatal(err)
	}
	if artifact != "" {
		if err = os.WriteFile(filepath.Join(artifact, "FATAL"), marker, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err = c.Restart(leader); !errors.Is(err, raftnode.ErrFatalState) {
		t.Fatal("unattended retry bypassed fatal marker", err)
	}
	incidentAfter, err := hashDrillTree(filepath.Dir(damagedPath))
	if err != nil {
		t.Fatal(err)
	}
	delete(incidentAfter, "FATAL")
	if !reflect.DeepEqual(incidentFiles, incidentAfter) {
		t.Fatal("corrupt startup modified other original incident files")
	}
	report["corrupt_start_refused"] = true
	report["corrupt_wal_preserved"] = true
	report["fatal_restart_refused"] = true
	restoreStart := time.Now()
	restored := filepath.Join(originalRoot, "restored-members")
	if err = copyDrillTree(backup, restored); err != nil {
		t.Fatal(err)
	}
	restoredHashes, err := hashDrillTree(restored)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(backupHashes, restoredHashes) {
		t.Fatal("restore materialization checksum mismatch")
	}
	c.Root = restored
	for i := range c.Nodes {
		if err = c.Restart(i); err != nil {
			t.Fatal(err)
		}
	}
	leader, err = c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client, closeClient, err := c.ColdClient(leader)
	if err != nil {
		t.Fatal(err)
	}
	defer closeClient()
	for key, value := range expected {
		result, err := client.Get(ctx, &pb.GetRequest{Key: []byte(key)})
		if err != nil || !result.Found || string(result.Value) != value {
			t.Fatalf("restored retained key mismatch: %s %v", key, err)
		}
	}
	for _, key := range deleted {
		result, err := client.Get(ctx, &pb.GetRequest{Key: []byte(key)})
		if err != nil || result.Found {
			t.Fatalf("restored deleted key resurrected: %s %v", key, err)
		}
	}
	if !reflect.DeepEqual(exactDrillState(c.Nodes[leader]), expected) {
		t.Fatal("restored keyspace contains stale/unexpected state")
	}
	if _, err = client.Put(ctx, &pb.PutRequest{Key: []byte("post-restore"), Value: []byte("new-ack")}); err != nil {
		t.Fatal(err)
	}
	result, err := client.Get(ctx, &pb.GetRequest{Key: []byte("post-restore")})
	if err != nil || !result.Found || string(result.Value) != "new-ack" {
		t.Fatal("post-restore acknowledged readback failed", err)
	}
	report["restore_copy_to_verified_seconds"] = time.Since(restoreStart).Seconds()
	report["restored_exact_keyspace"] = true
	report["post_restore_write_readback"] = true
	currentBackup, err := hashDrillTree(backup)
	if err != nil || !reflect.DeepEqual(backupHashes, currentBackup) {
		t.Fatal("restore mutated verified backup", err)
	}
	damagedAfter, _ := os.ReadFile(damagedPath)
	if !bytes.Equal(damaged, damagedAfter) {
		t.Fatal("restore modified original corrupt member")
	}
	report["backup_unchanged"] = true
	report["original_incident_unchanged"] = true
	report["status"] = "PASS_LOCAL_LAB"
}
