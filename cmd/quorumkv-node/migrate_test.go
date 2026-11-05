package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/raftnode"
)

func TestMigrationExecutableRequiresExactApproval(t *testing.T) {
	if testing.Short() {
		t.Skip("owned executable fixture")
	}
	root := t.TempDir()
	dir := filepath.Join(root, "member")
	peer := raftnode.Peer{ID: "one", Addr: "127.0.0.1:29985"}
	n, err := raftnode.New(peer, []raftnode.Peer{peer}, dir, raftnode.StartOptions{ClusterID: "migration-cli", Bootstrap: true, BootstrapExpect: 1, DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for !n.IsLeader() {
		if time.Now().After(deadline) {
			n.Shutdown()
			t.Fatal("leader timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = n.Put([]byte("retained"), []byte("ack"), time.Second); err != nil {
		n.Shutdown()
		t.Fatal(err)
	}
	n.Shutdown()
	os.Remove(filepath.Join(dir, "cluster.json"))
	original, err := os.ReadFile(filepath.Join(dir, "raft-log.bolt"))
	if err != nil {
		t.Fatal(err)
	}
	binary := fixtureNodeBinary(t, root)
	args := []string{"migrate-identity", "-id=one", "-peers=one=" + peer.Addr, "-cluster-id=migration-cli", "-data-dir=" + dir}
	output, err := exec.Command(binary, args...).Output()
	if err != nil {
		t.Fatal(err)
	}
	var report raftnode.IdentityMigration
	if err = json.Unmarshal(output, &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "INSPECTED_NOT_APPLIED" {
		t.Fatal("inspection applied mutation")
	}
	if _, err = os.Stat(filepath.Join(dir, "cluster.json")); !os.IsNotExist(err) {
		t.Fatal("dry inspection created manifest")
	}
	after, _ := os.ReadFile(filepath.Join(dir, "raft-log.bolt"))
	if !bytes.Equal(original, after) {
		t.Fatal("inspection modified Bolt data")
	}
	if err = exec.Command(binary, append(args, "-approve-sha256=wrong")...).Run(); err == nil {
		t.Fatal("wrong approval accepted")
	}
	output, err = exec.Command(binary, append(args, "-approve-sha256="+report.SourceSHA256)...).Output()
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(output, &report); err != nil {
		t.Fatal(err)
	}
	if report.Status != "APPLIED_IDENTITY_ONLY" {
		t.Fatal(report.Status)
	}
	after, _ = os.ReadFile(filepath.Join(dir, "raft-log.bolt"))
	if !bytes.Equal(original, after) {
		t.Fatal("approved migration modified Bolt data")
	}
	if err = exec.Command(binary, args...).Run(); err == nil {
		t.Fatal("existing identity reassignment allowed")
	}
}
