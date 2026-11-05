package raftnode

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/pavann19/quorumkv/internal/diskhealth"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/pavann19/quorumkv/internal/store"
)

func TestFatalSubprocessHelper(t *testing.T) {
	mode := os.Getenv("QUORUMKV_FATAL_FIXTURE")
	if mode == "" {
		return
	}
	dir := os.Getenv("QUORUMKV_FATAL_DIR")
	st, err := store.Open(filepath.Join(dir, "store.wal"))
	if err != nil {
		os.Exit(2)
	}
	f := &FSM{Store: st, fatal: func(reason string, index uint64) { fatalStop(dir, reason, index) }}
	switch mode {
	case "storage":
		self := Peer{ID: "single", Addr: "127.0.0.1:29996"}
		st.Close()
		n, err := New(self, []Peer{self}, dir, StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, Bootstrap: true, BootstrapExpect: 1, ClusterID: "fatal-fixture"})
		if err != nil {
			os.Exit(3)
		}
		deadline := time.Now().Add(10 * time.Second)
		for !n.IsLeader() {
			if time.Now().After(deadline) {
				os.Exit(4)
			}
			time.Sleep(10 * time.Millisecond)
		}
		n.Store.Close() // Only this disposable fixture's store is made unwritable.
		n.Put([]byte("FAKE_PRIVATE_KEY"), []byte("FAKE_SECRET_VALUE"), time.Second)
	case "decode":
		f.Apply(&raft.Log{Index: 42, Data: []byte("FAKE_SECRET_VALUE")})
	case "command-format":
		f.commandFormatVersion = 1
		f.clusterID = "target-cluster"
		raw, _ := encodeCommandFormat(command{Op: opPut, Key: []byte("FAKE_PRIVATE_KEY"), Value: []byte("FAKE_SECRET_VALUE"), Limits: st.Limits()}, "other-cluster", 1)
		f.Apply(&raft.Log{Index: 44, Data: raw})
	case "unknown":
		raw, _ := encodeCommand(command{Op: 99, Value: []byte("FAKE_SECRET_VALUE")})
		f.Apply(&raft.Log{Index: 43, Data: raw})
	case "compaction":
		st.Close()
		f.Snapshot()
	}
	os.Exit(5) // Reaching here would mean failure was allowed to continue.
}

func TestFatalFailuresExitAndPersistRedactedIncident(t *testing.T) {
	for _, mode := range []string{"storage", "decode", "unknown", "compaction", "command-format"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFatalSubprocessHelper$")
			cmd.Env = append(os.Environ(), "QUORUMKV_FATAL_FIXTURE="+mode, "QUORUMKV_FATAL_DIR="+dir)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != FatalExitCode {
				t.Fatalf("did not fail-stop with 70: %v %s", err, output)
			}
			raw, err := os.ReadFile(filepath.Join(dir, "FATAL"))
			if err != nil {
				t.Fatal(err)
			}
			var incident fatalIncident
			if err = json.Unmarshal(raw, &incident); err != nil {
				t.Fatal(err)
			}
			if incident.Version != 1 || incident.Reason == "" || incident.UTC == "" {
				t.Fatalf("invalid incident %s", raw)
			}
			if strings.Contains(string(raw)+string(output), "FAKE_SECRET") || strings.Contains(string(raw)+string(output), "FAKE_PRIVATE") {
				t.Fatal("secret leaked")
			}
			self := Peer{ID: "single", Addr: "127.0.0.1:29996"}
			if n, err := New(self, []Peer{self}, dir, StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "fatal-fixture"}); !errors.Is(err, ErrFatalState) {
				if n != nil {
					n.Shutdown()
				}
				t.Fatalf("restart bypassed marker: %v", err)
			}
			after, _ := os.ReadFile(filepath.Join(dir, "FATAL"))
			if string(after) != string(raw) {
				t.Fatal("restart modified original incident")
			}
		})
	}
}

func TestFatalClearArchivesRatherThanErases(t *testing.T) {
	dir := t.TempDir()
	if err := writeFatal(dir, "synthetic", 17); err != nil {
		t.Fatal(err)
	}
	original, _ := os.ReadFile(filepath.Join(dir, "FATAL"))
	if err := writeFatal(dir, "later", 99); err != nil {
		t.Fatal(err)
	}
	after, _ := os.ReadFile(filepath.Join(dir, "FATAL"))
	if string(after) != string(original) {
		t.Fatal("original incident overwritten")
	}
	if err := checkFatal(dir, true); err != nil {
		t.Fatal(err)
	}
	archived, err := filepath.Glob(filepath.Join(dir, "FATAL.resolved.*"))
	if err != nil || len(archived) != 1 {
		t.Fatal("missing archive", archived, err)
	}
	preserved, _ := os.ReadFile(archived[0])
	if string(preserved) != string(original) {
		t.Fatal("incident changed")
	}
	if err := checkFatal(dir, false); err != nil {
		t.Fatal(err)
	}
}

func TestMalformedFatalMarkerStillBlocksStartup(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "FATAL"), []byte("partial marker"), 0600)
	if !errors.Is(checkFatal(dir, false), ErrFatalState) {
		t.Fatal("partial marker accepted")
	}
}

func TestStartupCorruptionPreservesWALAndCreatesFatalMarker(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.wal")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = st.Put([]byte("fixture"), []byte("original")); err != nil {
		t.Fatal(err)
	}
	st.Close()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	self := Peer{ID: "single", Addr: "127.0.0.1:29997"}
	n, err := New(self, []Peer{self}, dir, StartOptions{DiskPolicy: diskhealth.Policy{MaxUsedPercent: 95, MinFreeBytes: 64 << 20}, ClusterID: "corrupt-fixture", Bootstrap: true, BootstrapExpect: 1})
	if !errors.Is(err, ErrFatalState) {
		if n != nil {
			n.Shutdown()
		}
		t.Fatalf("corruption not classified fatal: %v", err)
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(raw) {
		t.Fatal("corrupt original modified")
	}
	if err = checkFatal(dir, false); !errors.Is(err, ErrFatalState) {
		t.Fatal("no restart block", err)
	}
}
