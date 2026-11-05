//go:build compat

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Separately tagged: this proof requires a receipt-verified retained previous
// executable, rather than silently substituting a second build of today's code.
func TestPreviousExecutableStorageCompatibility(t *testing.T) {
	previous := os.Getenv("QUORUMKV_PREVIOUS_NODE_BINARY")
	expected := os.Getenv("QUORUMKV_PREVIOUS_NODE_SHA256")
	previousBytes, err := os.ReadFile(previous)
	if err != nil {
		t.Fatal("retained previous executable required", err)
	}
	previousHash := fmt.Sprintf("%x", sha256.Sum256(previousBytes))
	if len(expected) != 64 || previousHash != expected {
		t.Fatal("previous executable receipt mismatch")
	}
	root := t.TempDir()
	current := fixtureNodeBinary(t, root)
	currentBytes, err := os.ReadFile(current)
	if err != nil {
		t.Fatal(err)
	}
	currentHash := fmt.Sprintf("%x", sha256.Sum256(currentBytes))
	if currentHash == previousHash {
		t.Fatal("previous executable is today's binary")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	evidence := os.Getenv("QUORUMKV_RESULTS_DIR")
	if evidence == "" {
		evidence = t.TempDir()
	}
	evidence = filepath.Join(evidence, "format-compat")
	if err = os.MkdirAll(evidence, 0700); err != nil {
		t.Fatal(err)
	}
	type running struct {
		command *exec.Cmd
		conn    *grpc.ClientConn
		client  pb.KVClient
		log     *os.File
		stopped bool
	}
	start := func(binary, dir, address, cluster, name string, versioned, bootstrap bool) *running {
		t.Helper()
		args := []string{"-id=one", "-peers=one=" + address, "-cluster-id=" + cluster, "-data-dir=" + dir, "-disk-watermark-percent=95"}
		if bootstrap {
			args = append(args, "-bootstrap", "-bootstrap-expect=1")
		}
		tuning := map[string]any{"raft": map[string]any{"snapshot_interval_ms": 3600000, "snapshot_threshold": 8192, "trailing_logs": 0}}
		if !versioned && name != "previous-tail" {
			tuning["raft"] = map[string]any{"snapshot_interval_ms": 20, "snapshot_threshold": 1, "trailing_logs": 0}
		}
		tuningBytes, _ := json.Marshal(tuning)
		tuningPath := filepath.Join(root, name+"-tuning.json")
		if e := os.WriteFile(tuningPath, tuningBytes, 0600); e != nil {
			t.Fatal(e)
		}
		args = append(args, "-config="+tuningPath)

		if versioned {
			args = append(args, "-wal-format-version=1", "-snapshot-format-version=1", "-command-format-version=1")
		}
		log, e := os.Create(filepath.Join(evidence, name+".log"))
		if e != nil {
			t.Fatal(e)
		}
		command := exec.CommandContext(ctx, binary, args...)
		command.Stdout, command.Stderr = log, log
		if e = command.Start(); e != nil {
			log.Close()
			t.Fatal(e)
		}
		conn, e := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
		if e != nil {
			command.Process.Kill()
			command.Wait()
			log.Close()
			t.Fatal(e)
		}
		r := &running{command: command, conn: conn, client: pb.NewKVClient(conn), log: log}
		t.Cleanup(func() {
			if !r.stopped {
				r.command.Process.Kill()
				r.command.Wait()
				r.conn.Close()
				r.log.Close()
			}
		})
		for deadline := time.Now().Add(10 * time.Second); ; {
			call, stop := context.WithTimeout(ctx, 500*time.Millisecond)
			_, e = r.client.Get(call, &pb.GetRequest{Key: []byte("readiness")})
			stop()
			if e == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("compatibility fixture did not become readable", name, e)
			}
			time.Sleep(25 * time.Millisecond)
		}
		return r
	}
	stop := func(r *running) {
		t.Helper()
		if e := r.command.Process.Kill(); e != nil {
			t.Fatal(e)
		}
		r.command.Wait()
		r.conn.Close()
		r.log.Close()
		r.stopped = true
	}
	address := func() string {
		t.Helper()
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		value := listener.Addr().String()
		listener.Close()
		return value
	}
	put := func(r *running, key, value string) {
		t.Helper()
		if _, e := r.client.Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte(value)}); e != nil {
			t.Fatal("single attempted mutation not acknowledged", key, e)
		}
	}
	get := func(r *running, key, want string, found bool) {
		t.Helper()
		value, e := r.client.Get(ctx, &pb.GetRequest{Key: []byte(key)})
		if e != nil || value.Found != found || string(value.Value) != want {
			t.Fatal("compatibility readback mismatch", key, e, value)
		}
	}
	seed := func(r *running) {
		t.Helper()
		put(r, "retained", "ack")
		put(r, "deleted", "seed")
		if _, e := r.client.Delete(ctx, &pb.DeleteRequest{Key: []byte("deleted")}); e != nil {
			t.Fatal(e)
		}
		get(r, "retained", "ack", true)
		get(r, "deleted", "", false)
	}

	waitSnapshot := func(directory string, minimum uint64) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; {
			items, e := os.ReadDir(filepath.Join(directory, "snapshots"))
			var latest uint64
			pending := false
			if e == nil {
				for _, item := range items {
					if strings.HasSuffix(item.Name(), ".tmp") {
						pending = true
						continue
					}
					if !item.IsDir() {
						continue
					}
					data, e := os.ReadFile(filepath.Join(directory, "snapshots", item.Name(), "meta.json"))
					if e != nil {
						continue
					}
					var meta struct{ Index uint64 }
					if json.Unmarshal(data, &meta) == nil && meta.Index > latest {
						latest = meta.Index
					}
				}
			}
			if latest >= minimum && !pending {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("actual executable snapshot did not complete", latest, minimum)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	convert := func(source, destination, profile, label string) map[string]any {
		t.Helper()
		args := []string{"migrate-format", "-source=" + source, "-destination=" + destination, "-to-profile=" + profile}
		raw, e := exec.CommandContext(ctx, current, args...).CombinedOutput()
		if e != nil {
			t.Fatal("conversion inspection failed", e, string(raw))
		}
		os.WriteFile(filepath.Join(evidence, label+"-plan.json"), raw, 0600)
		var plan struct {
			Approval string `json:"approval_sha256"`
		}
		if e = json.Unmarshal(raw, &plan); e != nil || len(plan.Approval) != 64 {
			t.Fatal("missing conversion plan", e, string(raw))
		}
		raw, e = exec.CommandContext(ctx, current, append(args, "-approve-sha256="+plan.Approval)...).CombinedOutput()
		if e != nil {
			t.Fatal("approved copy conversion failed", e, string(raw))
		}
		os.WriteFile(filepath.Join(evidence, label+"-applied.json"), raw, 0600)
		var result map[string]any
		if e = json.Unmarshal(raw, &result); e != nil || result["status"] != "CONVERTED_COPY_VERIFIED_SOURCE_UNCHANGED" {
			t.Fatal("conversion was not verified", e, string(raw))
		}
		return result
	}
	// Actual N-1 writes -> N reads/writes -> N-1 reads/writes in explicit legacy
	// format. This is compatibility, not conversion to the new encoding.
	legacyDir := filepath.Join(root, "legacy")
	legacyAddress := address()
	old := start(previous, legacyDir, legacyAddress, "compat-legacy", "previous-legacy", false, true)
	seed(old)
	waitSnapshot(legacyDir, 5)
	stop(old)
	now := start(current, legacyDir, legacyAddress, "compat-legacy", "current-legacy", false, false)
	get(now, "retained", "ack", true)
	get(now, "deleted", "", false)
	put(now, "new-write", "new-ack")
	waitSnapshot(legacyDir, 7)
	stop(now)
	old = start(previous, legacyDir, legacyAddress, "compat-legacy", "previous-legacy-again", false, false)
	get(old, "new-write", "new-ack", true)
	get(old, "deleted", "", false)
	put(old, "old-again", "old-ack")
	waitSnapshot(legacyDir, 9)
	stop(old)
	old = start(previous, legacyDir, legacyAddress, "compat-legacy", "previous-tail", false, false)
	put(old, "legacy-tail", "tail-ack")
	stop(old)

	legacyBefore := compatibilityMemberHashes(t, legacyDir)
	upgraded := filepath.Join(root, "converted-versioned")
	forward := convert(legacyDir, upgraded, "versioned", "forward-conversion")
	if !reflect.DeepEqual(legacyBefore, compatibilityMemberHashes(t, legacyDir)) {
		t.Fatal("CLI forward conversion changed actual previous-binary source")
	}
	now = start(current, upgraded, legacyAddress, "compat-legacy", "current-converted", true, false)
	get(now, "retained", "ack", true)
	get(now, "deleted", "", false)
	get(now, "new-write", "new-ack", true)
	get(now, "old-again", "old-ack", true)
	get(now, "legacy-tail", "tail-ack", true)
	put(now, "converted-write", "versioned-ack")
	stop(now)
	upgradedBefore := compatibilityMemberHashes(t, upgraded)
	downgraded := filepath.Join(root, "converted-legacy")
	backward := convert(upgraded, downgraded, "legacy", "backward-conversion")
	if !reflect.DeepEqual(upgradedBefore, compatibilityMemberHashes(t, upgraded)) {
		t.Fatal("CLI backward conversion changed versioned source")
	}
	old = start(previous, downgraded, legacyAddress, "compat-legacy", "previous-converted-back", false, false)
	get(old, "retained", "ack", true)
	get(old, "deleted", "", false)
	get(old, "converted-write", "versioned-ack", true)
	get(old, "legacy-tail", "tail-ack", true)
	put(old, "after-converted-downgrade", "old-ack")
	get(old, "after-converted-downgrade", "old-ack", true)
	stop(old)
	for _, proof := range []map[string]any{forward, backward} {
		p, ok := proof["proof"].(map[string]any)
		if !ok || p["Snapshots"].(float64) < 1 || p["Commands"].(float64) < 1 {
			t.Fatal("conversion did not exercise snapshot plus command tail", proof)
		}
	}
	// A true previous binary must refuse the new profile before changing any
	// member data, after which the current binary can still restart and write.
	versionedDir := filepath.Join(root, "versioned")
	versionedAddress := address()
	now = start(current, versionedDir, versionedAddress, "compat-versioned", "current-versioned", true, true)
	seed(now)
	stop(now)
	fingerprints := func() map[string]string { return compatibilityMemberHashes(t, versionedDir) }
	before := fingerprints()
	refuseCtx, refuseCancel := context.WithTimeout(ctx, 5*time.Second)
	refuse := exec.CommandContext(refuseCtx, previous, "-id=one", "-peers=one="+versionedAddress, "-cluster-id=compat-versioned", "-data-dir="+versionedDir, "-disk-watermark-percent=95")
	output, refuseErr := refuse.CombinedOutput()
	refuseCancel()
	os.WriteFile(filepath.Join(evidence, "previous-refusal.log"), output, 0600)
	if refuseErr == nil || !strings.Contains(string(output), "persisted manifest") || !reflect.DeepEqual(before, fingerprints()) {
		t.Fatal("previous executable did not preserve/refuse new profile", refuseErr, string(output))
	}
	now = start(current, versionedDir, versionedAddress, "compat-versioned", "current-after-refusal", true, false)
	get(now, "retained", "ack", true)
	get(now, "deleted", "", false)
	put(now, "after-refusal", "ack")
	get(now, "after-refusal", "ack", true)
	stop(now)
	report := map[string]any{"status": "PASS_LOCAL_EXECUTABLE_COMPATIBILITY_AND_REFUSAL", "previous_sha256": previousHash, "current_sha256": currentHash, "legacy_n_minus_1_to_n_read_write": "PASS", "legacy_n_to_n_minus_1_read_write": "PASS", "new_profile_previous_executable_refusal": "PASS", "refused_member_files_unchanged": before, "current_restart_after_refusal": "PASS", "format_conversion": "PASS_LOCAL_FORWARD_AND_BACKWARD_COPY", "new_profile_writable_downgrade": "PASS_LOCAL_PREVIOUS_EXECUTABLE_ON_CONVERTED_COPY", "forward_conversion": forward, "backward_conversion": backward, "termination_scope": "owned child processes killed after mutation acknowledgements; not power loss"}
	raw, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(filepath.Join(evidence, "compatibility.json"), raw, 0600); e != nil {
		t.Fatal(e)
	}
	t.Logf("previous_sha256=%s current_sha256=%s legacy_bidirectional_read_write=true versioned_profile_previous_refusal=true bytes_preserved=true conversion=PASS_LOCAL_FORWARD_BACKWARD_COPY", previousHash, currentHash)
}

func compatibilityMemberHashes(t *testing.T, directory string) map[string]string {
	t.Helper()
	result := map[string]string{}
	e := filepath.WalkDir(directory, func(path string, entry os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected fixture symlink")
		}
		rel, e := filepath.Rel(directory, path)
		if e != nil {
			return e
		}
		if rel == "member.lock" {
			return nil
		}
		data, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		h := sha256.Sum256(data)
		result[filepath.ToSlash(rel)] = hex.EncodeToString(h[:])
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	return result
}
