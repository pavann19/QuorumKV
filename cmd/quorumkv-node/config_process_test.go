package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestConfiguredExecutableReportsAndEnforcesEffectiveSettings(t *testing.T) {
	if testing.Short() {
		t.Skip("owned process fixture")
	}
	root := t.TempDir()
	binary := fixtureNodeBinary(t, root)
	nextAddr := func() string {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		lis.Close()
		return addr
	}
	peer, admin := nextAddr(), nextAddr()
	listen := nextAddr()
	cfg := defaultNodeConfig()
	cfg.WALFormatVersion = 1
	cfg.CommandFormatVersion = 1
	cfg.SnapshotFormatVersion = 1
	cfg.ID = "overridden"
	cfg.ReadMode = "barrier"
	cfg.ClusterID = "config-fixture"
	cfg.Peers = "one=" + peer
	cfg.DataDir = filepath.Join(root, "member")
	cfg.AdminAddr = admin
	cfg.ListenAddr = listen
	cfg.Raft.HeartbeatMS = 200
	cfg.Raft.ElectionMS = 300
	cfg.Raft.LeaderLeaseMS = 100
	cfg.Raft.CommitMS = 10
	cfg.Disk.MaxUsedPercent = 95 // Explicit disposable profile; production remains 90.
	cfg.Store.MaxBytes = 32
	cfg.Store.MaxEntries = 4
	cfg.Requests.KeyBytes = 2
	cfg.Requests.ValueBytes = 3
	cfg.Requests.Inflight = 2
	cfg.Requests.Streams = 7
	cfg.Requests.TimeoutMS = 500
	raw, _ := json.Marshal(cfg)
	path := filepath.Join(root, "node.json")
	os.WriteFile(path, raw, 0600)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-config="+path, "-id=one", "-read-mode=quorum", "-max-store-bytes=64", "-bootstrap", "-bootstrap-expect=1")
	log, err := os.Create(filepath.Join(root, "node.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout, cmd.Stderr = log, log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	client := &http.Client{Timeout: time.Second}
	var effective map[string]any
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, e := client.Get("http://" + admin + "/effective-config")
		if e == nil {
			err = json.NewDecoder(response.Body).Decode(&effective)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			break
		}
		if time.Now().After(deadline) {
			data, _ := os.ReadFile(filepath.Join(root, "node.log"))
			t.Fatalf("config endpoint unavailable: %s", data)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for name, want := range map[string]float64{"command_format_version": 1, "snapshot_format_version": 1, "wal_format_version": 1, "max_key_bytes": 2, "max_value_bytes": 3, "max_inflight_kv": 2, "max_streams": 7, "rpc_deadline_ms": 500, "max_store_bytes": 64, "disk_watermark_percent": 95} {
		if effective[name] != want {
			t.Fatalf("%s: got %v want %v", name, effective[name], want)
		}
	}
	nested, ok := effective["node_configuration"].(map[string]any)
	if effective["read_mode"] != "quorum" {
		t.Fatal("read mode flag did not override file", effective["read_mode"])
	}
	if !ok || nested["read_mode"] != "quorum" || nested["listen_addr"] != listen || nested["peers"] != "one="+peer {
		t.Fatalf("advertised/listen config missing: %v", nested)
	}
	raftConfig, ok := effective["raft"].(map[string]any)
	if !ok || raftConfig["heartbeat_timeout_ms"] != float64(200) || raftConfig["commit_timeout_ms"] != float64(10) {
		t.Fatalf("effective Raft settings missing: %v", raftConfig)
	}
	cfg.ID = "one"
	cfg.ReadMode = "quorum"
	cfg.Store.MaxBytes = 64
	if effective["configuration_sha256"] != fmt.Sprintf("%x", cfg.hash("")) {
		t.Fatal("effective hash does not identify merged config")
	}
	conn, err := grpc.NewClient(listen, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	kv := pb.NewKVClient(conn)
	if _, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("abc"), Value: []byte("v")}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("configured key bound bypassed: %v", err)
	}
	for {
		_, err = kv.Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("123")})
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("bounded write unavailable: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, err := kv.Get(ctx, &pb.GetRequest{Key: []byte("k")})
	if err != nil || !got.Found || string(got.Value) != "123" {
		t.Fatalf("configured readback: %v %v", got, err)
	}
	// Unknown fields must fail before a new member path can be created.
	deniedDir := filepath.Join(root, "must-not-exist")
	bad := filepath.Join(root, "bad.json")
	os.WriteFile(bad, []byte(`{"unexpected":true}`), 0600)
	denied := exec.CommandContext(ctx, binary, "-config="+bad, "-id=one", "-cluster-id=test", "-peers=one="+peer, "-data-dir="+deniedDir)
	if err = denied.Run(); err == nil {
		t.Fatal("unknown configuration accepted")
	}
	if _, err = os.Stat(deniedDir); !os.IsNotExist(err) {
		t.Fatal("invalid configuration created member data")
	}
	cfg.DataDir = deniedDir
	cfg.Raft.LeaderLeaseMS = cfg.Raft.HeartbeatMS + 1
	invalid, _ := json.Marshal(cfg)
	os.WriteFile(bad, invalid, 0600)
	denied = exec.CommandContext(ctx, binary, "-config="+bad, "-bootstrap", "-bootstrap-expect=1")
	if err = denied.Run(); err == nil {
		t.Fatal("invalid Raft settings started")
	}
	if _, err = os.Stat(deniedDir); !os.IsNotExist(err) {
		t.Fatal("semantic configuration error touched member data")
	}

}
