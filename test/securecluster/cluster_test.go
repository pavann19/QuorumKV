package securecluster

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/identity"
	"github.com/pavann19/quorumkv/internal/labcluster"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"github.com/pavann19/quorumkv/test/fault"
	"google.golang.org/grpc"
)

func TestProcessMTLSKillLeaderAndRotation(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process synthetic fixture")
	}
	root := t.TempDir()
	binary := os.Getenv("QUORUMKV_NODE_BINARY")
	if binary == "" {
		binary = filepath.Join(root, "node.exe")
		build := exec.Command("go", "build", "-o", binary, "../../cmd/quorumkv-node")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build %v %s", err, out)
		}
	} else if info, err := os.Stat(binary); err != nil || info.IsDir() {
		t.Fatalf("precompiled owned-fixture executable invalid: %v", err)
	}

	// Validate the real executable refuses exposure before opening a data store.
	deniedPath := filepath.Join(root, "must-not-exist")
	denied := exec.Command(binary, "-id=node0", "-peers=node0=0.0.0.0:19099", "-data-dir="+deniedPath)
	if err := denied.Run(); err == nil {
		t.Fatal("plaintext exposure accepted")
	}
	if _, err := os.Stat(deniedPath); !os.IsNotExist(err) {
		t.Fatal("rejected invocation touched data")
	}
	c, err := labcluster.New(root)
	if err != nil {
		t.Fatal(err)
	}
	c.Close() // Retain only owned generated PKI/data/addresses.
	commands := make([]*exec.Cmd, 3)
	admin := make([]string, 3)
	for i := range admin {
		lis, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		admin[i] = lis.Addr().String()
		lis.Close()
	}
	stop := func(i int) {
		if commands[i] != nil && commands[i].ProcessState == nil {
			commands[i].Process.Kill()
			commands[i].Wait()
		}
	}
	defer func() {
		for i := range commands {
			stop(i)
		}
	}()
	spec := ""
	for _, p := range c.Peers {
		if spec != "" {
			spec += ","
		}
		spec += p.ID + "=" + p.Addr
	}
	launch := func(i int) {
		p := c.Peers[i]
		cmd := exec.Command(binary, "-disk-watermark-percent=95", "-cluster-id="+filepath.Base(c.Root), "-id="+p.ID, "-peers="+spec, "-data-dir="+filepath.Join(c.Root, p.ID), "-identity="+filepath.Join(c.Root, p.ID+"-identity.json"), "-admin-addr="+admin[i])
		log, e := os.OpenFile(filepath.Join(root, fmt.Sprintf("node%d.log", i)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { log.Close() })
		cmd.Stdout, cmd.Stderr = log, log
		if e = cmd.Start(); e != nil {
			t.Fatal(e)
		}
		commands[i] = cmd
	}
	for i := range commands {
		launch(i)
	}
	ident, err := identity.Load(filepath.Join(c.Root, "client-identity.json"))
	if err != nil {
		t.Fatal(err)
	}
	clients := []pb.KVClient{}
	ids := []string{}
	for _, p := range c.Peers {
		conn, e := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(ident.ClientCredentials()))
		if e != nil {
			t.Fatal(e)
		}
		defer conn.Close()
		clients = append(clients, pb.NewKVClient(conn))
		ids = append(ids, p.ID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	r := fault.NewRecorder()
	if err = r.Execute(ctx, 0, "put", "retained", "acknowledged", ids, clients, time.Second); err != nil {
		t.Fatal(err)
	}
	findLeader := func() int {
		client := &http.Client{Timeout: 1200 * time.Millisecond}
		for {
			for i, addr := range admin {
				resp, e := client.Get("http://" + addr + "/ready")
				if e == nil {
					resp.Body.Close()
					if resp.StatusCode == 200 {
						return i
					}
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal("no ready leader")
				return -1
			case <-time.After(30 * time.Millisecond):
			}
		}
	}
	leader := findLeader()
	r.Mark("kill_authenticated_leader", ids[leader])
	start := time.Now()
	stop(leader)
	if err = r.Execute(ctx, 0, "put", "after-failure", "new", ids, clients, time.Second); err != nil {
		t.Fatal(err)
	}
	recovery := time.Since(start)
	if recovery > 15*time.Second {
		t.Fatalf("recovery gate: %s", recovery)
	}
	if err = c.Rotate(leader); err != nil {
		t.Fatal(err)
	}
	launch(leader)
	for _, key := range []string{"retained", "after-failure"} {
		if err = r.Execute(ctx, 0, "get", key, "", ids, clients, time.Second); err != nil {
			t.Fatal(err)
		}
	}
	fault.CheckAndCommit(t, "authenticated_leader_kill", r.History(), r.Events()...)
	t.Logf("authenticated_process_kill_first_ack_ms=%.3f rotated_member=%s", float64(recovery)/float64(time.Millisecond), ids[leader])
}
