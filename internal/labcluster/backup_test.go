package labcluster

import (
	"context"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestOfflineBackupRestoreExactState(t *testing.T) {
	c, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader, err := c.Leader(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"retained", "deleted"} {
		if _, err = c.Clients[leader].Put(ctx, &pb.PutRequest{Key: []byte(key), Value: []byte("ack")}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = c.Clients[leader].Delete(ctx, &pb.DeleteRequest{Key: []byte("deleted")}); err != nil {
		t.Fatal(err)
	}
	if err = c.Nodes[leader].Raft.Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	// Gracefully stop all owned members, copy every persistent member file to a
	// fresh location, and keep originals untouched. This is not power-loss proof.
	for i := range c.Nodes {
		if err = c.Stop(i); err != nil {
			t.Fatal(err)
		}
	}
	restored := filepath.Join(c.Root, "restored-copy")
	if err = os.Mkdir(restored, 0700); err != nil {
		t.Fatal(err)
	}
	for _, peer := range c.Peers {
		source := filepath.Join(c.Root, peer.ID)
		err = filepath.WalkDir(source, func(path string, d os.DirEntry, e error) error {
			if e != nil {
				return e
			}
			rel, e := filepath.Rel(source, path)
			if e != nil {
				return e
			}
			dst := filepath.Join(restored, peer.ID, rel)
			if d.IsDir() {
				return os.MkdirAll(dst, 0700)
			}
			in, e := os.Open(path)
			if e != nil {
				return e
			}
			defer in.Close()
			out, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				return e
			}
			_, e = io.Copy(out, in)
			if e == nil {
				e = out.Sync()
			}
			closeErr := out.Close()
			if e != nil {
				return e
			}
			return closeErr
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	start := time.Now()
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
	result, err := client.Get(ctx, &pb.GetRequest{Key: []byte("retained")})
	if err != nil || !result.GetFound() || string(result.GetValue()) != "ack" {
		t.Fatalf("restore lost ack: %v %v", result, err)
	}
	result, err = client.Get(ctx, &pb.GetRequest{Key: []byte("deleted")})
	if err != nil || result.Found {
		t.Fatalf("restore resurrected deletion: %v %v", result, err)
	}
	actual := map[string]string{}
	for _, e := range c.Nodes[leader].Store.Dump() {
		actual[string(e.Key)] = string(e.Value)
	}
	if !reflect.DeepEqual(actual, map[string]string{"retained": "ack"}) {
		t.Fatalf("restored keyspace: %v", actual)
	}
	if _, err = client.Put(ctx, &pb.PutRequest{Key: []byte("after-restore"), Value: []byte("new")}); err != nil {
		t.Fatal(err)
	}
	t.Logf("offline_backup_restore_seconds=%.6f retained=1 deletions=1 exact_keyspace=true acknowledged_prefix_preserved=true", time.Since(start).Seconds())
}
