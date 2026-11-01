package identity

import (
	"context"
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	transportpb "github.com/Jille/raft-grpc-transport/proto"
	"github.com/pavann19/quorumkv/internal/kv"
	"github.com/pavann19/quorumkv/internal/labpki"
	"github.com/pavann19/quorumkv/internal/store"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestMTLSAuthorizationAndRotation(t *testing.T) {
	dir := t.TempDir()
	ca, err := labpki.New()
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, ca.PEM, 0600); err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	serverID := "spiffe://quorumkv/peer/node1"
	writer := "spiffe://quorumkv/client/writer"
	reader := "spiffe://quorumkv/client/reader"
	cert, key, err := ca.Issue(dir, "server", serverID, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ident, err := New(Config{Certificate: cert, Key: key, CA: caPath, Self: serverID, Peers: map[string]string{addr: serverID}, Clients: map[string]string{writer: "write", reader: "read"}})
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "store.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := grpc.NewServer(grpc.Creds(ident.ServerCredentials()), grpc.UnaryInterceptor(ident.Unary), grpc.StreamInterceptor(ident.Stream))
	pb.RegisterKVServer(server, kv.New(st))
	transportpb.RegisterRaftTransportServer(server, &transportpb.UnimplementedRaftTransportServer{})
	go server.Serve(lis)
	defer server.Stop()
	dial := func(id string, issuer *labpki.CA, expected string, expires time.Time, raw bool) *grpc.ClientConn {
		t.Helper()
		c, k, e := issuer.Issue(dir, "client", id, expires)
		if e != nil {
			t.Fatal(e)
		}
		var creds credentials.TransportCredentials
		if raw {
			pair, e := tls.LoadX509KeyPair(c, k)
			if e != nil {
				t.Fatal(e)
			}
			creds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true, Certificates: []tls.Certificate{pair}})
		} else {
			client, e := New(Config{Certificate: c, Key: k, CA: caPath, Self: id, Peers: map[string]string{addr: expected}})
			if e != nil {
				t.Fatal(e)
			}
			creds = client.ClientCredentials()
		}
		conn, e := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}
	put := func(conn *grpc.ClientConn, want codes.Code) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
		defer cancel()
		_, e := pb.NewKVClient(conn).Put(ctx, &pb.PutRequest{Key: []byte("k"), Value: []byte("v")})
		if status.Code(e) != want {
			t.Fatalf("put code %s want %s: %v", status.Code(e), want, e)
		}
	}
	conn := dial(writer, ca, serverID, time.Now().Add(time.Hour), false)
	put(conn, codes.OK)
	ro := dial(reader, ca, serverID, time.Now().Add(time.Hour), false)
	put(ro, codes.PermissionDenied)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := transportpb.NewRaftTransportClient(ro).RequestVote(ctx, &transportpb.RequestVoteRequest{}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("client reached consensus handler: %v", err)
	}
	if stream, err := transportpb.NewRaftTransportClient(ro).InstallSnapshot(ctx); err == nil {
		_, err = stream.CloseAndRecv()
		if status.Code(err) != codes.PermissionDenied {
			t.Fatalf("client reached snapshot stream: %v", err)
		}
	}
	put(dial("spiffe://quorumkv/client/unknown", ca, serverID, time.Now().Add(time.Hour), false), codes.Unavailable)
	rogue, _ := labpki.New()
	put(dial(writer, rogue, serverID, time.Now().Add(time.Hour), true), codes.Unavailable)
	put(dial(writer, ca, serverID, time.Now().Add(-time.Hour), true), codes.Unavailable)
	put(dial(writer, ca, "spiffe://quorumkv/peer/wrong", time.Now().Add(time.Hour), false), codes.Unavailable)
	// A peer certificate does not authorize client writes.
	put(dial(serverID, ca, serverID, time.Now().Add(time.Hour), false), codes.PermissionDenied)
	// Rotate leaf/key files in place; new handshakes must present the new leaf.
	oldPair, _ := tls.LoadX509KeyPair(cert, key)
	if _, _, err := ca.Issue(dir, "server", serverID, time.Now().Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	newPair, _ := tls.LoadX509KeyPair(cert, key)
	if string(oldPair.Certificate[0]) == string(newPair.Certificate[0]) {
		t.Fatal("rotation did not change leaf")
	}
	put(dial(writer, ca, serverID, time.Now().Add(time.Hour), false), codes.OK)
	if value, ok := st.Get([]byte("k")); !ok || string(value) != "v" {
		t.Fatal("authorized state lost")
	}
}
