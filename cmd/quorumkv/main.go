// Command quorumkv runs the single-node QuorumKV gRPC server.
package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/pavann19/quorumkv/internal/kv"
	"github.com/pavann19/quorumkv/internal/store"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:7070", "loopback-only legacy single-node lab address")
		walPath = flag.String("wal", "quorumkv.wal", "path to the WAL file")
	)
	flag.Parse()
	host, _, err := net.SplitHostPort(*addr)
	ip := net.ParseIP(host)
	if err != nil || ip == nil || !ip.IsLoopback() {
		exitf("legacy lab server requires literal loopback address; use quorumkv-node with mTLS for network service")
	}

	s, err := store.Open(*walPath)
	if err != nil {
		exitf("opening store: %v", err)
	}
	defer s.Close()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		exitf("listening on %s: %v", *addr, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKVServer(grpcServer, kv.New(s))

	fmt.Printf("quorumkv listening on %s (wal=%s)\n", *addr, *walPath)
	if err := grpcServer.Serve(lis); err != nil {
		exitf("serving: %v", err)
	}
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
