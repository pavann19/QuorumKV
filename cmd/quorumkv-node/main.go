// Command quorumkv-node runs one node of a Raft-replicated QuorumKV
// cluster. Every node in a fixed-membership cluster is started with the
// same --peers list; see internal/raftnode.New for why calling
// BootstrapCluster on every node with an identical configuration is safe
// (hashicorp/raft's own documented pattern for static clusters).
package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	"github.com/pavann19/quorumkv/internal/raftnode"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func main() {
	var (
		id      = flag.String("id", "", "this node's Raft server ID (must be unique in --peers)")
		peers   = flag.String("peers", "", "comma-separated id=host:port list of every node in the cluster, including this one")
		dataDir = flag.String("data-dir", "", "directory for this node's WAL, Raft log, and snapshots")
	)
	flag.Parse()

	if *id == "" || *peers == "" || *dataDir == "" {
		exitf("usage: quorumkv-node -id=<id> -peers=id1=host:port,id2=host:port,... -data-dir=<dir>")
	}

	peerList, err := parsePeers(*peers)
	if err != nil {
		exitf("parsing -peers: %v", err)
	}

	var self raftnode.Peer
	found := false
	for _, p := range peerList {
		if p.ID == *id {
			self = p
			found = true
			break
		}
	}
	if !found {
		exitf("-id=%q is not present in -peers=%q", *id, *peers)
	}

	node, err := raftnode.New(self, peerList, *dataDir)
	if err != nil {
		exitf("starting node: %v", err)
	}

	lis, err := net.Listen("tcp", self.Addr)
	if err != nil {
		exitf("listening on %s: %v", self.Addr, err)
	}

	grpcServer := grpc.NewServer()
	node.RegisterOn(grpcServer)
	pb.RegisterKVServer(grpcServer, raftnode.NewKVServer(node))

	fmt.Printf("quorumkv-node %s listening on %s (peers=%s, data-dir=%s)\n", self.ID, self.Addr, *peers, *dataDir)
	if err := grpcServer.Serve(lis); err != nil {
		exitf("serving: %v", err)
	}
}

func parsePeers(s string) ([]raftnode.Peer, error) {
	parts := strings.Split(s, ",")
	peers := make([]raftnode.Peer, 0, len(parts))
	for _, part := range parts {
		idAddr := strings.SplitN(part, "=", 2)
		if len(idAddr) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q, want id=host:port", part)
		}
		peers = append(peers, raftnode.Peer{ID: idAddr[0], Addr: idAddr[1]})
	}
	return peers, nil
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
