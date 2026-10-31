// Command quorumkv-node runs one node of a Raft-replicated QuorumKV
// cluster. Every node in a fixed-membership cluster is started with the
// same --peers list; see internal/raftnode.New for why calling
// BootstrapCluster on every node with an identical configuration is safe
// (hashicorp/raft's own documented pattern for static clusters).
package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/pavann19/quorumkv/internal/identity"
	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/telemetry"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func main() {
	var (
		id           = flag.String("id", "", "this node's Raft server ID (must be unique in --peers)")
		peers        = flag.String("peers", "", "comma-separated id=host:port list of every node in the cluster, including this one")
		dataDir      = flag.String("data-dir", "", "directory for this node's WAL, Raft log, and snapshots")
		identityPath = flag.String("identity", "", "operator-owned mTLS identity JSON; mandatory for non-loopback addresses")
		adminAddr    = flag.String("admin-addr", "", "optional literal-loopback HTTP health/metrics listener")
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

	var node *raftnode.Node
	metrics := &telemetry.Metrics{}
	configHash := "loopback-lab-plaintext"
	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(identity.MaxMessage), grpc.MaxSendMsgSize(identity.MaxMessage), grpc.MaxConcurrentStreams(128), grpc.ConnectionTimeout(5 * time.Second)}
	opts = append(opts, grpc.ChainUnaryInterceptor(metrics.Unary))
	if *identityPath != "" {
		ident, e := identity.Load(*identityPath)
		if e != nil {
			exitf("identity: %v", e)
		}
		raw, e := os.ReadFile(*identityPath)
		if e != nil {
			exitf("identity hash: %v", e)
		}
		configHash = fmt.Sprintf("%x", sha256.Sum256(raw))
		bindings := map[string]string{}
		for _, p := range peerList {
			bindings[p.Addr] = p.ID
		}
		if e := ident.CheckMember(*id, bindings); e != nil {
			exitf("identity membership: %v", e)
		}
		node, err = raftnode.NewAuthenticated(self, peerList, *dataDir, ident.ClientCredentials())
		opts = append(opts, grpc.Creds(ident.ServerCredentials()), grpc.ChainUnaryInterceptor(ident.Unary), grpc.ChainStreamInterceptor(ident.Stream))
	} else {
		for _, p := range peerList {
			host, _, e := net.SplitHostPort(p.Addr)
			ip := net.ParseIP(host)
			if e != nil || ip == nil || !ip.IsLoopback() {
				exitf("plaintext lab mode requires literal loopback peer addresses")
			}
		}
		node, err = raftnode.New(self, peerList, *dataDir)
	}
	if err != nil {
		exitf("starting node: %v", err)
	}
	defer node.Shutdown()

	lis, err := net.Listen("tcp", self.Addr)
	if err != nil {
		exitf("listening on %s: %v", self.Addr, err)
	}

	grpcServer := grpc.NewServer(opts...)
	node.RegisterOn(grpcServer)
	pb.RegisterKVServer(grpcServer, raftnode.NewKVServer(node))
	if *adminAddr != "" {
		host, _, e := net.SplitHostPort(*adminAddr)
		ip := net.ParseIP(host)
		if e != nil || ip == nil || !ip.IsLoopback() {
			exitf("admin listener requires literal loopback")
		}
		adminLis, e := net.Listen("tcp", *adminAddr)
		if e != nil {
			exitf("admin listen: %v", e)
		}
		admin := &http.Server{Handler: metrics.Handler(node, configHash), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8192}
		defer admin.Close()
		go admin.Serve(adminLis)
	}
	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-stopCtx.Done()
		done := make(chan struct{})
		go func() { grpcServer.GracefulStop(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			grpcServer.Stop()
		}
	}()

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
		if len(idAddr) != 2 || idAddr[0] == "" {
			return nil, fmt.Errorf("invalid peer entry %q, want id=host:port", part)
		}
		for _, p := range peers {
			if p.ID == idAddr[0] || p.Addr == idAddr[1] {
				return nil, fmt.Errorf("duplicate peer")
			}
		}
		if _, _, err := net.SplitHostPort(idAddr[1]); err != nil {
			return nil, err
		}
		peers = append(peers, raftnode.Peer{ID: idAddr[0], Addr: idAddr[1]})
	}
	return peers, nil
}

func exitf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
