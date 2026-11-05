// Command quorumkv-node runs one member; new clusters require explicit bootstrap.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
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

	"github.com/pavann19/quorumkv/internal/diskhealth"
	"github.com/pavann19/quorumkv/internal/identity"
	"github.com/pavann19/quorumkv/internal/raftnode"
	"github.com/pavann19/quorumkv/internal/requestlimits"
	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/telemetry"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "migrate-format" {
		if err := runFormatMigration(os.Args[2:], os.Stdout); err != nil {
			exitf("format migration refused: %v", err)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "migrate-identity" {
		if err := runIdentityMigration(os.Args[2:], os.Stdout); err != nil {
			exitf("identity migration refused: %v", err)
		}
		return
	}
	defaults := defaultNodeConfig()
	var (
		readMode         = flag.String("read-mode", defaults.ReadMode, "quorum for log-free fenced reads; barrier for compatibility/comparison")
		maxAppendEntries = flag.Int("max-append-entries", defaults.Raft.MaxAppendEntries, "maximum logical replication group, 1..8; wire messages are byte-bounded")
		commandFormat    = flag.Uint("command-format-version", 0, "0 legacy commands; 1 bounded protobuf requires WAL 1 and snapshot 1")
		snapshotFormat   = flag.Uint("snapshot-format-version", 0, "0 legacy compatibility; 1 bounded protobuf snapshots for a fresh qualified cluster")
		walFormat        = flag.Uint("wal-format-version", 0, "0 legacy WAL; 1 cluster-bound versioned WAL for a fresh qualified member")
		configPath       = flag.String("config", "", "strict JSON node configuration; explicit flags override file fields")
		diskWatermark    = flag.Int("disk-watermark-percent", defaults.Disk.MaxUsedPercent, "reject new writes at or above this used-space percent")
		minDiskFree      = flag.Uint64("min-disk-free-bytes", defaults.Disk.MinFreeBytes, "reject new writes below this available-space reserve")
		maxStoreBytes    = flag.Int64("max-store-bytes", defaults.Store.MaxBytes, "maximum logical key+value bytes; identical on every member")
		maxStoreEntries  = flag.Int64("max-store-entries", defaults.Store.MaxEntries, "maximum distinct keys; identical on every member")
		clearFatal       = flag.Bool("clear-fatal", false, "operator-only: archive fatal marker after verified recovery; does not repair storage")
		bootstrap        = flag.Bool("bootstrap", false, "explicitly initialize empty Raft state for a new cluster")
		bootstrapExpect  = flag.Int("bootstrap-expect", 0, "expected number of initial voters; required on bootstrap")
		clusterID        = flag.String("cluster-id", "", "operator-assigned cluster identity; required on every start")
		id               = flag.String("id", "", "this node's Raft server ID (must be unique in --peers)")
		peers            = flag.String("peers", "", "comma-separated id=host:port list of every node in the cluster, including this one")
		dataDir          = flag.String("data-dir", "", "directory for this node's WAL, Raft log, and snapshots")
		identityPath     = flag.String("identity", "", "operator-owned mTLS identity JSON; mandatory for non-loopback addresses")
		listenAddr       = flag.String("listen-addr", "", "listener address; defaults to this member advertised peer address")
		adminAddr        = flag.String("admin-addr", "", "optional literal-loopback HTTP health/metrics listener")
	)
	flag.Parse()

	settings, err := readNodeConfig(*configPath)
	if err != nil {
		exitf("configuration: %v", err)
	}
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "command-format-version":
			if *commandFormat > 1 {
				exitf("unsupported command format version")
			}
			settings.CommandFormatVersion = uint32(*commandFormat)
		case "wal-format-version":
			if *walFormat > 1 {
				exitf("unsupported WAL format version")
			}
			settings.WALFormatVersion = uint32(*walFormat)
		case "max-append-entries":
			settings.Raft.MaxAppendEntries = *maxAppendEntries
		case "read-mode":
			settings.ReadMode = *readMode
		case "snapshot-format-version":
			if *snapshotFormat > 1 {
				exitf("unsupported snapshot format version")
			}
			settings.SnapshotFormatVersion = uint32(*snapshotFormat)
		case "id":
			settings.ID = *id
		case "peers":
			settings.Peers = *peers
		case "data-dir":
			settings.DataDir = *dataDir
		case "cluster-id":
			settings.ClusterID = *clusterID
		case "identity":
			settings.Identity = *identityPath
		case "listen-addr":
			settings.ListenAddr = *listenAddr
		case "admin-addr":
			settings.AdminAddr = *adminAddr
		case "max-store-bytes":
			settings.Store.MaxBytes = *maxStoreBytes
		case "max-store-entries":
			settings.Store.MaxEntries = *maxStoreEntries
		case "disk-watermark-percent":
			settings.Disk.MaxUsedPercent = *diskWatermark
		case "min-disk-free-bytes":
			settings.Disk.MinFreeBytes = *minDiskFree
		}
	})
	if err = settings.validate(); err != nil {
		exitf("configuration: %v", err)
	}
	settings = settings.normalized()
	*listenAddr = settings.ListenAddr
	*id, *peers, *dataDir, *clusterID, *identityPath, *adminAddr = settings.ID, settings.Peers, settings.DataDir, settings.ClusterID, settings.Identity, settings.AdminAddr
	*maxStoreBytes, *maxStoreEntries = settings.Store.MaxBytes, settings.Store.MaxEntries
	*diskWatermark, *minDiskFree = settings.Disk.MaxUsedPercent, settings.Disk.MinFreeBytes

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

	start := raftnode.StartOptions{CommandFormatVersion: settings.CommandFormatVersion, WALFormatVersion: settings.WALFormatVersion, ReadMode: settings.ReadMode, SnapshotFormatVersion: settings.SnapshotFormatVersion, RaftSettings: settings.Raft, RequestLimits: settings.Requests, DiskPolicy: diskhealth.Policy{MaxUsedPercent: *diskWatermark, MinFreeBytes: *minDiskFree}, StoreLimits: store.Limits{MaxBytes: *maxStoreBytes, MaxEntries: *maxStoreEntries}, ClearFatal: *clearFatal, Bootstrap: *bootstrap, BootstrapExpect: *bootstrapExpect, ClusterID: *clusterID}
	var node *raftnode.Node
	metrics := &telemetry.Metrics{}
	configHash := fmt.Sprintf("%x", settings.hash(""))
	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(identity.MaxMessage), grpc.MaxSendMsgSize(identity.MaxMessage), grpc.MaxConcurrentStreams(settings.Requests.Streams), grpc.ConnectionTimeout(requestlimits.RPCWait)}
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
		identityDigest := fmt.Sprintf("%x", sha256.Sum256(raw))
		configHash = fmt.Sprintf("%x", settings.hash(identityDigest))
		bindings := map[string]string{}
		for _, p := range peerList {
			bindings[p.Addr] = p.ID
		}
		if e := ident.CheckMember(*id, bindings); e != nil {
			exitf("identity membership: %v", e)
		}
		node, err = raftnode.NewAuthenticated(self, peerList, *dataDir, ident.ClientCredentials(), start)
		opts = append(opts, grpc.Creds(ident.ServerCredentials()), grpc.ChainUnaryInterceptor(ident.Unary), grpc.ChainStreamInterceptor(ident.Stream))
	} else {
		for _, p := range peerList {
			host, _, e := net.SplitHostPort(p.Addr)
			ip := net.ParseIP(host)
			if e != nil || ip == nil || !ip.IsLoopback() {
				exitf("plaintext lab mode requires literal loopback peer addresses")
			}
		}
		node, err = raftnode.New(self, peerList, *dataDir, start)
	}
	if err != nil {
		if errors.Is(err, raftnode.ErrFatalState) {
			fmt.Fprintln(os.Stderr, "fatal state: preserve member directory and follow recovery runbook")
			os.Exit(raftnode.FatalExitCode)
		}
		exitf("starting node: %v", err)
	}
	defer node.Shutdown()

	lis, err := net.Listen("tcp", settings.ListenAddr)
	if err != nil {
		exitf("listening on %s: %v", settings.ListenAddr, err)
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
		admin := &http.Server{Handler: metrics.Handler(node, configHash, settings), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8192}
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

	fmt.Printf("configuration_sha256=%s\n", configHash)
	fmt.Printf("quorumkv-node %s listening on %s (advertise=%s, peers=%s, data-dir=%s)\n", self.ID, settings.ListenAddr, self.Addr, *peers, *dataDir)
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
