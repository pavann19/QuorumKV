// Package labcluster owns an isolated loopback mTLS cluster and synthetic PKI.
// It accepts no existing member/data directory or external target address.
package labcluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/pavann19/quorumkv/internal/identity"
	"github.com/pavann19/quorumkv/internal/labpki"
	"github.com/pavann19/quorumkv/internal/raftnode"
	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Cluster struct {
	Root           string
	Peers          []raftnode.Peer
	Nodes          []*raftnode.Node
	Servers        []*grpc.Server
	Clients        []pb.KVClient
	conns          []*grpc.ClientConn
	identities     []*identity.Identity
	ca             *labpki.CA
	mtls           bool
	clientIdentity *identity.Identity
}

func New(parent string) (*Cluster, error) {
	return NewMode(parent, true)
}

// NewMode permits a plaintext comparison only for self-created loopback fixtures.
func NewMode(parent string, mtls bool) (*Cluster, error) {
	root, err := os.MkdirTemp(parent, "quorumkv-disposable-")
	if err != nil {
		return nil, err
	}
	c := &Cluster{Root: root, mtls: mtls}
	ok := false
	defer func() {
		if !ok {
			c.Close()
		}
	}()
	if err := os.WriteFile(filepath.Join(root, "LAB_ONLY"), []byte("synthetic isolated loopback fixtures\n"), 0600); err != nil {
		return nil, err
	}
	c.ca, err = labpki.New()
	if err != nil {
		return nil, err
	}
	caPath := filepath.Join(root, "ca.pem")
	if err = os.WriteFile(caPath, c.ca.PEM, 0600); err != nil {
		return nil, err
	}
	listeners := []net.Listener{}
	defer func() {
		if !ok {
			for _, l := range listeners {
				l.Close()
			}
		}
	}()
	bindings := map[string]string{}
	for i := 0; i < 3; i++ {
		lis, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			return nil, e
		}
		listeners = append(listeners, lis)
		id := fmt.Sprintf("node%d", i)
		addr := lis.Addr().String()
		c.Peers = append(c.Peers, raftnode.Peer{ID: id, Addr: addr})
		bindings[addr] = "spiffe://quorumkv/peer/" + id
	}
	c.Nodes = make([]*raftnode.Node, 3)
	c.Servers = make([]*grpc.Server, 3)
	for i, p := range c.Peers {
		cert, key, e := c.ca.Issue(root, p.ID, "spiffe://quorumkv/peer/"+p.ID, time.Now().Add(48*time.Hour))
		if e != nil {
			return nil, e
		}
		cfg := identity.Config{Certificate: cert, Key: key, CA: caPath, Self: "spiffe://quorumkv/peer/" + p.ID, Peers: bindings, Clients: map[string]string{"spiffe://quorumkv/client/lab": "write"}}
		raw, _ := json.MarshalIndent(cfg, "", "  ")
		if e = os.WriteFile(filepath.Join(root, p.ID+"-identity.json"), raw, 0600); e != nil {
			return nil, e
		}
		ident, e := identity.New(cfg)
		if e != nil {
			return nil, e
		}
		c.identities = append(c.identities, ident)
		if e = c.start(i, listeners[i]); e != nil {
			return nil, e
		}
	}
	cert, key, err := c.ca.Issue(root, "client", "spiffe://quorumkv/client/lab", time.Now().Add(48*time.Hour))
	if err != nil {
		return nil, err
	}
	cfg := identity.Config{Certificate: cert, Key: key, CA: caPath, Self: "spiffe://quorumkv/client/lab", Peers: bindings}
	raw, _ := json.MarshalIndent(cfg, "", "  ")
	if err = os.WriteFile(filepath.Join(root, "client-identity.json"), raw, 0600); err != nil {
		return nil, err
	}
	client, err := identity.New(cfg)
	if err != nil {
		return nil, err
	}
	c.clientIdentity = client
	for _, p := range c.Peers {
		creds := client.ClientCredentials()
		if !mtls {
			creds = insecure.NewCredentials()
		}
		conn, e := grpc.NewClient(p.Addr, grpc.WithTransportCredentials(creds), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(identity.MaxMessage), grpc.MaxCallSendMsgSize(identity.MaxMessage)))
		if e != nil {
			return nil, e
		}
		c.conns = append(c.conns, conn)
		c.Clients = append(c.Clients, pb.NewKVClient(conn))
	}
	ok = true
	return c, nil
}
func (c *Cluster) start(i int, lis net.Listener) error {
	var n *raftnode.Node
	var err error
	if c.mtls {
		n, err = raftnode.NewAuthenticated(c.Peers[i], c.Peers, filepath.Join(c.Root, c.Peers[i].ID), c.identities[i].ClientCredentials())
	} else {
		n, err = raftnode.New(c.Peers[i], c.Peers, filepath.Join(c.Root, c.Peers[i].ID))
	}
	if err != nil {
		return err
	}
	opts := []grpc.ServerOption{grpc.MaxRecvMsgSize(identity.MaxMessage), grpc.MaxSendMsgSize(identity.MaxMessage), grpc.MaxConcurrentStreams(128)}
	if c.mtls {
		opts = append(opts, grpc.Creds(c.identities[i].ServerCredentials()), grpc.ChainUnaryInterceptor(c.identities[i].Unary), grpc.ChainStreamInterceptor(c.identities[i].Stream))
	}
	s := grpc.NewServer(opts...)
	n.RegisterOn(s)
	pb.RegisterKVServer(s, raftnode.NewKVServer(n))
	c.Nodes[i], c.Servers[i] = n, s
	go s.Serve(lis)
	return nil
}

func (c *Cluster) ColdClient(i int) (pb.KVClient, func(), error) {
	creds := c.clientIdentity.ClientCredentials()
	if !c.mtls {
		creds = insecure.NewCredentials()
	}
	conn, err := grpc.NewClient(c.Peers[i].Addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewKVClient(conn), func() { conn.Close() }, nil
}
func (c *Cluster) Stop(i int) error {
	if c.Servers[i] != nil {
		c.Servers[i].Stop()
		c.Servers[i] = nil
	}
	if c.Nodes[i] != nil {
		err := c.Nodes[i].Shutdown()
		c.Nodes[i] = nil
		return err
	}
	return nil
}
func (c *Cluster) Restart(i int) error {
	if c.Nodes[i] != nil {
		return errors.New("node already running")
	}
	lis, err := net.Listen("tcp", c.Peers[i].Addr)
	if err != nil {
		return err
	}
	if err = c.start(i, lis); err != nil {
		lis.Close()
	}
	return err
}
func (c *Cluster) Rotate(i int) error {
	_, _, err := c.ca.Issue(c.Root, c.Peers[i].ID, "spiffe://quorumkv/peer/"+c.Peers[i].ID, time.Now().Add(48*time.Hour))
	return err
}
func (c *Cluster) Leader(ctx context.Context) (int, error) {
	for {
		for i, n := range c.Nodes {
			if n != nil && n.IsLeader() {
				return i, nil
			}
		}
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
func (c *Cluster) Close() {
	for _, conn := range c.conns {
		conn.Close()
	}
	for i := range c.Nodes {
		c.Stop(i)
	}
}
