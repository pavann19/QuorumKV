// Package identity supplies operator-configured mTLS and method authorization.
// Trust roots and allowlists are immutable for a process lifetime. Leaf files
// are reloaded on new handshakes so rotation never requires a plaintext fallback.
package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const MaxMessage = 4 << 20

type Config struct {
	Certificate string            `json:"certificate"`
	Key         string            `json:"key"`
	CA          string            `json:"ca"`
	Self        string            `json:"self"`    // spiffe://quorumkv/peer/<member> or /client/<name>
	Peers       map[string]string `json:"peers"`   // exact dial address -> member URI
	Clients     map[string]string `json:"clients"` // exact client URI -> read or write
}

type Identity struct {
	cfg   Config
	roots *x509.CertPool
}

func Load(path string) (*Identity, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 65537))
	d.DisallowUnknownFields()
	var cfg Config
	if err := d.Decode(&cfg); err != nil {
		return nil, err
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, errors.New("trailing or oversized identity configuration")
	}
	return New(cfg)
}

func validURI(s, role string) bool {
	prefix := "spiffe://quorumkv/" + role + "/"
	if !strings.HasPrefix(s, prefix) {
		return false
	}
	name := strings.TrimPrefix(s, prefix)
	if len(name) == 0 || len(name) > 64 {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

func New(cfg Config) (*Identity, error) {
	if !validURI(cfg.Self, "peer") && !validURI(cfg.Self, "client") {
		return nil, errors.New("invalid self identity")
	}
	// Copy mutable caller-owned maps before admitting requests.
	peers, clients := map[string]string{}, map[string]string{}
	for addr, id := range cfg.Peers {
		if _, _, err := net.SplitHostPort(addr); err != nil || !validURI(id, "peer") {
			return nil, fmt.Errorf("invalid peer binding %q", addr)
		}
		peers[addr] = id
	}
	for id, role := range cfg.Clients {
		if !validURI(id, "client") || (role != "read" && role != "write") {
			return nil, errors.New("invalid client policy")
		}
		clients[id] = role
	}
	cfg.Peers, cfg.Clients = peers, clients
	pem, err := os.ReadFile(cfg.CA)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, errors.New("empty CA pool")
	}
	i := &Identity{cfg: cfg, roots: roots}
	if _, err := i.leaf(); err != nil {
		return nil, err
	}
	return i, nil
}

func certID(c *x509.Certificate) string {
	if len(c.URIs) != 1 {
		return ""
	}
	return c.URIs[0].String()
}
func (i *Identity) leaf() (*tls.Certificate, error) {
	c, err := tls.LoadX509KeyPair(i.cfg.Certificate, i.cfg.Key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(c.Certificate[0])
	if err != nil {
		return nil, err
	}
	if certID(cert) != i.cfg.Self {
		return nil, errors.New("leaf identity differs from configured self")
	}
	now := time.Now()
	if now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
		return nil, errors.New("local certificate expired or not yet valid")
	}
	return &c, nil
}
func (i *Identity) known(id string) bool {
	if _, ok := i.cfg.Clients[id]; ok {
		return true
	}
	for _, p := range i.cfg.Peers {
		if id == p {
			return true
		}
	}
	return false
}
func (i *Identity) verify(state tls.ConnectionState, expected string, usage x509.ExtKeyUsage) error {
	if len(state.PeerCertificates) == 0 {
		return errors.New("client certificate required")
	}
	leaf := state.PeerCertificates[0]
	intermediates := x509.NewCertPool()
	for _, c := range state.PeerCertificates[1:] {
		intermediates.AddCert(c)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: i.roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
		return err
	}
	id := certID(leaf)
	if expected != "" {
		if id != expected {
			return errors.New("target identity mismatch")
		}
	} else if !i.known(id) {
		return errors.New("identity not allowed")
	}
	return nil
}

func (i *Identity) ServerCredentials() credentials.TransportCredentials {
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: i.roots, SessionTicketsDisabled: true,
		GetCertificate:   func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return i.leaf() },
		VerifyConnection: func(s tls.ConnectionState) error { return i.verify(s, "", x509.ExtKeyUsageClientAuth) }}
	return credentials.NewTLS(cfg)
}

// The adopted Raft manager shares dial options across all targets. Resolve the
// expected URI from each handshake authority rather than accepting any peer.
type outbound struct{ i *Identity }

func (i *Identity) ClientCredentials() credentials.TransportCredentials { return &outbound{i} }
func (c *outbound) ClientHandshake(ctx context.Context, authority string, conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	expected, ok := c.i.cfg.Peers[authority]
	if !ok {
		return nil, nil, errors.New("unconfigured peer authority")
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, InsecureSkipVerify: true, // Chain and exact URI verified below, never skipped.
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return c.i.leaf() },
		VerifyConnection:     func(s tls.ConnectionState) error { return c.i.verify(s, expected, x509.ExtKeyUsageServerAuth) }}
	return credentials.NewTLS(cfg).ClientHandshake(ctx, authority, conn)
}
func (c *outbound) ServerHandshake(net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, errors.New("outbound credentials only")
}
func (c *outbound) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "tls", SecurityVersion: "1.3"}
}
func (c *outbound) Clone() credentials.TransportCredentials { return &outbound{c.i} }
func (c *outbound) OverrideServerName(string) error         { return errors.New("identity override forbidden") }

func (i *Identity) Authorize(ctx context.Context, method string) error {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "mTLS required")
	}
	auth, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return status.Error(codes.Unauthenticated, "mTLS required")
	}
	if err := i.verify(auth.State, "", x509.ExtKeyUsageClientAuth); err != nil {
		return status.Error(codes.Unauthenticated, "identity invalid or expired")
	}
	id := certID(auth.State.PeerCertificates[0])
	if strings.HasPrefix(method, "/RaftTransport/") && validURI(id, "peer") {
		return nil
	}
	role := i.cfg.Clients[id]
	if method == "/quorumkv.v1.KV/Get" && (role == "read" || role == "write") {
		return nil
	}
	if (method == "/quorumkv.v1.KV/Put" || method == "/quorumkv.v1.KV/Delete") && role == "write" {
		return nil
	}
	return status.Error(codes.PermissionDenied, "method not authorized")
}
func (i *Identity) Unary(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if err := i.Authorize(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return next(ctx, req)
}

type checkedStream struct {
	grpc.ServerStream
	i      *Identity
	method string
}

func (s *checkedStream) RecvMsg(m any) error {
	if err := s.i.Authorize(s.Context(), s.method); err != nil {
		return err
	}
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}
	return s.i.Authorize(s.Context(), s.method)
}
func (s *checkedStream) SendMsg(m any) error {
	if err := s.i.Authorize(s.Context(), s.method); err != nil {
		return err
	}
	return s.ServerStream.SendMsg(m)
}
func (i *Identity) Stream(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	if err := i.Authorize(stream.Context(), info.FullMethod); err != nil {
		return err
	}
	return next(srv, &checkedStream{stream, i, info.FullMethod})
}

func (i *Identity) CheckMember(id string, peers map[string]string) error {
	if i.cfg.Self != "spiffe://quorumkv/peer/"+id {
		return errors.New("self identity and member ID differ")
	}
	if len(peers) != len(i.cfg.Peers) {
		return errors.New("peer identity bindings differ from membership")
	}
	for addr, id := range peers {
		if i.cfg.Peers[addr] != "spiffe://quorumkv/peer/"+id {
			return errors.New("peer address identity mismatch")
		}
	}
	return nil
}
