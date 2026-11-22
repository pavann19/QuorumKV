package identity

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/labpki"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

func authorizationFixture(t testing.TB) (*Identity, tls.ConnectionState) {
	t.Helper()
	dir := t.TempDir()
	ca, err := labpki.New()
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, ca.PEM, 0600); err != nil {
		t.Fatal(err)
	}
	writer := "spiffe://quorumkv/client/writer"
	cert, key, err := ca.Issue(dir, "writer", writer, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	i, err := New(Config{Certificate: cert, Key: key, CA: caPath, Self: writer, Clients: map[string]string{writer: "write"}})
	if err != nil {
		t.Fatal(err)
	}
	pair, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	return i, tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf}}
}

// The same benchmark is compiled into before/after binaries. Certificate setup
// is outside timing; every iteration still runs the actual method authorization.
func BenchmarkAuthorize(b *testing.B) {
	i, state := authorizationFixture(b)
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
	if err := i.Authorize(ctx, "/quorumkv.v1.KV/Put"); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if err := i.Authorize(ctx, "/quorumkv.v1.KV/Put"); err != nil {
			b.Fatal(err)
		}
	}
}
