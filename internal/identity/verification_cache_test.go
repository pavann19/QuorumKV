package identity

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net/url"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type testIssuer struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func issueTestCertificate(t *testing.T, parent *testIssuer, serial int64, id string, ca bool, expires time.Time, usages ...x509.ExtKeyUsage) *testIssuer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	uri, err := url.Parse(id)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: expires, BasicConstraintsValid: true, IsCA: ca, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: usages, URIs: []*url.URL{uri}}
	if ca {
		template.KeyUsage |= x509.KeyUsageCertSign
	}
	issuer, signer := template, key
	if parent != nil {
		issuer, signer = parent.cert, parent.key
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, issuer, &key.PublicKey, signer)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	return &testIssuer{cert, key}
}

func TestVerificationCachePreservesTrustUsageIdentityAndPermissions(t *testing.T) {
	now := time.Now()
	root := issueTestCertificate(t, nil, 1, "spiffe://quorumkv/peer/root", true, now.Add(24*time.Hour))
	writer := "spiffe://quorumkv/client/writer"
	leaf := issueTestCertificate(t, root, 2, writer, false, now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	roots := x509.NewCertPool()
	roots.AddCert(root.cert)
	i := &Identity{roots: roots, cfg: Config{Clients: map[string]string{writer: "read"}}}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf.cert}}
	if err := i.verifyAt(state, "", x509.ExtKeyUsageClientAuth, now); err != nil {
		t.Fatal(err)
	}
	if err := i.verifyAt(state, "", x509.ExtKeyUsageServerAuth, now); err == nil {
		t.Fatal("client-auth proof authorized server-auth")
	}
	if err := i.verifyAt(state, "spiffe://quorumkv/peer/wrong", x509.ExtKeyUsageClientAuth, now); err == nil {
		t.Fatal("cached proof ignored exact target")
	}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: state}})
	if err := i.Authorize(ctx, "/quorumkv.v1.KV/Get"); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"/quorumkv.v1.KV/Put", "/RaftTransport/AppendEntries"} {
		if status.Code(i.Authorize(ctx, method)) != codes.PermissionDenied {
			t.Fatal("cached proof bypassed method role", method)
		}
	}
	rogueRoot := issueTestCertificate(t, nil, 3, "spiffe://quorumkv/peer/root", true, now.Add(24*time.Hour))
	rogue := issueTestCertificate(t, rogueRoot, 2, writer, false, now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
	state.PeerCertificates = []*x509.Certificate{rogue.cert}
	if err := i.verifyAt(state, "", x509.ExtKeyUsageClientAuth, now); err == nil {
		t.Fatal("same URI from rogue CA reused proof")
	}
	if len(i.verified.entries) != 1 {
		t.Fatal("failed verification populated cache")
	}
}

func TestVerificationCacheChecksLeafIntermediateAndRootValidity(t *testing.T) {
	for _, expiring := range []string{"leaf", "intermediate", "root"} {
		t.Run(expiring, func(t *testing.T) {
			now := time.Now().Truncate(time.Second)
			expires := func(name string) time.Time {
				if name == expiring {
					return now.Add(30 * time.Minute)
				}
				return now.Add(4 * time.Hour)
			}
			root := issueTestCertificate(t, nil, 1, "spiffe://quorumkv/peer/root", true, expires("root"))
			intermediate := issueTestCertificate(t, root, 2, "spiffe://quorumkv/peer/intermediate", true, expires("intermediate"))
			writer := "spiffe://quorumkv/client/writer"
			leaf := issueTestCertificate(t, intermediate, 3, writer, false, expires("leaf"), x509.ExtKeyUsageClientAuth)
			roots := x509.NewCertPool()
			roots.AddCert(root.cert)
			i := &Identity{roots: roots, cfg: Config{Clients: map[string]string{writer: "write"}}}
			state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf.cert, intermediate.cert}}
			if err := i.verifyAt(state, "", x509.ExtKeyUsageClientAuth, now); err != nil {
				t.Fatal(err)
			}
			if err := i.verifyAt(state, "", x509.ExtKeyUsageClientAuth, now.Add(31*time.Minute)); err == nil {
				t.Fatal("cached chain outlived", expiring)
			}
			if err := i.verifyAt(state, "", x509.ExtKeyUsageClientAuth, now.Add(-2*time.Hour)); err == nil {
				t.Fatal("cached chain survived time before NotBefore")
			}
			// A proof cannot cross Identity instances with different trust roots.
			other := &Identity{roots: x509.NewCertPool(), cfg: i.cfg}
			if err := other.verifyAt(state, "", x509.ExtKeyUsageClientAuth, now); err == nil {
				t.Fatal("proof leaked across trust stores")
			}
		})
	}
}

func TestVerificationCacheBoundedConcurrentAndRotation(t *testing.T) {
	now := time.Now()
	root := issueTestCertificate(t, nil, 1, "spiffe://quorumkv/peer/root", true, now.Add(24*time.Hour))
	roots := x509.NewCertPool()
	roots.AddCert(root.cert)
	writer := "spiffe://quorumkv/client/writer"
	i := &Identity{roots: roots, cfg: Config{Clients: map[string]string{writer: "write"}}}
	states := make([]tls.ConnectionState, maxVerifiedChains+4)
	for n := range states {
		leaf := issueTestCertificate(t, root, int64(n+2), writer, false, now.Add(time.Hour), x509.ExtKeyUsageClientAuth)
		states[n] = tls.ConnectionState{PeerCertificates: []*x509.Certificate{leaf.cert}}
		if err := i.verifyAt(states[n], "", x509.ExtKeyUsageClientAuth, now); err != nil {
			t.Fatal(err)
		}
		if len(i.verified.entries) > maxVerifiedChains {
			t.Fatal("unbounded cache")
		}
	}
	if len(i.verified.entries) != maxVerifiedChains {
		t.Fatal("cache bound not exercised")
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := 0; n < 32; n++ {
				if err := i.verifyAt(states[(worker*32+n)%len(states)], "", x509.ExtKeyUsageClientAuth, now); err != nil {
					t.Error(err)
				}
			}
		}(worker)
	}
	wg.Wait()
	if len(i.verified.entries) != maxVerifiedChains {
		t.Fatal("concurrent cache grew")
	}
}
