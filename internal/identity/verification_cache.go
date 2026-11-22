package identity

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"sync"
	"time"
)

// Roots are immutable for this Identity's lifetime. Cache successful chain
// verification only, bounded independently of the number of handshakes. Method
// permissions, exact peer URI and the whole verified chain's validity interval
// are still checked on every invocation, including long-lived streams.
const maxVerifiedChains = 256

type verificationInterval struct{ notBefore, notAfter time.Time }
type verificationCache struct {
	sync.Mutex
	entries map[[32]byte]verificationInterval
	order   [maxVerifiedChains][32]byte
	next    int
}

func chainKey(certificates []*x509.Certificate, usage x509.ExtKeyUsage) [32]byte {
	h := sha256.New()
	var size [8]byte
	binary.LittleEndian.PutUint64(size[:], uint64(usage))
	h.Write(size[:])
	for _, cert := range certificates {
		binary.LittleEndian.PutUint64(size[:], uint64(len(cert.Raw)))
		h.Write(size[:])
		h.Write(cert.Raw)
	}
	var key [32]byte
	h.Sum(key[:0])
	return key
}

func (i *Identity) verifyChain(certificates []*x509.Certificate, usage x509.ExtKeyUsage, now time.Time) error {
	key := chainKey(certificates, usage)
	i.verified.Lock()
	interval, hit := i.verified.entries[key]
	i.verified.Unlock()
	if hit && !now.Before(interval.notBefore) && !now.After(interval.notAfter) {
		return nil
	}
	intermediates := x509.NewCertPool()
	for _, cert := range certificates[1:] {
		intermediates.AddCert(cert)
	}
	chains, err := certificates[0].Verify(x509.VerifyOptions{Roots: i.roots, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{usage}, CurrentTime: now})
	if err != nil {
		return err
	}
	// Any one verified chain suffices. Never reuse its proof outside the
	// intersection of the validity windows of every certificate, including root.
	chain := chains[0]
	interval = verificationInterval{notBefore: chain[0].NotBefore, notAfter: chain[0].NotAfter}
	for _, cert := range chain[1:] {
		if cert.NotBefore.After(interval.notBefore) {
			interval.notBefore = cert.NotBefore
		}
		if cert.NotAfter.Before(interval.notAfter) {
			interval.notAfter = cert.NotAfter
		}
	}
	i.verified.Lock()
	defer i.verified.Unlock()
	if i.verified.entries == nil {
		i.verified.entries = make(map[[32]byte]verificationInterval)
	}
	if _, exists := i.verified.entries[key]; !exists {
		if len(i.verified.entries) == maxVerifiedChains {
			delete(i.verified.entries, i.verified.order[i.verified.next])
		}
		i.verified.order[i.verified.next] = key
		i.verified.next = (i.verified.next + 1) % maxVerifiedChains
	}
	i.verified.entries[key] = interval
	return nil
}
