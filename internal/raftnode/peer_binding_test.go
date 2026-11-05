package raftnode

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/metadata"
)

func TestPeerOutgoingReplacesUntrustedBinding(t *testing.T) {
	original := metadata.Pairs(PeerClusterHeader, "wrong", PeerClusterHeader, "duplicate", PeerProtocolHeader, "invalid", "existing", "keep")
	ctx := metadata.NewOutgoingContext(context.Background(), original)
	result, _ := metadata.FromOutgoingContext(peerOutgoing(ctx, "correct", 1))
	if len(result.Get(PeerClusterHeader)) != 1 || result.Get(PeerClusterHeader)[0] != "correct" || len(result.Get(PeerProtocolHeader)) != 1 || result.Get(PeerProtocolHeader)[0] != PeerProtocolVersion || result.Get("existing")[0] != "keep" {
		t.Fatal("invalid outgoing binding", result)
	}
	if original.Get(PeerClusterHeader)[0] != "wrong" {
		t.Fatal("mutated caller metadata")
	}
}
func TestClusterIDMetadataValidationBeforeStorage(t *testing.T) {
	peer := Peer{ID: "one", Addr: "127.0.0.1:29983"}
	for _, id := range []string{"spaces not allowed", "line\nbreak", "non-ascii-é"} {
		path := filepath.Join(t.TempDir(), "must-not-exist")
		n, err := New(peer, []Peer{peer}, path, StartOptions{ClusterID: id, Bootstrap: true, BootstrapExpect: 1})
		if err == nil {
			n.Shutdown()
			t.Fatal("invalid metadata cluster ID accepted")
		}
		if _, err = os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("invalid cluster ID created storage")
		}
	}
}
