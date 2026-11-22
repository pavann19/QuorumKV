package fault

import (
	"sync"
	"testing"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func TestClusterClientConnectionsBoundedPerMember(t *testing.T) {
	c := &Cluster{Nodes: []*Node{{ID: "node1", RealAddr: "127.0.0.1:1"}, {ID: "node2", RealAddr: "127.0.0.1:2"}}}
	first := c.Client(t, "node1")
	clients := make(chan pb.KVClient, 256)
	var group sync.WaitGroup
	for n := 0; n < cap(clients); n++ {
		group.Add(1)
		go func() { defer group.Done(); clients <- c.Client(t, "node1") }()
	}
	group.Wait()
	close(clients)
	unique := map[pb.KVClient]bool{first: true}
	for client := range clients {
		unique[client] = true
	}
	if len(unique) != 1 {
		t.Fatalf("257 polls retained %d clients for one member; want one", len(unique))
	}
	if other := c.Client(t, "node2"); other == first {
		t.Fatal("distinct members shared a connection")
	}
	t.Log("257 polls use one client per member")
}
