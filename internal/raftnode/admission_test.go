package raftnode

import (
	"sync"
	"testing"
)

func TestRequestLimitSharedByAllNodeHandlers(t *testing.T) {
	n := &Node{}
	client, readiness := NewKVServer(n), NewKVServer(n)
	for i := 0; i < 64; i++ {
		if !client.acquire() {
			t.Fatalf("request %d rejected before limit", i)
		}
		defer client.release()
	}
	if readiness.acquire() {
		readiness.release()
		t.Fatal("another handler bypassed the node's outstanding request limit")
	}
	other := NewKVServer(&Node{})
	if !other.acquire() {
		t.Fatal("independent node inherited admission exhaustion")
	}
	other.release()
}

func TestConcurrentHandlerConstructionSharesAdmission(t *testing.T) {
	n := &Node{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	servers := make(chan *KVServer, 64)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			servers <- NewKVServer(n)
		}()
	}
	close(start)
	wg.Wait()
	close(servers)
	want := NewKVServer(n)
	for got := range servers {
		if got != want {
			t.Error("concurrent constructor replaced request admission")
		}
	}
}
