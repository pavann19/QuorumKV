// Package faultproxy implements a Toxiproxy-style TCP relay: a proxy sits
// on a directed edge between two nodes and can cut, restore, or delay
// traffic on that edge under test control. This is the "Toxiproxy-style
// proxy between nodes" alternative the build plan itself names, used
// because there's no Docker/containers available in this environment to
// do partitioning via iptables between containers (see docs/DECISIONS.md).
//
// One DirectedEdge exists per (from-node, to-node) pair. Since each node in
// test/fault's cluster is bootstrapped with its OWN local view of every
// peer's address (see docs/DECISIONS.md's "per-node-perspective Raft
// configuration" section for why that's safe here), routing every node's
// traffic to a given peer through a dedicated edge proxy gives independent
// control over each direction of each link -- e.g. cutting node1->node2
// without affecting node3->node2 -- with no need for source-port
// fingerprinting or deep packet inspection.
package faultproxy

import (
	"io"
	"net"
	"sync"
	"time"
)

// DirectedEdge relays TCP connections from whoever dials its listen
// address to a fixed target address, and can be told to stop relaying
// (simulating a cut link) or add latency (simulating a slow link).
type DirectedEdge struct {
	ListenAddr string
	TargetAddr string

	mu      sync.Mutex
	cut     bool
	delay   time.Duration
	conns   map[net.Conn]struct{}
	lis     net.Listener
	closeCh chan struct{}
}

// NewDirectedEdge starts listening on listenAddr and relaying accepted
// connections to targetAddr. Call Close when done.
func NewDirectedEdge(listenAddr, targetAddr string) (*DirectedEdge, error) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	e := &DirectedEdge{
		ListenAddr: lis.Addr().String(),
		TargetAddr: targetAddr,
		conns:      make(map[net.Conn]struct{}),
		lis:        lis,
		closeCh:    make(chan struct{}),
	}
	go e.acceptLoop()
	return e, nil
}

func (e *DirectedEdge) acceptLoop() {
	for {
		conn, err := e.lis.Accept()
		if err != nil {
			return // listener closed
		}
		e.mu.Lock()
		cut := e.cut
		e.mu.Unlock()
		if cut {
			conn.Close() // reject immediately: simulates a link that's down
			continue
		}
		go e.relay(conn)
	}
}

func (e *DirectedEdge) relay(client net.Conn) {
	backend, err := net.Dial("tcp", e.TargetAddr)
	if err != nil {
		client.Close()
		return
	}

	e.mu.Lock()
	e.conns[client] = struct{}{}
	e.conns[backend] = struct{}{}
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.conns, client)
		delete(e.conns, backend)
		e.mu.Unlock()
		client.Close()
		backend.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); e.pump(backend, client) }()
	go func() { defer wg.Done(); e.pump(client, backend) }()
	wg.Wait()
}

// pump copies from src to dst, applying the current delay (if any) once
// per chunk read -- enough to make a real, measurable difference to RPC
// latency without needing per-byte precision.
func (e *DirectedEdge) pump(dst io.Writer, src io.Reader) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			e.mu.Lock()
			d := e.delay
			e.mu.Unlock()
			if d > 0 {
				time.Sleep(d)
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// Cut stops accepting new relayed traffic (existing connections are also
// torn down), simulating this directed link going down.
func (e *DirectedEdge) Cut() {
	e.mu.Lock()
	e.cut = true
	conns := make([]net.Conn, 0, len(e.conns))
	for c := range e.conns {
		conns = append(conns, c)
	}
	e.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
}

// Restore resumes relaying new connections on this link.
func (e *DirectedEdge) Restore() {
	e.mu.Lock()
	e.cut = false
	e.mu.Unlock()
}

// SetDelay adds d latency to every chunk relayed across this link (0 to
// disable). Simulates a slow, not down, link.
func (e *DirectedEdge) SetDelay(d time.Duration) {
	e.mu.Lock()
	e.delay = d
	e.mu.Unlock()
}

// Close stops the listener and all active relayed connections.
func (e *DirectedEdge) Close() error {
	err := e.lis.Close()
	e.mu.Lock()
	conns := make([]net.Conn, 0, len(e.conns))
	for c := range e.conns {
		conns = append(conns, c)
	}
	e.mu.Unlock()
	for _, c := range conns {
		c.Close()
	}
	return err
}
