package faultproxy

import (
	"bufio"
	"net"
	"testing"
	"time"
)

func startEchoServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { lis.Close() })
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				buf := make([]byte, 1024)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						return
					}
					if _, err := conn.Write(buf[:n]); err != nil {
						return
					}
				}
			}()
		}
	}()
	return lis.Addr().String()
}

func TestDirectedEdge_RelaysTraffic(t *testing.T) {
	backend := startEchoServer(t)
	edge, err := NewDirectedEdge("127.0.0.1:0", backend)
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()

	conn, err := net.Dial("tcp", edge.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if line != "hello\n" {
		t.Fatalf("expected echoed 'hello', got %q", line)
	}
}

func TestDirectedEdge_CutRejectsNewConnections(t *testing.T) {
	backend := startEchoServer(t)
	edge, err := NewDirectedEdge("127.0.0.1:0", backend)
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()

	edge.Cut()

	conn, err := net.Dial("tcp", edge.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected the cut edge to close the connection, but a read succeeded")
	}
}

func TestDirectedEdge_CutTearsDownExistingConnections(t *testing.T) {
	backend := startEchoServer(t)
	edge, err := NewDirectedEdge("127.0.0.1:0", backend)
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()

	conn, err := net.Dial("tcp", edge.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Make sure the relay goroutine has actually established the backend
	// connection before we cut.
	if _, err := conn.Write([]byte("x\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		t.Fatal(err)
	}

	edge.Cut()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("expected an existing connection to be torn down on Cut")
	}
}

func TestDirectedEdge_RestoreAllowsNewConnections(t *testing.T) {
	backend := startEchoServer(t)
	edge, err := NewDirectedEdge("127.0.0.1:0", backend)
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()

	edge.Cut()
	edge.Restore()

	conn, err := net.Dial("tcp", edge.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	if _, err := conn.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		t.Fatalf("expected traffic to flow again after Restore, got: %v", err)
	}
	if line != "hello\n" {
		t.Fatalf("expected echoed 'hello', got %q", line)
	}
}

func TestDirectedEdge_DelayAddsLatency(t *testing.T) {
	backend := startEchoServer(t)
	edge, err := NewDirectedEdge("127.0.0.1:0", backend)
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()
	edge.SetDelay(300 * time.Millisecond)

	conn, err := net.Dial("tcp", edge.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	start := time.Now()
	if _, err := conn.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if elapsed < 300*time.Millisecond {
		t.Fatalf("expected at least 300ms of added delay, got %v", elapsed)
	}
}
