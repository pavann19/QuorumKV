package faultproxy

import (
	"net"
	"testing"
	"time"
)

func TestCutRejectsDialThatFinishesAfterRestore(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	backend, peer := net.Pipe()
	defer peer.Close()
	edge, err := newDirectedEdge("127.0.0.1:0", "unused", func(string, string) (net.Conn, error) {
		close(started)
		<-release
		return backend, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()
	client, err := net.Dial("tcp", edge.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dial did not start")
	}
	edge.Cut()
	edge.Restore()
	close(release)
	client.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatal("pre-cut dial survived cut/restore")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("connection leaked after cut/restore")
	}
	peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("backend remained open")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("backend leaked")
	}
}

func TestRelayClosesOtherPumpOnEOF(t *testing.T) {
	backend, peer := net.Pipe()
	defer peer.Close()
	edge, err := newDirectedEdge("127.0.0.1:0", "unused", func(string, string) (net.Conn, error) { return backend, nil })
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()
	client, err := net.Dial("tcp", edge.ListenAddr)
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected close")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("opposite pump remained blocked")
	}
}
