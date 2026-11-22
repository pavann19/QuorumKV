package faultproxy

import (
	"io"
	"strings"
	"testing"
	"time"
)

type observedReader struct {
	io.Reader
	read chan struct{}
}

func (r observedReader) Read(data []byte) (int, error) {
	n, err := r.Reader.Read(data)
	if n > 0 {
		close(r.read)
	}
	return n, err
}

func TestDelayedPumpStopsOnClose(t *testing.T) {
	edge, err := NewDirectedEdge("127.0.0.1:0", "127.0.0.1:1")
	if err != nil {
		t.Fatal(err)
	}
	defer edge.Close()
	edge.SetDelay(2 * time.Second)
	read, done := make(chan struct{}), make(chan struct{})
	go func() { edge.pump(io.Discard, observedReader{strings.NewReader("buffered"), read}); close(done) }()
	select {
	case <-read:
	case <-time.After(time.Second):
		t.Fatal("pump did not read")
	}
	edge.Close()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("closed proxy retained delayed pump")
	}
}
