package raftnode

import (
	"bytes"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/pavann19/quorumkv/internal/requestlimits"
	"github.com/pavann19/quorumkv/internal/store"
	storagepb "github.com/pavann19/quorumkv/proto/storagepb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

func TestCommandV1RoundtripAndLegacyIsolation(t *testing.T) {
	for _, c := range []command{{Op: opPut, Key: []byte{0, 255}, Value: []byte{0, 255}, Limits: store.DefaultLimits()}, {Op: opDelete, Key: []byte("deleted"), Limits: store.DefaultLimits()}, {Op: opPut, Key: bytes.Repeat([]byte("k"), requestlimits.MaxKeyBytes), Value: bytes.Repeat([]byte("v"), requestlimits.MaxValueBytes), Limits: store.DefaultLimits()}} {
		raw, err := encodeCommandFormat(c, "command-test", 1)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeCommandFormat(raw, "command-test", 1)
		if err != nil || !reflect.DeepEqual(got, c) {
			t.Fatal("roundtrip changed command", err)
		}
		if _, err = decodeCommandFormat(raw, "other-cluster", 1); !errors.Is(err, ErrCommandFormat) {
			t.Fatal("cross-cluster command accepted", err)
		}
		if _, err = decodeCommandFormat(raw, "command-test", 0); err == nil {
			t.Fatal("legacy decoder accepted protobuf command")
		}
		legacy, err := encodeCommand(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = decodeCommandFormat(legacy, "command-test", 1); !errors.Is(err, ErrCommandFormat) {
			t.Fatal("new profile guessed legacy encoding", err)
		}
		raw[len(raw)-1] ^= 255
		if !reflect.DeepEqual(got, c) {
			t.Fatal("decoder retained caller payload buffer")
		}
	}
}

func TestCommandV1MalformedSchemaAndBoundsRefuse(t *testing.T) {
	valid := command{Op: opPut, Key: []byte("key"), Value: []byte("value"), Limits: store.DefaultLimits()}
	raw, _ := encodeCommandFormat(valid, "cluster", 1)
	variants := [][]byte{nil, raw[:7], append(bytes.Clone(raw), 0x08, 1), append(bytes.Clone(raw), 0xa0, 6, 1), append(bytes.Clone(raw), 0x2a, 0), make([]byte, maxCommandBytes+1)}
	for _, change := range []func(*storagepb.CommandEnvelope){func(e *storagepb.CommandEnvelope) { e.SchemaVersion = 2 }, func(e *storagepb.CommandEnvelope) { e.Operation = 0 }, func(e *storagepb.CommandEnvelope) { e.Operation = 3 }, func(e *storagepb.CommandEnvelope) { e.MaxStoreBytes = math.MaxUint64 }, func(e *storagepb.CommandEnvelope) { e.MaxStoreEntries = 0 }, func(e *storagepb.CommandEnvelope) { e.Key = nil }, func(e *storagepb.CommandEnvelope) { e.Key = make([]byte, requestlimits.MaxKeyBytes+1) }, func(e *storagepb.CommandEnvelope) { e.Value = make([]byte, requestlimits.MaxValueBytes+1) }, func(e *storagepb.CommandEnvelope) { e.Operation = 2 }} {
		var envelope storagepb.CommandEnvelope
		proto.Unmarshal(raw[8:], &envelope)
		change(&envelope)
		body, _ := proto.Marshal(&envelope)
		variants = append(variants, append(bytes.Clone(commandMagic[:]), body...))
	}
	for i, v := range variants {
		if _, err := decodeCommandFormat(v, "cluster", 1); !errors.Is(err, ErrCommandFormat) {
			t.Fatal("malformed command accepted", i, err)
		}
	}
	for _, c := range []command{{Op: 99, Key: []byte("k"), Limits: store.DefaultLimits()}, {Op: opDelete, Key: []byte("k"), Value: []byte("not-empty"), Limits: store.DefaultLimits()}, {Op: opPut, Key: []byte("k")}} {
		if _, err := encodeCommandFormat(c, "cluster", 1); !errors.Is(err, ErrCommandFormat) {
			t.Fatal("invalid encoder input accepted", err)
		}
	}
}

func TestCommandFormatProfileRefusesBeforeStorage(t *testing.T) {
	peer := Peer{ID: "one", Addr: "127.0.0.1:29972"}
	for _, options := range []StartOptions{{CommandFormatVersion: 2}, {CommandFormatVersion: 1}, {CommandFormatVersion: 1, WALFormatVersion: 1}, {CommandFormatVersion: 1, SnapshotFormatVersion: 1}} {
		options.ClusterID = "format-test"
		options.Bootstrap = true
		options.BootstrapExpect = 1
		path := filepath.Join(t.TempDir(), "must-not-exist")
		if n, err := New(peer, []Peer{peer}, path, options); err == nil {
			n.Shutdown()
			t.Fatal("incompatible profile accepted")
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatal("incompatible profile touched storage")
		}
	}
}

func TestPeerCommandFormatFencesAndMetadataOwnership(t *testing.T) {
	original := metadata.Pairs(PeerCommandFormatHeader, "spoof", PeerCommandFormatHeader, "duplicate", PeerProtocolHeader, "wrong")
	ctx := metadata.NewOutgoingContext(context.Background(), original)
	md, _ := metadata.FromOutgoingContext(peerOutgoing(ctx, "cluster", 1, 1))
	if md.Get(PeerProtocolHeader)[0] != PeerCommandProtocolVersion || len(md.Get(PeerCommandFormatHeader)) != 1 || md.Get(PeerCommandFormatHeader)[0] != "1" {
		t.Fatal("outbound command binding incorrect", md)
	}
	if len(original.Get(PeerCommandFormatHeader)) != 2 {
		t.Fatal("caller metadata changed")
	}
	incoming := metadata.NewIncomingContext(context.Background(), md)
	if err := checkPeerBinding(incoming, "cluster", 1, 1); err != nil {
		t.Fatal("matching command binding rejected", err)
	}
	if err := checkPeerBinding(incoming, "cluster", 1); err == nil {
		t.Fatal("new protocol reached legacy profile")
	}
	legacy, _ := metadata.FromOutgoingContext(peerOutgoing(ctx, "cluster", 1))
	if len(legacy.Get(PeerCommandFormatHeader)) != 0 {
		t.Fatal("legacy outgoing retained spoofed command binding")
	}
	if err := checkPeerBinding(metadata.NewIncomingContext(context.Background(), legacy), "cluster", 1, 1); err == nil {
		t.Fatal("legacy protocol reached new profile")
	}
	for _, bad := range []metadata.MD{metadata.Pairs(PeerClusterHeader, "cluster", PeerProtocolHeader, "3", PeerSnapshotFormatHeader, "1"), metadata.Pairs(PeerClusterHeader, "cluster", PeerProtocolHeader, "3", PeerSnapshotFormatHeader, "1", PeerCommandFormatHeader, "1", PeerCommandFormatHeader, "1")} {
		if err := checkPeerBinding(metadata.NewIncomingContext(context.Background(), bad), "cluster", 1, 1); err == nil {
			t.Fatal("missing/duplicate binding accepted")
		}
	}
}
