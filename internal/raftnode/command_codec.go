package raftnode

import (
	"bytes"
	"errors"
	"math"

	"github.com/pavann19/quorumkv/internal/requestlimits"
	"github.com/pavann19/quorumkv/internal/store"
	storagepb "github.com/pavann19/quorumkv/proto/storagepb"
	"google.golang.org/protobuf/proto"
)

var ErrCommandFormat = errors.New("raftnode: unsupported or invalid command format")
var commandMagic = [8]byte{'Q', 'K', 'C', 'M', 'D', 0, 0, 1}

const maxCommandBytes = requestlimits.MaxKeyBytes + requestlimits.MaxValueBytes + 4096

func validCommandV1(c command, clusterID string) bool {
	if len(clusterID) < 1 || len(clusterID) > 128 || len(c.Key) < 1 || len(c.Key) > requestlimits.MaxKeyBytes || len(c.Value) > requestlimits.MaxValueBytes || c.Limits.Validate() != nil {
		return false
	}
	for _, r := range clusterID {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return false
		}
	}
	return c.Op == opPut || c.Op == opDelete && len(c.Value) == 0
}
func encodeCommandFormat(c command, clusterID string, format uint32) ([]byte, error) {
	if format == 0 {
		return encodeCommand(c)
	}
	if format != 1 || !validCommandV1(c, clusterID) {
		return nil, ErrCommandFormat
	}
	envelope := &storagepb.CommandEnvelope{SchemaVersion: 1, ClusterId: clusterID, Operation: uint32(c.Op) + 1, Key: c.Key, Value: c.Value, MaxStoreBytes: uint64(c.Limits.MaxBytes), MaxStoreEntries: uint64(c.Limits.MaxEntries)}
	body, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(body)+len(commandMagic) > maxCommandBytes {
		return nil, ErrCommandFormat
	}
	return append(append([]byte(nil), commandMagic[:]...), body...), nil
}
func decodeCommandFormat(data []byte, clusterID string, format uint32) (command, error) {
	if format == 0 {
		return decodeCommand(data)
	}
	if format != 1 || len(data) < len(commandMagic) || len(data) > maxCommandBytes || !bytes.Equal(data[:len(commandMagic)], commandMagic[:]) {
		return command{}, ErrCommandFormat
	}
	var envelope storagepb.CommandEnvelope
	body := data[len(commandMagic):]
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(body, &envelope); err != nil {
		return command{}, ErrCommandFormat
	}
	if len(envelope.ProtoReflect().GetUnknown()) != 0 || envelope.SchemaVersion != 1 || envelope.ClusterId != clusterID || envelope.MaxStoreBytes > math.MaxInt64 || envelope.MaxStoreEntries > math.MaxInt64 || envelope.Operation < 1 || envelope.Operation > 2 {
		return command{}, ErrCommandFormat
	}
	c := command{Op: commandOp(envelope.Operation - 1), Key: envelope.Key, Value: envelope.Value, Limits: store.Limits{MaxBytes: int64(envelope.MaxStoreBytes), MaxEntries: int64(envelope.MaxStoreEntries)}}
	if !validCommandV1(c, clusterID) {
		return command{}, ErrCommandFormat
	}
	// Reject duplicate fields, explicit default encodings, reordered fields and
	// alternate varints. One profile has one canonical interpretation/encoding.
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(&envelope)
	if err != nil || !bytes.Equal(body, canonical) {
		return command{}, ErrCommandFormat
	}
	return c, nil
}
