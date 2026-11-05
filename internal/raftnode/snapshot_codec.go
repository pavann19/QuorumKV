package raftnode

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/pavann19/quorumkv/internal/store"
	"github.com/pavann19/quorumkv/internal/wal"
	storagepb "github.com/pavann19/quorumkv/proto/storagepb"
	"google.golang.org/protobuf/proto"
)

var ErrSnapshotFormat = errors.New("raftnode: unsupported or invalid snapshot format")
var snapshotMagic = []byte{'Q', 'K', 'S', 'N', 'A', 'P', 0, 1}

const maxSnapshotHeader = 64 << 10
const maxSnapshotEntry = wal.MaxRecordSize + 32

func writeSnapshotFrame(writer io.Writer, message proto.Message, max int) error {
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	if len(data) > max {
		return ErrSnapshotFormat
	}
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(data)))
	if n, err := writer.Write(length[:]); err != nil {
		return err
	} else if n != len(length) {
		return io.ErrShortWrite
	}
	if n, err := writer.Write(data); err != nil {
		return err
	} else if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}
func readSnapshotFrame(reader io.Reader, message proto.Message, max int) error {
	var length [4]byte
	if _, err := io.ReadFull(reader, length[:]); err != nil {
		return err
	}
	size := binary.LittleEndian.Uint32(length[:])
	if uint64(size) > uint64(max) {
		return fmt.Errorf("%w: frame exceeds bound", ErrSnapshotFormat)
	}
	data := make([]byte, int(size))
	if _, err := io.ReadFull(reader, data); err != nil {
		return err
	}
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(data, message); err != nil {
		return fmt.Errorf("%w: protobuf frame", ErrSnapshotFormat)
	}
	if len(message.ProtoReflect().GetUnknown()) != 0 {
		return fmt.Errorf("%w: unknown fields require explicit schema upgrade", ErrSnapshotFormat)
	}
	return nil
}
func encodeSnapshotV1(writer io.Writer, clusterID string, limits store.Limits, index uint64, entries []store.Entry) error {
	if err := limits.Validate(); err != nil {
		return err
	}
	var total uint64
	seen := map[string]bool{}
	for _, entry := range entries {
		key := string(entry.Key)
		if seen[key] {
			return fmt.Errorf("%w: duplicate key", ErrSnapshotFormat)
		}
		seen[key] = true
		length := uint64(len(entry.Key)) + uint64(len(entry.Value))
		if length > uint64(wal.MaxRecordSize-9) {
			return fmt.Errorf("%w: entry exceeds WAL record bound", ErrSnapshotFormat)
		}
		if length > uint64(limits.MaxBytes)-total {
			return store.ErrCapacity
		}
		total += length
		if total > uint64(limits.MaxBytes) || uint64(len(entries)) > uint64(limits.MaxEntries) {
			return store.ErrCapacity
		}
	}
	if n, err := writer.Write(snapshotMagic); err != nil {
		return err
	} else if n != len(snapshotMagic) {
		return io.ErrShortWrite
	}
	header := &storagepb.SnapshotHeader{SchemaVersion: 1, ClusterId: clusterID, MaxStoreBytes: uint64(limits.MaxBytes), MaxStoreEntries: uint64(limits.MaxEntries), EntryCount: uint64(len(entries)), LogicalBytes: total, LastCommandIndex: index}
	if err := writeSnapshotFrame(writer, header, maxSnapshotHeader); err != nil {
		return err
	}
	for _, entry := range entries {
		if err := writeSnapshotFrame(writer, &storagepb.SnapshotEntry{Key: entry.Key, Value: entry.Value}, maxSnapshotEntry); err != nil {
			return err
		}
	}
	return nil
}
func decodeSnapshotV1(reader io.Reader, clusterID string, limits store.Limits) ([]store.Entry, uint64, error) {
	if err := limits.Validate(); err != nil {
		return nil, 0, err
	}
	buffered := bufio.NewReader(reader)
	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(buffered, magic); err != nil {
		return nil, 0, err
	}
	if !bytes.Equal(magic, snapshotMagic) {
		return nil, 0, fmt.Errorf("%w: magic/version", ErrSnapshotFormat)
	}
	var header storagepb.SnapshotHeader
	if err := readSnapshotFrame(buffered, &header, maxSnapshotHeader); err != nil {
		return nil, 0, err
	}
	if header.SchemaVersion != 1 || header.ClusterId != clusterID || header.MaxStoreBytes != uint64(limits.MaxBytes) || header.MaxStoreEntries != uint64(limits.MaxEntries) {
		return nil, 0, fmt.Errorf("%w: schema/cluster/capacity mismatch", ErrSnapshotFormat)
	}
	if header.EntryCount > uint64(limits.MaxEntries) || header.LogicalBytes > uint64(limits.MaxBytes) {
		return nil, 0, store.ErrCapacity
	}
	entries := []store.Entry{}
	seen := map[string]bool{}
	var total uint64
	for i := uint64(0); i < header.EntryCount; i++ {
		var entry storagepb.SnapshotEntry
		frameLimit := maxSnapshotEntry
		remaining := header.LogicalBytes - total
		if remaining < uint64(maxSnapshotEntry-32) {
			frameLimit = int(remaining) + 32
		}
		if err := readSnapshotFrame(buffered, &entry, frameLimit); err != nil {
			return nil, 0, err
		}
		length := uint64(len(entry.Key)) + uint64(len(entry.Value))
		if length > uint64(wal.MaxRecordSize-9) {
			return nil, 0, fmt.Errorf("%w: entry exceeds WAL record bound", ErrSnapshotFormat)
		}
		if total > header.LogicalBytes || length > header.LogicalBytes-total {
			return nil, 0, fmt.Errorf("%w: logical size", ErrSnapshotFormat)
		}
		key := string(entry.Key)
		if seen[key] {
			return nil, 0, fmt.Errorf("%w: duplicate key", ErrSnapshotFormat)
		}
		seen[key] = true
		total += length
		entries = append(entries, store.Entry{Key: entry.Key, Value: entry.Value})
	}
	if total != header.LogicalBytes {
		return nil, 0, fmt.Errorf("%w: keyspace size", ErrSnapshotFormat)
	}
	if _, err := buffered.ReadByte(); err != io.EOF {
		if err != nil {
			return nil, 0, err
		}
		return nil, 0, fmt.Errorf("%w: trailing data", ErrSnapshotFormat)
	}
	return entries, header.LastCommandIndex, nil
}
