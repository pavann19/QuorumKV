package raftnode

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"io"

	"github.com/pavann19/quorumkv/internal/store"
)

const migrationMaxEntries = 1_000_000
const migrationMaxLogicalBytes = 256 << 20
const migrationMaxSnapshotBytes = migrationMaxLogicalBytes + migrationMaxEntries*32 + (2 << 20)

// Guard only the outer gob message framing and declared slice count; the adopted
// decoder still validates type definitions and field values. Type messages are
// small and bounded. The large value message is not copied into the guard.
// This prevents a declared Entry slice from allocating outside the migration
// ceiling before post-decode logical/duplicate validation can run.
func guardedLegacySnapshot(reader io.Reader, size int64) (io.Reader, error) {
	if size < 0 || size > migrationMaxSnapshotBytes {
		return nil, errors.New("legacy snapshot file exceeds migration ceiling")
	}
	r := bufio.NewReader(reader)
	var prefix bytes.Buffer
	readUint := func(input io.Reader) (uint64, error) {
		var first [1]byte
		if _, err := io.ReadFull(input, first[:]); err != nil {
			return 0, err
		}
		prefix.Write(first[:])
		if first[0] <= 127 {
			return uint64(first[0]), nil
		}
		n := -int(int8(first[0]))
		if n < 1 || n > 8 {
			return 0, errors.New("invalid legacy integer width")
		}
		var b [8]byte
		if _, err := io.ReadFull(input, b[:n]); err != nil {
			return 0, err
		}
		prefix.Write(b[:n])
		var value uint64
		for _, c := range b[:n] {
			value = value<<8 | uint64(c)
		}
		return value, nil
	}
	for messages := 0; messages < 32; messages++ {
		length, err := readUint(r)
		if err != nil {
			return nil, err
		}
		if length == 0 || length > migrationMaxSnapshotBytes {
			return nil, errors.New("legacy message exceeds migration ceiling")
		}
		message := &io.LimitedReader{R: r, N: int64(length)}
		typeID, err := readUint(message)
		if err != nil {
			return nil, err
		}
		if typeID&1 == 1 { // Negative signed gob ID declares a type.
			if length > 64<<10 {
				return nil, errors.New("legacy type declaration exceeds ceiling")
			}
			if _, err = io.Copy(&prefix, message); err != nil {
				return nil, err
			}
			if message.N != 0 {
				return nil, io.ErrUnexpectedEOF
			}
			continue
		}
		delta, err := readUint(message)
		if err != nil || delta != 0 {
			return nil, errors.New("legacy snapshot is not a singleton slice")
		}
		count, err := readUint(message)
		if err != nil {
			return nil, err
		}
		if count > migrationMaxEntries {
			return nil, errors.New("legacy snapshot entry count exceeds migration ceiling")
		}
		if int64(prefix.Len())+message.N != size {
			return nil, errors.New("legacy snapshot has trailing or incomplete framed data")
		}
		return io.MultiReader(bytes.NewReader(prefix.Bytes()), io.LimitReader(r, message.N)), nil
	}
	return nil, errors.New("legacy snapshot type sequence exceeds ceiling")
}

func decodeMigrationLegacySnapshot(reader io.Reader, size int64) ([]store.Entry, error) {
	guarded, err := guardedLegacySnapshot(io.LimitReader(reader, migrationMaxSnapshotBytes+1), size)
	if err != nil {
		return nil, err
	}
	decoder := gob.NewDecoder(guarded)
	var entries []store.Entry
	if err = decoder.Decode(&entries); err != nil {
		return nil, err
	}
	// Do not decode a second attacker-chosen object merely to test EOF: that
	// could allocate outside the first object's guard. The decoder may buffer,
	// so bound the complete value message and check framing before decoding.
	if len(entries) > migrationMaxEntries {
		return nil, errors.New("legacy entry ceiling")
	}
	return entries, nil
}
