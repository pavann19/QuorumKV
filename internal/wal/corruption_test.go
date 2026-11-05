package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// Complete damaged records cannot safely be classified as unacknowledged tails.
// In particular, recovery must not erase acknowledged records following damage.
func TestCompleteCorruptionFailsWithoutModifyingWAL(t *testing.T) {
	for _, mode := range []string{"middle_checksum", "last_checksum", "unknown_op", "bad_structure"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "corrupt.wal")
			w, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"one", "two", "end"} {
				if err := w.Append(Record{Op: OpPut, Key: []byte(key), Value: []byte("acknowledged")}); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			start := headerLen + int(binary.LittleEndian.Uint32(raw[:4]))
			if mode == "last_checksum" {
				start += headerLen + int(binary.LittleEndian.Uint32(raw[start:start+4]))
			}
			end := start + headerLen + int(binary.LittleEndian.Uint32(raw[start:start+4]))
			body := raw[start+headerLen : end]
			switch mode {
			case "unknown_op":
				body[0] = 255
			case "bad_structure":
				binary.LittleEndian.PutUint32(body[1:5], ^uint32(0))
			default:
				body[len(body)-1] ^= 255
			}
			if mode == "unknown_op" || mode == "bad_structure" {
				binary.LittleEndian.PutUint32(raw[start+4:start+8], crc32.ChecksumIEEE(body))
			}
			if err := os.WriteFile(path, raw, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := Replay(path); !errors.Is(err, ErrCorrupt) {
				t.Errorf("expected typed corruption error from replay, got %v", err)
			}
			if opened, err := Open(path); !errors.Is(err, ErrCorrupt) {
				if opened != nil {
					opened.Close()
				}
				t.Errorf("expected typed corruption error from open, got %v", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, after) {
				t.Fatal("failed recovery destroyed original corruption evidence or later acknowledgements")
			}
		})
	}
}
