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

func TestVersionedWALReplayRepairResetAndLegacyRefusal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "store.wal")
	format := Format{Version: 1, ClusterID: "synthetic-format-test"}
	w, err := OpenReplayFormat(path, format, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.AppendBatch([]Record{{Op: OpPut, Key: []byte("keep"), Value: []byte("old")}, {Op: OpDelete, Key: []byte("deleted")}}); err != nil {
		t.Fatal(err)
	}
	header := bytes.Clone(w.fileHeader)
	w.Close()
	raw, _ := os.ReadFile(path)
	if legacy, err := Open(path); legacy != nil || !errors.Is(err, ErrCorrupt) {
		if legacy != nil {
			legacy.Close()
		}
		t.Fatal("legacy reader accepted versioned file", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(raw, after) {
		t.Fatal("legacy refusal modified versioned file")
	}
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	f.Write([]byte{1, 2, 3})
	f.Close()
	var records []Record
	valid, err := InspectFormat(path, format, func(r Record) error { records = append(records, r); return nil })
	if err != nil || valid != int64(len(raw)) || len(records) != 2 {
		t.Fatalf("readonly prefix %d %+v %v", valid, records, err)
	}
	info, _ := os.Stat(path)
	if info.Size() != int64(len(raw)+3) {
		t.Fatal("inspection repaired suffix")
	}
	w, err = OpenReplayFormat(path, format, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Reset([]Record{{Op: OpPut, Key: []byte("replacement"), Value: []byte("durable")}}); err != nil {
		t.Fatal(err)
	}
	if err = w.Append(Record{Op: OpDelete, Key: []byte("replacement")}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	after, _ = os.ReadFile(path)
	if !bytes.Equal(header, after[:len(header)]) {
		t.Fatal("reset changed validated header/CRC seed")
	}
	records = nil
	valid, err = InspectFormat(path, format, func(r Record) error { records = append(records, r); return nil })
	if err != nil || valid != int64(len(after)) || len(records) != 2 || string(records[0].Key) != "replacement" || records[1].Op != OpDelete {
		t.Fatalf("reset/reopen %+v %v", records, err)
	}
}

func TestVersionedWALHeaderRefusalsPreserveEveryByte(t *testing.T) {
	format := Format{Version: 1, ClusterID: "header-test"}
	path := filepath.Join(t.TempDir(), "source.wal")
	w, err := OpenReplayFormat(path, format, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Op: OpPut, Key: []byte("acknowledged"), Value: []byte("value")})
	w.Close()
	original, _ := os.ReadFile(path)
	variants := map[string][]byte{}
	for _, index := range []int{0, 8, 12, 18, len(w.fileHeader) - 1} {
		v := bytes.Clone(original)
		v[index] ^= 0xff
		variants[string(rune('a'+index))] = v
	}
	for cut := 0; cut < len(w.fileHeader); cut++ {
		variants["partial-"+string(rune('a'+cut))] = bytes.Clone(original[:cut])
	}
	for name, data := range variants {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "refused.wal")
			os.WriteFile(p, data, 0600)
			visits := 0
			x, err := OpenReplayFormat(p, format, func(Record) error { visits++; return nil })
			if x != nil || !errors.Is(err, ErrFormat) || visits != 0 {
				if x != nil {
					x.Close()
				}
				t.Fatalf("header accepted %v visits=%d", err, visits)
			}
			after, _ := os.ReadFile(p)
			if !bytes.Equal(data, after) {
				t.Fatal("refusal changed original")
			}
		})
	}
	if _, err = OpenReplayFormat(path, Format{Version: 1, ClusterID: "other-cluster"}, nil); !errors.Is(err, ErrFormat) {
		t.Fatal("cross-cluster WAL accepted", err)
	}
	// A valid header checksum with a different record seed still cannot validate
	// records from the original file. This is corruption detection, not a MAC.
	forged := bytes.Clone(original)
	forged[14] ^= 1
	binary.LittleEndian.PutUint32(forged[len(w.fileHeader)-4:len(w.fileHeader)], crc32.ChecksumIEEE(forged[:len(w.fileHeader)-4]))
	os.WriteFile(path, forged, 0600)
	if _, err = OpenReplayFormat(path, format, nil); !errors.Is(err, ErrCorrupt) {
		t.Fatal("record seed not enforced", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(after, forged) {
		t.Fatal("record corruption repaired")
	}
}

func TestWALFormatsNeverConvertOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.wal")
	w, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Op: OpPut, Key: []byte("ack")})
	w.Close()
	before, _ := os.ReadFile(path)
	if _, err = OpenReplayFormat(path, Format{Version: 1, ClusterID: "cluster"}, nil); !errors.Is(err, ErrFormat) {
		t.Fatal("legacy file implicitly converted", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("legacy file changed")
	}
	for _, format := range []Format{{Version: 2, ClusterID: "cluster"}, {Version: 1}, {Version: 0, ClusterID: "cluster"}, {Version: 1, ClusterID: "bad/cluster"}} {
		p := filepath.Join(t.TempDir(), "must-not-exist.wal")
		if _, err = OpenReplayFormat(p, format, nil); !errors.Is(err, ErrFormat) {
			t.Fatal("invalid format accepted", format, err)
		}
		if _, err = os.Stat(p); !os.IsNotExist(err) {
			t.Fatal("invalid format touched storage")
		}
	}
}

func TestVersionedWALVisitorFailurePreservesTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source.wal")
	format := Format{Version: 1, ClusterID: "visitor-test"}
	w, err := OpenReplayFormat(path, format, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.Append(Record{Op: OpPut, Key: []byte("ack")})
	w.Close()
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	f.Write([]byte{1, 2, 3})
	f.Close()
	before, _ := os.ReadFile(path)
	denied := errors.New("visitor refused")
	if _, err = OpenReplayFormat(path, format, func(Record) error { return denied }); !errors.Is(err, denied) {
		t.Fatal("visitor failure ignored", err)
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("visitor failure repaired original")
	}
}
