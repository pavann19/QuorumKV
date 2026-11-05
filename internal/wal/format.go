package wal

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

// Format must be selected by the persisted member manifest. It is never inferred
// from a damaged file or silently upgraded while opening it.
type Format struct {
	Version   uint32
	ClusterID string
}

var ErrFormat = errors.New("wal: unsupported or mismatched file format")
var fileMagic = [8]byte{'Q', 'K', 'W', 'A', 'L', 0, 0, 1}

const fixedFileHeader = 18 // magic + version + cluster length + record CRC seed
const maxClusterID = 128

func (f Format) Validate() error {
	if f.Version == 0 {
		if f.ClusterID != "" {
			return fmt.Errorf("%w: legacy format has no cluster binding", ErrFormat)
		}
		return nil
	}
	if f.Version != 1 || len(f.ClusterID) < 1 || len(f.ClusterID) > maxClusterID {
		return ErrFormat
	}
	for _, c := range f.ClusterID {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return ErrFormat
		}
	}
	return nil
}

func makeFileHeader(format Format) ([]byte, uint32, error) {
	if err := format.Validate(); err != nil {
		return nil, 0, err
	}
	if format.Version == 0 {
		return nil, 0, nil
	}
	header := make([]byte, fixedFileHeader+len(format.ClusterID)+4)
	copy(header, fileMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], format.Version)
	binary.LittleEndian.PutUint16(header[12:14], uint16(len(format.ClusterID)))
	if _, err := rand.Read(header[14:18]); err != nil {
		return nil, 0, err
	}
	copy(header[18:], format.ClusterID)
	binary.LittleEndian.PutUint32(header[len(header)-4:], crc32.ChecksumIEEE(header[:len(header)-4]))
	return header, binary.LittleEndian.Uint32(header[14:18]), nil
}

// Header lengths are bounded before allocation. A partial header is not a
// repairable unacknowledged record tail: an operator must resolve it explicitly.
func readFileHeader(reader io.Reader, format Format) ([]byte, uint32, error) {
	if err := format.Validate(); err != nil {
		return nil, 0, err
	}
	if format.Version == 0 {
		return nil, 0, nil
	}
	fixed := make([]byte, fixedFileHeader)
	if _, err := io.ReadFull(reader, fixed); err != nil {
		return nil, 0, fmt.Errorf("%w: incomplete header", ErrFormat)
	}
	length := int(binary.LittleEndian.Uint16(fixed[12:14]))
	if !bytes.Equal(fixed[:8], fileMagic[:]) || binary.LittleEndian.Uint32(fixed[8:12]) != format.Version || length < 1 || length > maxClusterID {
		return nil, 0, ErrFormat
	}
	header := make([]byte, fixedFileHeader+length+4)
	copy(header, fixed)
	if _, err := io.ReadFull(reader, header[fixedFileHeader:]); err != nil {
		return nil, 0, fmt.Errorf("%w: incomplete header", ErrFormat)
	}
	if binary.LittleEndian.Uint32(header[len(header)-4:]) != crc32.ChecksumIEEE(header[:len(header)-4]) || string(header[fixedFileHeader:len(header)-4]) != format.ClusterID {
		return nil, 0, ErrFormat
	}
	return header, binary.LittleEndian.Uint32(header[14:18]), nil
}

// OpenReplayFormat creates only a new WAL in the selected format, or validates
// an existing header before visiting any record or attempting tail repair.
func OpenReplayFormat(path string, format Format, visit func(Record) error) (*WAL, error) {
	return openReplayFormat(path, syncDir, format, visit)
}

func recordChecksum(seed uint32, body []byte) uint32 {
	return crc32.Update(seed, crc32.IEEETable, body)
}
