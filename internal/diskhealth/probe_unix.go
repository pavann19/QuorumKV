//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package diskhealth

import (
	"errors"
	"golang.org/x/sys/unix"
	"math"
)

func Probe(path string) (Space, error) {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return Space{}, err
	}
	if stat.Bsize <= 0 {
		return Space{}, errors.New("invalid filesystem block size")
	}
	size := uint64(stat.Bsize)
	if uint64(stat.Bavail) > math.MaxUint64/size || uint64(stat.Blocks) > math.MaxUint64/size {
		return Space{}, errors.New("filesystem size overflow")
	}
	return Space{Available: uint64(stat.Bavail) * size, Total: uint64(stat.Blocks) * size}, nil
}
