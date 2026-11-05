package wal

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// Windows has no portable os.File directory fsync guarantee. File Sync still
// runs; target-filesystem power-loss durability remains unverified.
func syncDir(string) error { return nil }

func replaceWAL(from, to string) error {
	for attempt := 0; ; attempt++ {
		err := os.Rename(from, to)
		// Retry only transient sharing/lock violations, never arbitrary I/O errors.
		if err == nil || attempt == 5 || (!errors.Is(err, syscall.Errno(32)) && !errors.Is(err, syscall.Errno(33))) {
			return err
		}
		time.Sleep(time.Duration(1<<attempt) * 10 * time.Millisecond)
	}
}
