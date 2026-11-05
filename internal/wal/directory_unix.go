//go:build !windows

package wal

import (
	"errors"
	"os"
)

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func replaceWAL(from, to string) error { return os.Rename(from, to) }
