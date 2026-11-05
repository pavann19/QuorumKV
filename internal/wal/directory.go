package wal

import (
	"errors"
	"os"
	"path/filepath"
)

// SyncDirectory persists directory entries on supported Unix filesystems.
// Windows deliberately has no portable directory fsync guarantee.
func SyncDirectory(path string) error { return syncDir(path) }

// MakeDurableDir syncs each newly created directory and its parent. It never
// removes existing paths. Callers still need an exclusive member data directory.
func MakeDurableDir(path string, mode os.FileMode) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return &os.PathError{Op: "mkdir", Path: path, Err: errors.New("not a directory")}
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	parent := filepath.Dir(filepath.Clean(path))
	if parent == path {
		return err
	}
	if err = MakeDurableDir(parent, mode); err != nil {
		return err
	}
	if err = os.Mkdir(path, mode); err != nil {
		return err
	}
	if err = syncDir(path); err != nil {
		return err
	}
	return syncDir(parent)
}
