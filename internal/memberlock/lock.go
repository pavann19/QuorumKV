// Package memberlock holds an OS-managed lock for one member data directory.
// The lock file persists; ownership belongs to the open handle, never its name.
package memberlock

import (
	"fmt"
	"os"
	"path/filepath"
)

type Lock struct{ file *os.File }

func Acquire(dir string) (*Lock, error) {
	f, err := os.OpenFile(filepath.Join(dir, "member.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockFile(f); err != nil {
		f.Close()
		return nil, fmt.Errorf("member directory already owned or lock unavailable: %w", err)
	}
	return &Lock{file: f}, nil
}

// Close releases ownership. Never unlink the file: unlinking a locked inode
// would allow another owner to lock a different inode at the same path.
func (l *Lock) Close() error { return l.file.Close() }
