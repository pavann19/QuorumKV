//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package memberlock

import (
	"errors"
	"os"
)

func lockFile(*os.File) error { return errors.New("member locking unsupported on this platform") }
