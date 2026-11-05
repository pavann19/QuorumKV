//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package diskhealth

func Probe(string) (Space, error) { return Space{}, ErrUnknown }
