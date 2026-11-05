package diskhealth

import "golang.org/x/sys/windows"

func Probe(path string) (Space, error) {
	pointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return Space{}, err
	}
	var available, total, free uint64
	if err = windows.GetDiskFreeSpaceEx(pointer, &available, &total, &free); err != nil {
		return Space{}, err
	}
	return Space{Available: available, Total: total}, nil
}
