package memberlock

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestExclusionAndRelease(t *testing.T) {
	dir := t.TempDir()
	first, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Acquire(dir); err == nil {
		other.Close()
		t.Fatal("second owner accepted")
	}
	if err = first.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	next.Close()
	if _, err = os.Stat(filepath.Join(dir, "member.lock")); err != nil {
		t.Fatal("lock inode removed", err)
	}
}

func TestProcessLockHelper(t *testing.T) {
	if os.Getenv("QUORUMKV_LOCK_HELPER") != "1" {
		return
	}
	dir := os.Getenv("QUORUMKV_LOCK_DIR")
	lock, err := Acquire(dir)
	if err != nil {
		os.Exit(2)
	}
	defer lock.Close()
	if err = os.WriteFile(filepath.Join(dir, "ready"), []byte("owned"), 0600); err != nil {
		os.Exit(3)
	}
	for {
		time.Sleep(time.Second)
	}
}

func TestAbruptProcessExitReleasesOwnership(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessLockHelper$")
	cmd.Env = append(os.Environ(), "QUORUMKV_LOCK_HELPER=1", "QUORUMKV_LOCK_DIR="+dir)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("child not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if other, err := Acquire(dir); err == nil {
		other.Close()
		t.Fatal("live child ownership bypassed")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	next, err := Acquire(dir)
	if err != nil {
		t.Fatal("ownership survived process death", err)
	}
	next.Close()
}
