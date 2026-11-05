package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// fixtureNodeBinary supplies one owned candidate to both process test families.
// Linux offline runners identify and hash the explicit executable before use.
func fixtureNodeBinary(t *testing.T, root string) string {
	t.Helper()
	if binary := os.Getenv("QUORUMKV_NODE_BINARY"); binary != "" {
		info, err := os.Stat(binary)
		if err != nil || !info.Mode().IsRegular() {
			t.Fatalf("precompiled owned-fixture executable invalid: %v", err)
		}
		return binary
	}
	binary := filepath.Join(root, "node.exe")
	if output, err := exec.Command("go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build %v %s", err, output)
	}
	return binary
}
