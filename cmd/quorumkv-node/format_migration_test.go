package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestFormatMigrationCLIRefusesIncompleteOrUnknownArguments(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	destination := filepath.Join(root, "target")
	for _, args := range [][]string{nil, {"-source=" + source, "-destination=" + destination, "-to-profile=unknown"}, {"-source=" + source, "-destination=" + destination, "-to-profile=legacy", "-timeout=0"}, {"-source=" + source, "-destination=" + destination, "-to-profile=versioned", "-timeout=31m"}, {"-source=" + source, "-destination=" + destination, "-to-profile=versioned", "extra"}} {
		var out bytes.Buffer
		if err := runFormatMigration(args, &out); err == nil {
			t.Fatal("invalid migration CLI accepted", args)
		}
		if _, err := os.Stat(destination); !os.IsNotExist(err) {
			t.Fatal("invalid CLI created destination")
		}
	}
}
