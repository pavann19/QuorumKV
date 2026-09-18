// Command walwriter is a test helper, not part of the product: it opens a
// WAL at the given path and writes sequentially numbered Put records as
// fast as it can, printing "OK <n>" (flushed) immediately after each
// record's Append call returns -- i.e. after that record has been fsynced.
// test/crashrecovery drives this binary as a real OS subprocess and kills
// it with SIGKILL at a random point, using the "OK" lines it managed to
// print as a lower bound on how many records were durably committed before
// the kill.
package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"

	"github.com/pavann19/quorumkv/internal/wal"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: walwriter <wal-path>")
		os.Exit(2)
	}
	path := os.Args[1]

	w, err := wal.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "opening WAL:", err)
		os.Exit(1)
	}

	out := bufio.NewWriter(os.Stdout)
	for i := 0; ; i++ {
		rec := wal.Record{
			Op:    wal.OpPut,
			Key:   []byte("key-" + strconv.Itoa(i)),
			Value: []byte("value-" + strconv.Itoa(i)),
		}
		if err := w.Append(rec); err != nil {
			fmt.Fprintln(os.Stderr, "append failed:", err)
			os.Exit(1)
		}
		fmt.Fprintf(out, "OK %d\n", i)
		if err := out.Flush(); err != nil {
			// The parent's read end may already be gone if it's about to
			// kill us; that's fine, just stop rather than spin on errors.
			return
		}
	}
}
