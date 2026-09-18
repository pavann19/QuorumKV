// Package crashrecovery is the QuorumKV MVP's actual exit criterion: "100
// random kill-and-recover trials, zero cases of a committed write
// disappearing or an uncommitted write appearing." It doesn't simulate a
// crash in-process -- it runs the walwriter helper binary as a real OS
// subprocess and sends it a real, abrupt kill signal at a random point,
// then replays the WAL it left behind and checks the result against what
// the helper actually managed to report as durably written.
//
// Platform note: on Linux this would be SIGKILL; on Windows (this
// development machine), (*os.Process).Kill() maps to TerminateProcess,
// which is the closest abrupt-termination equivalent Windows offers -- no
// graceful shutdown, no deferred cleanup, no chance for the process to
// flush anything after the signal arrives. It is not a bit-for-bit
// reproduction of a Linux SIGKILL or of a real power-loss crash (which can
// also reorder or lose writes at the disk/OS cache layer in ways a
// same-machine kill cannot), but it is a real, external, abrupt
// termination of the process mid-operation, which is what this test needs.
package crashrecovery

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pavann19/quorumkv/internal/wal"
)

const numTrials = 100

var walwriterPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "walwriter-bin-*")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer os.RemoveAll(dir)

	walwriterPath = filepath.Join(dir, "walwriter.exe")
	build := exec.Command("go", "build", "-o", walwriterPath, "../../cmd/walwriter")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building walwriter: %v\n%s\n", err, out)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// trialResult is what one kill-and-recover trial observed.
type trialResult struct {
	lastConfirmedOK int // highest "OK n" line successfully read before the kill; -1 if none
	recovered       []wal.Record
}

func runOneTrial(t *testing.T, walPath string, killAfter time.Duration) trialResult {
	t.Helper()

	cmd := exec.Command(walwriterPath, walPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("getting stdout pipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting walwriter: %v", err)
	}

	var mu sync.Mutex
	lastOK := -1
	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			n, ok := parseOKLine(line)
			if !ok {
				continue
			}
			mu.Lock()
			lastOK = n
			mu.Unlock()
		}
	}()

	time.Sleep(killAfter)

	// The abrupt kill this test needs -- see the package doc comment for
	// why this is Windows' closest equivalent to SIGKILL.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("killing walwriter: %v", err)
	}
	_ = cmd.Wait() // expected to return a non-nil "killed" error; not the point of this test
	<-done         // drain the scanner goroutine so lastOK is its final value

	mu.Lock()
	finalOK := lastOK
	mu.Unlock()

	records, err := wal.Replay(walPath)
	if err != nil {
		t.Fatalf("replaying WAL after kill: %v", err)
	}

	return trialResult{lastConfirmedOK: finalOK, recovered: records}
}

func parseOKLine(line string) (n int, ok bool) {
	const prefix = "OK "
	if !strings.HasPrefix(line, prefix) {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(line, prefix))
	if err != nil {
		return 0, false
	}
	return n, true
}

// TestCrashRecovery_100Trials is the MVP exit criterion. Each trial: start
// walwriter, let it run for a random short duration, kill it abruptly,
// replay its WAL, and check two invariants:
//
//  1. No committed write disappears: every "OK n" the helper printed before
//     the kill must be present, in order, in the replayed WAL (records
//     0..lastConfirmedOK all recovered with matching key/value).
//  2. No uncommitted write appears out of nowhere: the WAL must never
//     contain a record beyond lastConfirmedOK+1. (+1, not +0, accounts for
//     the real, benign race where Append's fsync completed and the record
//     is genuinely durable, but the kill arrived before the "OK" line's
//     Flush was observed by the parent -- that record is legitimately
//     committed, just not yet reported. Anything beyond that would mean a
//     torn write was incorrectly replayed as valid, which Replay's CRC
//     check is specifically designed to prevent -- see internal/wal.)
func TestCrashRecovery_100Trials(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash-recovery trials in -short mode")
	}

	rng := rand.New(rand.NewSource(1))
	failures := 0

	for trial := 0; trial < numTrials; trial++ {
		walPath := filepath.Join(t.TempDir(), fmt.Sprintf("trial-%d.wal", trial))
		killAfter := time.Duration(rng.Intn(20)) * time.Millisecond

		result := runOneTrial(t, walPath, killAfter)

		if result.lastConfirmedOK < 0 {
			// Nothing was confirmed durable before the kill (a very fast
			// kill). The only valid recovered state is empty or exactly
			// one record (index 0, via the same fsync-before-flush race).
			if len(result.recovered) > 1 {
				t.Errorf("trial %d: no writes were confirmed OK, but recovered %d records (want 0 or 1)",
					trial, len(result.recovered))
				failures++
			}
			continue
		}

		wantMin := result.lastConfirmedOK + 1 // records 0..lastConfirmedOK, inclusive
		wantMax := wantMin + 1                // + the fsync-but-not-yet-reported race window

		if len(result.recovered) < wantMin {
			t.Errorf("trial %d: a committed write disappeared -- confirmed OK up to %d (want >= %d records), recovered only %d",
				trial, result.lastConfirmedOK, wantMin, len(result.recovered))
			failures++
			continue
		}
		if len(result.recovered) > wantMax {
			t.Errorf("trial %d: an uncommitted write appeared -- confirmed OK up to %d (want <= %d records), recovered %d",
				trial, result.lastConfirmedOK, wantMax, len(result.recovered))
			failures++
			continue
		}

		for i := 0; i <= result.lastConfirmedOK; i++ {
			wantKey := "key-" + strconv.Itoa(i)
			wantValue := "value-" + strconv.Itoa(i)
			if string(result.recovered[i].Key) != wantKey || string(result.recovered[i].Value) != wantValue {
				t.Errorf("trial %d: record %d content mismatch: got key=%q value=%q, want key=%q value=%q",
					trial, i, result.recovered[i].Key, result.recovered[i].Value, wantKey, wantValue)
				failures++
			}
		}
	}

	t.Logf("%d/%d trials completed with zero committed-write-disappeared or uncommitted-write-appeared violations",
		numTrials-failures, numTrials)
}
