package wal

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestResetCrashHelper(t *testing.T) {
	if os.Getenv("QUORUMKV_RESET_CRASH_HELPER") != "1" {
		return
	}
	w, err := Open(os.Getenv("QUORUMKV_RESET_CRASH_PATH"))
	if err != nil {
		os.Exit(2)
	}
	if err = w.Reset([]Record{{Op: OpPut, Key: []byte("retained"), Value: []byte("snapshot-ack")}}); err != nil {
		os.Exit(3)
	}
	// The parent kills this process only after this acknowledgement is observed.
	fmt.Fprintln(os.Stdout, "RESET_ACK")
	for {
		time.Sleep(time.Hour)
	}
}

func TestAcknowledgedResetSurvivesAbruptProcessExit(t *testing.T) {
	const trials = 10
	for trial := 0; trial < trials; trial++ {
		path := filepath.Join(t.TempDir(), "store.wal")
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Append(Record{Op: OpPut, Key: []byte("stale"), Value: []byte("must-not-resurrect")}); err != nil {
			w.Close()
			t.Fatal(err)
		}
		w.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestResetCrashHelper$")
		cmd.Env = append(os.Environ(), "QUORUMKV_RESET_CRASH_HELPER=1", "QUORUMKV_RESET_CRASH_PATH="+path)
		output, err := cmd.StdoutPipe()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if err = cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		ready := make(chan bool, 1)
		go func() { scanner := bufio.NewScanner(output); ready <- scanner.Scan() && scanner.Text() == "RESET_ACK" }()
		acknowledged := false
		select {
		case acknowledged = <-ready:
		case <-ctx.Done():
		}
		killErr := cmd.Process.Kill()
		cmd.Wait()
		cancel()
		if !acknowledged || killErr != nil {
			t.Fatalf("trial %d: reset was not acknowledged before kill: %v", trial, killErr)
		}
		records, err := Replay(path)
		if err != nil || len(records) != 1 || string(records[0].Key) != "retained" || string(records[0].Value) != "snapshot-ack" {
			t.Fatalf("trial %d: reset lost/stale state resurrected: %v %v", trial, records, err)
		}
		w, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Append(Record{Op: OpPut, Key: []byte("after-restart"), Value: []byte("ack")}); err != nil {
			w.Close()
			t.Fatal(err)
		}
		w.Close()
		records, err = Replay(path)
		if err != nil || len(records) != 2 {
			t.Fatalf("trial %d: post-reset append failed: %v", trial, err)
		}
	}
	t.Logf("acknowledged_reset_abrupt_exit_trials=%d stale_resurrections=0 acknowledged_reset_losses=0 scope=process_termination_not_power_loss", trials)
}
