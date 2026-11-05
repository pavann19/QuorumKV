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

func crashBatchRecords() []Record {
	records := []Record{}
	for i := 0; i < 8; i++ {
		records = append(records, Record{Op: OpPut, Key: []byte(fmt.Sprint(i)), Value: []byte("batch-ack")})
	}
	return append(records, Record{Op: OpDelete, Key: []byte("deleted")})
}
func TestBatchCrashHelper(t *testing.T) {
	if os.Getenv("QUORUMKV_BATCH_CRASH_HELPER") != "1" {
		return
	}
	var w *WAL
	var err error
	if os.Getenv("QUORUMKV_BATCH_CRASH_FORMAT") == "1" {
		w, err = OpenReplayFormat(os.Getenv("QUORUMKV_BATCH_CRASH_PATH"), Format{Version: 1, ClusterID: "versioned-crash-fixture"}, nil)
	} else {
		w, err = Open(os.Getenv("QUORUMKV_BATCH_CRASH_PATH"))
	}
	if err != nil {
		os.Exit(2)
	}
	if err = w.AppendBatch(crashBatchRecords()); err != nil {
		os.Exit(3)
	}
	fmt.Fprintln(os.Stdout, "BATCH_ACK")
	for {
		time.Sleep(time.Hour)
	}
}

func TestVersionedAcknowledgedBatchSurvivesAbruptProcessExit(t *testing.T) {
	const trials = 10
	for trial := 0; trial < trials; trial++ {
		path := filepath.Join(t.TempDir(), "fresh-versioned.wal")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBatchCrashHelper$")
		cmd.Env = append(os.Environ(), "QUORUMKV_BATCH_CRASH_HELPER=1", "QUORUMKV_BATCH_CRASH_FORMAT=1", "QUORUMKV_BATCH_CRASH_PATH="+path)
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if err = cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		ready := make(chan bool, 1)
		go func() { scanner := bufio.NewScanner(pipe); ready <- scanner.Scan() && scanner.Text() == "BATCH_ACK" }()
		ack := false
		select {
		case ack = <-ready:
		case <-ctx.Done():
		}
		killErr := cmd.Process.Kill()
		cmd.Wait()
		cancel()
		if !ack || killErr != nil {
			t.Fatal("missing versioned ack before kill", trial, killErr)
		}
		var records []Record
		valid, err := InspectFormat(path, Format{Version: 1, ClusterID: "versioned-crash-fixture"}, func(r Record) error { records = append(records, r); return nil })
		info, statErr := os.Stat(path)
		if err != nil || statErr != nil || len(records) != 9 || valid != info.Size() {
			t.Fatalf("lost versioned ack trial=%d count=%d error=%v stat=%v", trial, len(records), err, statErr)
		}
		for i := 0; i < 8; i++ {
			if string(records[i].Key) != fmt.Sprint(i) || string(records[i].Value) != "batch-ack" {
				t.Fatal("acknowledged record changed", trial, i)
			}
		}
		if records[8].Op != OpDelete || string(records[8].Key) != "deleted" {
			t.Fatal("acknowledged tombstone lost", trial)
		}
	}
	t.Logf("versioned_new_file_acknowledged_batch_abrupt_exit_trials=%d lost_acknowledged_records=0 scope=process_termination_not_power_loss", trials)
}
func TestAcknowledgedBatchSurvivesAbruptProcessExit(t *testing.T) {
	const trials = 10
	for trial := 0; trial < trials; trial++ {
		path := filepath.Join(t.TempDir(), "store.wal")
		w, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.Append(Record{Op: OpPut, Key: []byte("deleted"), Value: []byte("before")}); err != nil {
			t.Fatal(err)
		}
		w.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBatchCrashHelper$")
		cmd.Env = append(os.Environ(), "QUORUMKV_BATCH_CRASH_HELPER=1", "QUORUMKV_BATCH_CRASH_PATH="+path)
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		if err = cmd.Start(); err != nil {
			cancel()
			t.Fatal(err)
		}
		ready := make(chan bool, 1)
		go func() { scanner := bufio.NewScanner(pipe); ready <- scanner.Scan() && scanner.Text() == "BATCH_ACK" }()
		acknowledged := false
		select {
		case acknowledged = <-ready:
		case <-ctx.Done():
		}
		killErr := cmd.Process.Kill()
		cmd.Wait()
		cancel()
		if !acknowledged || killErr != nil {
			t.Fatal("batch was not acknowledged before fixture kill", trial, killErr)
		}
		replay, err := Replay(path)
		if err != nil || len(replay) != 10 {
			t.Fatal("acknowledged batch records lost", trial, len(replay), err)
		}
		state := map[string]string{}
		for _, record := range replay {
			if record.Op == OpPut {
				state[string(record.Key)] = string(record.Value)
			} else {
				delete(state, string(record.Key))
			}
		}
		if len(state) != 8 {
			t.Fatal("acknowledged delete/keyspace mismatch", trial, state)
		}
		for i := 0; i < 8; i++ {
			if state[fmt.Sprint(i)] != "batch-ack" {
				t.Fatal("acknowledged batch value lost", trial, i)
			}
		}
		w, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		if err = w.AppendBatch([]Record{{Op: OpPut, Key: []byte("after-restart"), Value: []byte("ack")}}); err != nil {
			t.Fatal(err)
		}
		w.Close()
		after, err := Replay(path)
		if err != nil || len(after) != 11 {
			t.Fatal("post-restart batch append failed", trial, err)
		}
	}
	t.Logf("acknowledged_batch_abrupt_exit_trials=%d lost_acknowledged_records=0 deletion_resurrections=0 scope=process_termination_not_power_loss", trials)
}
