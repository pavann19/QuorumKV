package fault

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

// These tests exercise the checker itself, with no cluster involved. The
// five scenario results only mean something if the checker can also say
// "no": without a known-bad history that it rejects, "linearizable" would
// only show that it accepts the histories those scenarios happened to
// produce. The negative controls below are that evidence.

func put(client int, key, val string, call, ret int64) Op {
	return Op{ClientID: client, Kind: "put", Key: key, Value: val, CallNS: call, ReturnNS: ret}
}

func failedPut(client int, key, val string, call, ret int64) Op {
	o := put(client, key, val, call, ret)
	o.Err = "rpc error: deadline exceeded"
	return o
}

func get(client int, key string, found bool, val string, call, ret int64) Op {
	return Op{ClientID: client, Kind: "get", Key: key, Found: found, GotValue: val, CallNS: call, ReturnNS: ret}
}

func failedGet(client int, key string, call, ret int64) Op {
	return Op{ClientID: client, Kind: "get", Key: key, CallNS: call, ReturnNS: ret, Err: "rpc error: unavailable"}
}

func check(history []Op) bool {
	res := porcupine.CheckOperationsTimeout(kvModel, toOperations(history), 10*time.Second)
	return res == porcupine.Ok
}

func TestChecker_RejectsStaleRead(t *testing.T) {
	// Put(v1) and Put(v2) both finish before the Get even starts, so the
	// Get must see v2. Returning v1 is the classic stale read.
	h := []Op{
		put(0, "k", "v1", 1, 2),
		put(0, "k", "v2", 3, 4),
		get(1, "k", true, "v1", 5, 6),
	}
	if check(h) {
		t.Fatal("checker accepted a stale read; it cannot be trusted to detect violations")
	}
}

func TestChecker_RejectsReadOfNeverWrittenValue(t *testing.T) {
	h := []Op{
		put(0, "k", "v1", 1, 2),
		get(1, "k", true, "ghost", 3, 4),
	}
	if check(h) {
		t.Fatal("checker accepted a read of a value that was never written")
	}
}

func TestChecker_RejectsNotFoundAfterCompletedPut(t *testing.T) {
	h := []Op{
		put(0, "k", "v1", 1, 2),
		get(1, "k", false, "", 3, 4),
	}
	if check(h) {
		t.Fatal("checker accepted not-found after a completed Put")
	}
}

func TestChecker_RejectsTimeGoingBackwards(t *testing.T) {
	// Reader observes v2, then a later reader observes v1: no single order
	// of the two writes can explain both reads.
	h := []Op{
		put(0, "k", "v1", 1, 2),
		put(0, "k", "v2", 3, 4),
		get(1, "k", true, "v2", 5, 6),
		get(2, "k", true, "v1", 7, 8),
	}
	if check(h) {
		t.Fatal("checker accepted reads that observe writes in contradictory orders")
	}
}

func TestChecker_AcceptsLatestRead(t *testing.T) {
	h := []Op{
		put(0, "k", "v1", 1, 2),
		put(0, "k", "v2", 3, 4),
		get(1, "k", true, "v2", 5, 6),
	}
	if !check(h) {
		t.Fatal("checker rejected a plainly linearizable history")
	}
}

func TestChecker_AcceptsOldValueWhileWriteInFlight(t *testing.T) {
	// The Get overlaps Put(v2), so returning either v1 or v2 is legal.
	h := []Op{
		put(0, "k", "v1", 1, 2),
		put(0, "k", "v2", 3, 10),
		get(1, "k", true, "v1", 5, 6),
	}
	if !check(h) {
		t.Fatal("checker rejected a read that overlaps an in-flight write")
	}
}

func TestChecker_KeysAreIndependent(t *testing.T) {
	h := []Op{
		put(0, "a", "1", 1, 2),
		put(0, "b", "2", 3, 4),
		get(1, "a", true, "1", 5, 6),
		get(1, "b", true, "2", 7, 8),
	}
	if !check(h) {
		t.Fatal("checker rejected independent per-key reads")
	}
}

// --- Failed (indeterminate) operations ---

func TestChecker_ErroredPutMayHaveHappened(t *testing.T) {
	// The Put(v2) timed out, but a later Get sees v2, so it did commit.
	// Dropping the errored Put would leave a read of a never-written value
	// and wrongly report a violation.
	h := []Op{
		put(0, "k", "v1", 1, 2),
		failedPut(0, "k", "v2", 3, 4),
		get(1, "k", true, "v2", 5, 6),
	}
	if !check(h) {
		t.Fatal("checker rejected a history explained by an errored Put that did take effect")
	}
}

func TestChecker_ErroredPutMayNotHaveHappened(t *testing.T) {
	h := []Op{
		put(0, "k", "v1", 1, 2),
		failedPut(0, "k", "v2", 3, 4),
		get(1, "k", true, "v1", 5, 6),
	}
	if !check(h) {
		t.Fatal("checker rejected a history where the errored Put did not take effect")
	}
}

func TestChecker_ErroredPutCannotExcuseStaleRead(t *testing.T) {
	// An errored Put(v3) may or may not have applied, but neither case
	// makes reading v1 legal after Put(v2) completed.
	h := []Op{
		put(0, "k", "v1", 1, 2),
		put(0, "k", "v2", 3, 4),
		failedPut(0, "k", "v3", 5, 6),
		get(1, "k", true, "v1", 7, 8),
	}
	if check(h) {
		t.Fatal("an errored Put must not be able to excuse a stale read")
	}
}

func TestChecker_ErroredPutCannotBeObservedThenUnobserved(t *testing.T) {
	// If v2 was seen, it happened; a later read of v1 would mean it
	// un-happened.
	h := []Op{
		put(0, "k", "v1", 1, 2),
		failedPut(0, "k", "v2", 3, 4),
		get(1, "k", true, "v2", 5, 6),
		get(1, "k", true, "v1", 7, 8),
	}
	if check(h) {
		t.Fatal("checker let an errored Put be observed and then unobserved")
	}
}

func TestChecker_ErroredGetsAreDropped(t *testing.T) {
	ops := toOperations([]Op{
		put(0, "k", "v1", 1, 2),
		failedGet(1, "k", 3, 4),
	})
	if len(ops) != 1 {
		t.Fatalf("expected the errored Get to be dropped, got %d operations", len(ops))
	}
}
