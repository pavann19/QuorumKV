// The M2 exit criterion: "at least 5 distinct fault scenarios (leader
// partition, minority partition, rolling restarts, network delay, double
// leader attempt) each produce a committed history file and a Porcupine
// PASS." Each test below is one of those five named scenarios.
package fault

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/pavann19/quorumkv/proto/quorumkvpb"
)

func keys(n int) []string {
	ks := make([]string, n)
	for i := range ks {
		ks[i] = "k" + string(rune('a'+i))
	}
	return ks
}

// TestScenario_LeaderPartition fully isolates the current leader from
// both other nodes. The majority side must elect a new leader and keep
// serving; the whole recorded history (routed through whichever nodes are
// actually reachable, via retryingCall) must still be linearizable.
func TestScenario_LeaderPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fault-injection scenario in -short mode")
	}
	c := Start(t, 3, 18101)
	leader := c.FindLeader(t, c.AllIDs(), 20*time.Second)
	t.Logf("initial leader: %s", leader)

	c.Partition(leader)
	t.Cleanup(func() { c.Heal(leader) })

	majority := make([]string, 0, 2)
	for _, id := range c.AllIDs() {
		if id != leader {
			majority = append(majority, id)
		}
	}
	newLeader := c.FindLeader(t, majority, 20*time.Second)
	t.Logf("new leader on majority side: %s", newLeader)

	history := RunWorkload(c, majority, 4, 10, keys(4), 15*time.Second)
	CheckAndCommit(t, "leader_partition", history)
}

// TestScenario_MinorityPartition isolates a single follower (never the
// leader) from the rest of the cluster. The majority (leader + remaining
// follower) must keep serving normally throughout.
func TestScenario_MinorityPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fault-injection scenario in -short mode")
	}
	c := Start(t, 3, 18111)
	leader := c.FindLeader(t, c.AllIDs(), 20*time.Second)

	var isolated string
	majority := make([]string, 0, 2)
	for _, id := range c.AllIDs() {
		if id == leader {
			majority = append(majority, id)
			continue
		}
		if isolated == "" {
			isolated = id
			continue
		}
		majority = append(majority, id)
	}
	t.Logf("leader=%s isolating minority follower=%s majority=%v", leader, isolated, majority)

	c.Partition(isolated)
	t.Cleanup(func() { c.Heal(isolated) })

	history := RunWorkload(c, majority, 4, 10, keys(4), 15*time.Second)
	CheckAndCommit(t, "minority_partition", history)
}

// TestScenario_RollingRestarts kills and restarts each node in turn (never
// more than one down at a time, so the majority is always intact) while a
// workload runs concurrently.
func TestScenario_RollingRestarts(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fault-injection scenario in -short mode")
	}
	c := Start(t, 3, 18121)
	c.FindLeader(t, c.AllIDs(), 20*time.Second)

	done := make(chan []Op, 1)
	go func() {
		done <- RunWorkload(c, c.AllIDs(), 4, 8, keys(4), 25*time.Second)
	}()

	// Build each node's own peer spec once (needed to restart it later
	// with the exact same per-node-perspective addressing it started with).
	peerSpecs := make(map[string]string, len(c.Nodes))
	dataDirs := make(map[string]string, len(c.Nodes))
	for _, n := range c.Nodes {
		spec := ""
		for other := range c.Edges[n.ID] {
			if spec != "" {
				spec += ","
			}
			spec += other + "=" + c.Edges[n.ID][other].ListenAddr
		}
		if spec != "" {
			spec += ","
		}
		spec += n.ID + "=" + n.RealAddr
		peerSpecs[n.ID] = spec
	}
	for _, n := range c.Nodes {
		dataDirs[n.ID] = filepath.Join(c.DataRoot, n.ID)
	}

	for _, n := range c.Nodes {
		time.Sleep(1 * time.Second)
		t.Logf("rolling restart: killing %s", n.ID)
		c.Kill(n.ID)
		time.Sleep(500 * time.Millisecond)
		t.Logf("rolling restart: restarting %s", n.ID)
		c.Restart(n.ID, dataDirs[n.ID], peerSpecs[n.ID])
	}

	history := <-done
	CheckAndCommit(t, "rolling_restarts", history)
}

// TestScenario_NetworkDelay adds substantial latency to every inter-node
// link (not a partition -- the cluster must stay correct, just slower).
func TestScenario_NetworkDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fault-injection scenario in -short mode")
	}
	c := Start(t, 3, 18131)
	c.FindLeader(t, c.AllIDs(), 20*time.Second)

	c.SetDelay(150 * time.Millisecond)
	t.Cleanup(func() { c.SetDelay(0) })

	history := RunWorkload(c, c.AllIDs(), 3, 6, keys(3), 30*time.Second)
	CheckAndCommit(t, "network_delay", history)
}

// TestScenario_DoubleLeaderAttempt is the split-brain check: partition the
// leader away from the majority, then attempt a write DIRECTLY against
// the now-isolated old leader (bypassing retry to other nodes) at the same
// time a write goes through the new majority-side leader. The old leader's
// write must fail (it cannot reach a quorum to commit), proving there is
// never a moment where two nodes can both successfully commit as leader.
func TestScenario_DoubleLeaderAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fault-injection scenario in -short mode")
	}
	c := Start(t, 3, 18141)
	oldLeader := c.FindLeader(t, c.AllIDs(), 20*time.Second)

	c.Partition(oldLeader)
	t.Cleanup(func() { c.Heal(oldLeader) })

	majority := make([]string, 0, 2)
	for _, id := range c.AllIDs() {
		if id != oldLeader {
			majority = append(majority, id)
		}
	}
	newLeader := c.FindLeader(t, majority, 20*time.Second)
	if newLeader == oldLeader {
		t.Fatalf("expected a different node to become leader once %s was partitioned", oldLeader)
	}

	// Direct, no-retry write against the isolated old leader: it may still
	// believe it's leader for a brief window (until its lease/quorum check
	// catches up), but the write must never actually commit.
	oldLeaderClient := c.Client(t, oldLeader)
	oldCtx, oldCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, oldErr := oldLeaderClient.Put(oldCtx, &pb.PutRequest{Key: []byte("split-brain-check"), Value: []byte("from-old-leader")})
	oldCancel()
	if oldErr == nil {
		t.Fatal("expected a write against the partitioned old leader to fail (no quorum available), but it succeeded")
	}
	t.Logf("write against isolated old leader %s correctly failed: %v", oldLeader, oldErr)

	newLeaderClient := c.Client(t, newLeader)
	newCtx, newCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, newErr := newLeaderClient.Put(newCtx, &pb.PutRequest{Key: []byte("split-brain-check"), Value: []byte("from-new-leader")})
	newCancel()
	if newErr != nil {
		t.Fatalf("expected a write against the new majority-side leader %s to succeed, got: %v", newLeader, newErr)
	}

	history := RunWorkload(c, majority, 3, 6, keys(3), 15*time.Second)
	CheckAndCommit(t, "double_leader_attempt", history)
}
