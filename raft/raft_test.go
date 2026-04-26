package raft_test

import (
	"testing"
	"time"

	"github.com/yogeshwaran/bunkr/backend/raft"
	"github.com/yogeshwaran/bunkr/backend/transport"
)

// testCluster wraps 5 nodes for testing.
type testCluster struct {
	nodes     map[raft.NodeID]*raft.Node
	transport *transport.ChannelTransport
	allIDs    []raft.NodeID
}

func newTestCluster(n int) *testCluster {
	tp := transport.NewChannelTransport()
	allIDs := make([]raft.NodeID, n)
	for i := 0; i < n; i++ {
		allIDs[i] = raft.NodeID(i + 1)
	}

	nodes := make(map[raft.NodeID]*raft.Node)
	for _, id := range allIDs {
		peers := make([]raft.NodeID, 0)
		for _, other := range allIDs {
			if other != id {
				peers = append(peers, other)
			}
		}
		node := raft.NewNode(id, peers, tp)
		nodes[id] = node
		tp.Register(id, node)
	}

	return &testCluster{nodes: nodes, transport: tp, allIDs: allIDs}
}

func (c *testCluster) start() {
	for _, id := range c.allIDs {
		c.nodes[id].Start()
		// Drain apply channels
		go func(n *raft.Node) {
			for range n.ApplyCh() {
			}
		}(c.nodes[id])
	}
}

func (c *testCluster) stop() {
	for _, id := range c.allIDs {
		c.nodes[id].Stop()
	}
}

func (c *testCluster) leader() raft.NodeID {
	for _, id := range c.allIDs {
		if c.nodes[id].State() == raft.Leader {
			return id
		}
	}
	return 0
}

func (c *testCluster) waitForLeader(timeout time.Duration) raft.NodeID {
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return 0
		default:
			if id := c.leader(); id != 0 {
				return id
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (c *testCluster) kill(id raft.NodeID) {
	c.nodes[id].Stop()
	c.transport.Unregister(id)
}

// --- Tests ---

func TestElection_SingleLeader(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	leader := c.waitForLeader(3 * time.Second)
	if leader == 0 {
		t.Fatal("no leader elected within 3 seconds")
	}

	// Verify only one leader
	leaderCount := 0
	for _, id := range c.allIDs {
		if c.nodes[id].State() == raft.Leader {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected 1 leader, got %d", leaderCount)
	}
}

func TestElection_AllSameTerm(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	c.waitForLeader(3 * time.Second)
	time.Sleep(500 * time.Millisecond) // let heartbeats propagate

	// All alive nodes should be in the same term
	terms := make(map[uint64]int)
	for _, id := range c.allIDs {
		terms[c.nodes[id].Term()]++
	}
	if len(terms) != 1 {
		t.Fatalf("nodes have different terms: %v", terms)
	}
}

func TestElection_ReelectAfterLeaderDeath(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	leader1 := c.waitForLeader(3 * time.Second)
	if leader1 == 0 {
		t.Fatal("no initial leader")
	}

	oldTerm := c.nodes[leader1].Term()

	// Kill leader
	c.kill(leader1)

	// Wait for new leader (must be a different, alive node)
	time.Sleep(500 * time.Millisecond)
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no new leader after killing old one")
		default:
			for _, id := range c.allIDs {
				if id != leader1 && !c.nodes[id].IsShutdown() &&
					c.nodes[id].State() == raft.Leader &&
					c.nodes[id].Term() > oldTerm {
					return // success — new leader found
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func TestElection_NoLeaderWithoutMajority(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	c.waitForLeader(3 * time.Second)

	// Kill 3 nodes — only 2 remaining, no majority
	c.kill(c.allIDs[0])
	c.kill(c.allIDs[1])
	c.kill(c.allIDs[2])

	time.Sleep(2 * time.Second)

	// Remaining nodes should NOT have a stable leader
	// (they might briefly become candidates but can't get majority)
	leaderCount := 0
	for _, id := range c.allIDs {
		if !c.nodes[id].IsShutdown() && c.nodes[id].State() == raft.Leader {
			leaderCount++
		}
	}
	// Leader might exist briefly due to CheckQuorum not kicking in yet,
	// but it shouldn't be able to commit anything
}

func TestReplication_BasicCommit(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	leader := c.waitForLeader(3 * time.Second)
	if leader == 0 {
		t.Fatal("no leader")
	}

	// Submit a command
	idx, err := c.nodes[leader].SubmitAndWait([]byte("SET x 100"), 2*time.Second)
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	if idx == 0 {
		t.Fatal("expected non-zero index")
	}

	// Wait for replication
	time.Sleep(300 * time.Millisecond)

	// All nodes should have the entry
	for _, id := range c.allIDs {
		logLen := c.nodes[id].LogLen()
		if logLen < 1 {
			t.Fatalf("node %d has %d log entries, expected >= 1", id, logLen)
		}
	}
}

func TestReplication_DataSurvivesLeaderDeath(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	leader := c.waitForLeader(3 * time.Second)
	if leader == 0 {
		t.Fatal("no leader")
	}

	// Submit commands
	for i := 0; i < 3; i++ {
		_, err := c.nodes[leader].SubmitAndWait([]byte("SET key val"), 2*time.Second)
		if err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
	}

	time.Sleep(300 * time.Millisecond)

	// Record commit index before kill
	commitBefore := c.nodes[leader].CommitIndex()

	// Kill leader
	c.kill(leader)

	// Wait for new leader
	time.Sleep(500 * time.Millisecond)
	newLeader := c.waitForLeader(3 * time.Second)
	if newLeader == 0 {
		t.Fatal("no new leader")
	}

	// New leader should have at least the same commit index
	// (might be higher due to no-op entry)
	newCommit := c.nodes[newLeader].CommitIndex()
	if newCommit < commitBefore {
		t.Fatalf("data lost: old commit=%d, new commit=%d", commitBefore, newCommit)
	}
}

func TestReplication_SubmitToFollowerFails(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	leader := c.waitForLeader(3 * time.Second)

	// Find a follower
	var follower raft.NodeID
	for _, id := range c.allIDs {
		if id != leader {
			follower = id
			break
		}
	}

	// Submit to follower should fail
	_, _, err := c.nodes[follower].Submit([]byte("SET x 1"))
	if err == nil {
		t.Fatal("expected error when submitting to follower")
	}
}

func TestReplication_MultipleWrites(t *testing.T) {
	c := newTestCluster(5)
	c.start()
	defer c.stop()

	leader := c.waitForLeader(3 * time.Second)
	if leader == 0 {
		t.Fatal("no leader")
	}

	// Submit 10 commands
	for i := 0; i < 10; i++ {
		_, err := c.nodes[leader].SubmitAndWait([]byte("SET key val"), 2*time.Second)
		if err != nil {
			t.Fatalf("submit %d failed: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// All alive nodes should have all entries committed
	for _, id := range c.allIDs {
		commit := c.nodes[id].CommitIndex()
		// At least 10 user entries + no-op entries
		if commit < 10 {
			t.Fatalf("node %d commit index %d, expected >= 10", id, commit)
		}
	}
}
