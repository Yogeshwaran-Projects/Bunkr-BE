package raft

import "fmt"

// NodeID uniquely identifies a node in the cluster.
type NodeID uint32

// NodeState represents the current role of a Raft node.
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

// LogEntry represents a single entry in the replicated log.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"`
	Command []byte `json:"command,omitempty"`
}

// RequestVoteReq is sent by candidates to gather votes.
type RequestVoteReq struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64

	// IsPreVote indicates this is a pre-vote request (Section 9.6).
	// Pre-votes don't increment the term and don't cause the receiver to step down.
	IsPreVote bool
}

// RequestVoteResp is the response to a vote request.
type RequestVoteResp struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesReq is sent by the leader to replicate log entries and as heartbeats.
type AppendEntriesReq struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResp is the response to an AppendEntries RPC.
type AppendEntriesResp struct {
	Term    uint64
	Success bool

	// Accelerated backtracking (optimization):
	// On failure, the follower returns the conflicting term and the first index
	// of that term, allowing the leader to skip an entire term on retry
	// instead of decrementing nextIndex one by one.
	ConflictTerm  uint64
	ConflictIndex uint64
}

// Event is emitted by nodes for observability (dashboard, logging).
type Event struct {
	Type   string
	NodeID NodeID
	Term   uint64
	Data   map[string]interface{}
}
