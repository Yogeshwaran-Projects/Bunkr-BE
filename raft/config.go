package raft

import (
	"fmt"
	"time"
)

// Config holds all tunable parameters for a Raft node.
// These match production defaults used by etcd and hashicorp/raft.
type Config struct {
	// HeartbeatTimeout is the interval between heartbeats sent by the leader.
	// Must be significantly less than ElectionTimeout.
	// etcd uses 100ms, hashicorp/raft uses 1000ms.
	HeartbeatTimeout time.Duration

	// ElectionTimeoutMin and ElectionTimeoutMax define the random range for
	// election timeouts. A follower starts an election if it doesn't hear from
	// the leader within this window.
	//
	// The relationship MUST be:
	//   broadcastTime << ElectionTimeout << MTBF
	//
	// etcd uses 1000-1500ms, hashicorp/raft uses 1000-2000ms.
	// We use 150-300ms for faster demo, but these are configurable.
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration

	// MaxEntriesPerAppend limits how many log entries are sent in a single
	// AppendEntries RPC. Prevents overwhelming slow followers.
	// 0 = no limit.
	MaxEntriesPerAppend int

	// SnapshotInterval is how many applied entries between automatic snapshots.
	// 0 = disable automatic snapshots.
	SnapshotInterval uint64

	// SnapshotThreshold is the minimum number of log entries before a snapshot
	// is allowed. Prevents snapshots when the log is small.
	SnapshotThreshold uint64

	// MaxInflightAppends limits how many AppendEntries RPCs can be in-flight
	// to a single follower simultaneously. Prevents unbounded goroutine growth.
	MaxInflightAppends int

	// PreVote enables the Pre-Vote protocol extension (Section 9.6 of Raft thesis).
	// Prevents partitioned nodes from incrementing terms and disrupting the cluster.
	PreVote bool

	// CheckQuorum makes the leader step down if it can't communicate with a
	// majority of the cluster within an election timeout period.
	// Prevents a partitioned leader from serving stale reads.
	CheckQuorum bool

	// LeaderLeaseTimeout is how long a leader's lease is valid for serving reads
	// without a heartbeat round-trip. Only used if ReadOnlyOption is LeaseBased.
	// Must be less than ElectionTimeoutMin to prevent reading from a stale leader.
	LeaderLeaseTimeout time.Duration

	// ReadOnlyOption determines how read-only queries are handled.
	ReadOnlyOption ReadOnlyOption

	// Logger for this node. If nil, uses default log package.
	LogLevel LogLevel
}

// ReadOnlyOption specifies how the state machine handles read-only queries.
type ReadOnlyOption int

const (
	// ReadOnlySafe requires a round of heartbeats to confirm leadership before
	// serving a read. This is the safest option (linearizable reads).
	ReadOnlySafe ReadOnlyOption = iota

	// ReadOnlyLeaseBased trusts the leader lease and serves reads immediately.
	// Faster but depends on bounded clock drift.
	ReadOnlyLeaseBased
)

// LogLevel controls verbosity.
type LogLevel int

const (
	LogLevelError LogLevel = iota
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
)

// DefaultConfig returns a config with production-safe defaults.
func DefaultConfig() Config {
	return Config{
		HeartbeatTimeout:    50 * time.Millisecond,
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		MaxEntriesPerAppend: 64,
		SnapshotInterval:    10000,
		SnapshotThreshold:   1000,
		MaxInflightAppends:  1,
		PreVote:             true,
		CheckQuorum:         true,
		LeaderLeaseTimeout:  100 * time.Millisecond,
		ReadOnlyOption:      ReadOnlySafe,
		LogLevel:            LogLevelInfo,
	}
}

// Validate checks that the config is sane.
func (c Config) Validate() error {
	if c.HeartbeatTimeout <= 0 {
		return errorf("HeartbeatTimeout must be > 0")
	}
	if c.ElectionTimeoutMin <= 0 || c.ElectionTimeoutMax <= 0 {
		return errorf("ElectionTimeout must be > 0")
	}
	if c.ElectionTimeoutMin >= c.ElectionTimeoutMax {
		return errorf("ElectionTimeoutMin must be < ElectionTimeoutMax")
	}
	if c.HeartbeatTimeout >= c.ElectionTimeoutMin {
		return errorf("HeartbeatTimeout (%v) must be < ElectionTimeoutMin (%v)", c.HeartbeatTimeout, c.ElectionTimeoutMin)
	}
	if c.LeaderLeaseTimeout > c.ElectionTimeoutMin {
		return errorf("LeaderLeaseTimeout must be <= ElectionTimeoutMin")
	}
	return nil
}

func errorf(format string, args ...interface{}) error {
	return fmt.Errorf("raft config: "+format, args...)
}
