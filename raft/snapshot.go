package raft

import (
	"fmt"
	"sync"
)

// Snapshot represents a point-in-time capture of the state machine.
// Used for log compaction — instead of keeping millions of log entries,
// take a snapshot of the state and discard entries before it.
type Snapshot struct {
	// LastIncludedIndex is the last log entry index included in this snapshot.
	LastIncludedIndex uint64

	// LastIncludedTerm is the term of LastIncludedIndex.
	LastIncludedTerm uint64

	// Data is the serialized state machine (e.g., KV store as JSON).
	Data []byte
}

// InstallSnapshotReq is sent by the leader to a far-behind follower.
// Instead of sending thousands of log entries, send a snapshot.
type InstallSnapshotReq struct {
	Term              uint64
	LeaderID          NodeID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotResp is the response to InstallSnapshot.
type InstallSnapshotResp struct {
	Term uint64
}

// SnapshotStore manages snapshots on disk.
type SnapshotStore struct {
	mu       sync.Mutex
	snapshot *Snapshot
}

// NewSnapshotStore creates a new in-memory snapshot store.
func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{}
}

// Save stores a snapshot.
func (s *SnapshotStore) Save(snap Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshot = &snap
}

// Latest returns the most recent snapshot, or nil if none.
func (s *SnapshotStore) Latest() *Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot
}

// SnapshotFunc is called to create a snapshot of the state machine.
// The application (KV store) provides this function.
type SnapshotFunc func() ([]byte, error)

// RestoreFunc is called to restore the state machine from a snapshot.
type RestoreFunc func(data []byte) error

// SetSnapshotFuncs registers the application's snapshot/restore functions.
func (n *Node) SetSnapshotFuncs(snapFn SnapshotFunc, restoreFn RestoreFunc) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.snapshotFn = snapFn
	n.restoreFn = restoreFn
}

// TakeSnapshot creates a snapshot of the current state and compacts the log.
func (n *Node) TakeSnapshot() error {
	n.mu.Lock()
	if n.snapshotFn == nil {
		n.mu.Unlock()
		return fmt.Errorf("no snapshot function registered")
	}
	lastApplied := n.lastApplied
	lastAppliedTerm := n.raftLog.TermAt(lastApplied)
	snapshotFn := n.snapshotFn
	n.mu.Unlock()

	// Take snapshot outside of lock (can be slow)
	data, err := snapshotFn()
	if err != nil {
		return fmt.Errorf("snapshot func: %w", err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	snap := Snapshot{
		LastIncludedIndex: lastApplied,
		LastIncludedTerm:  lastAppliedTerm,
		Data:              data,
	}

	if n.snapshotStore == nil {
		n.snapshotStore = NewSnapshotStore()
	}
	n.snapshotStore.Save(snap)

	// Compact the log: discard entries up to lastApplied
	n.raftLog.CompactTo(lastApplied)

	// Truncate WAL to match
	if n.wal != nil {
		n.wal.Truncate(n.currentTerm, n.votedFor, n.raftLog.entries)
	}

	n.emitLocked("snapshot_taken", map[string]interface{}{
		"last_included_index": lastApplied,
		"last_included_term":  lastAppliedTerm,
		"data_size":           len(data),
	})

	return nil
}

// maybeTakeSnapshot checks if it's time for an automatic snapshot.
// Called after applying entries. Must hold mu.
func (n *Node) maybeTakeSnapshot() {
	if n.config.SnapshotInterval == 0 || n.snapshotFn == nil {
		return
	}

	// Check if enough entries have been applied since last snapshot
	var lastSnapIdx uint64
	if n.snapshotStore != nil {
		if snap := n.snapshotStore.Latest(); snap != nil {
			lastSnapIdx = snap.LastIncludedIndex
		}
	}

	entriesSinceSnap := n.lastApplied - lastSnapIdx
	if entriesSinceSnap >= n.config.SnapshotInterval && n.lastApplied >= n.config.SnapshotThreshold {
		// Release lock for snapshot (it can be slow)
		go n.TakeSnapshot()
	}
}

// HandleInstallSnapshot processes an incoming InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(req InstallSnapshotReq) InstallSnapshotResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.IsShutdown() {
		return InstallSnapshotResp{Term: n.currentTerm}
	}

	if req.Term < n.currentTerm {
		return InstallSnapshotResp{Term: n.currentTerm}
	}

	n.leaderID = req.LeaderID
	n.becomeFollower(req.Term)

	// If we already have this snapshot or more, ignore
	if req.LastIncludedIndex <= n.commitIndex {
		return InstallSnapshotResp{Term: n.currentTerm}
	}

	// Restore state machine from snapshot
	if n.restoreFn != nil {
		if err := n.restoreFn(req.Data); err != nil {
			// Restore failed — don't update state
			return InstallSnapshotResp{Term: n.currentTerm}
		}
	}

	// Save snapshot
	snap := Snapshot{
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	}
	if n.snapshotStore == nil {
		n.snapshotStore = NewSnapshotStore()
	}
	n.snapshotStore.Save(snap)

	// Discard entire log and replace with snapshot marker
	n.raftLog = NewRaftLog()
	n.raftLog.SetOffset(req.LastIncludedIndex, req.LastIncludedTerm)

	// Update indices
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex

	// Persist
	n.persistState()
	if n.wal != nil {
		n.wal.Truncate(n.currentTerm, n.votedFor, nil)
	}

	n.emitLocked("snapshot_installed", map[string]interface{}{
		"last_included_index": req.LastIncludedIndex,
		"last_included_term":  req.LastIncludedTerm,
	})

	return InstallSnapshotResp{Term: n.currentTerm}
}
