package raft

import (
	"fmt"
	"time"
)

// ApplyMsg is sent to the application layer when an entry is committed.
type ApplyMsg struct {
	Index   uint64
	Term    uint64
	Command []byte
}

// Submit appends a command to the leader's log and triggers replication.
// Returns the log index and a channel that closes when committed.
func (n *Node) Submit(command []byte) (uint64, <-chan struct{}, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return 0, nil, fmt.Errorf("not leader (leader is node %d)", n.leaderID)
	}

	entry := LogEntry{
		Index:   n.raftLog.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: command,
	}
	n.raftLog.Append(entry)
	n.persistEntries([]LogEntry{entry})

	commitCh := make(chan struct{}, 1)
	n.commitWaiters[entry.Index] = append(n.commitWaiters[entry.Index], commitCh)

	n.emitLocked("entry_appended", map[string]interface{}{
		"index":   entry.Index,
		"term":    entry.Term,
		"command": string(command),
	})

	go n.replicateToAll()

	return entry.Index, commitCh, nil
}

// SubmitAndWait submits a command and blocks until committed or timeout.
func (n *Node) SubmitAndWait(command []byte, timeout time.Duration) (uint64, error) {
	idx, commitCh, err := n.Submit(command)
	if err != nil {
		return 0, err
	}

	select {
	case <-commitCh:
		return idx, nil
	case <-time.After(timeout):
		return 0, fmt.Errorf("commit timeout for index %d after %v", idx, timeout)
	case <-n.shutdownCh:
		return 0, fmt.Errorf("node shutting down")
	}
}

// replicateToAll triggers replication to all peers.
func (n *Node) replicateToAll() {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	peers := n.peers
	n.mu.RUnlock()

	for _, peer := range peers {
		go n.replicateTo(peer)
	}
}

// replicateTo sends AppendEntries to a single peer with flow control.
func (n *Node) replicateTo(peer NodeID) {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}

	nextIdx := n.nextIndex[peer]
	if nextIdx == 0 {
		nextIdx = 1
	}

	// If follower needs entries we've already compacted, send a snapshot instead
	if nextIdx <= n.raftLog.Offset() && n.snapshotStore != nil {
		snap := n.snapshotStore.Latest()
		if snap != nil && snap.LastIncludedIndex > 0 {
			term := n.currentTerm
			n.mu.RUnlock()
			n.sendSnapshot(peer, snap, term)
			return
		}
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm := n.raftLog.TermAt(prevLogIndex)

	// Respect MaxEntriesPerAppend for flow control
	entries := n.raftLog.GetEntries(nextIdx)
	maxEntries := n.config.MaxEntriesPerAppend
	if maxEntries > 0 && len(entries) > maxEntries {
		entries = entries[:maxEntries]
	}

	term := n.currentTerm
	commitIndex := n.commitIndex
	n.mu.RUnlock()

	// Copy to avoid race
	entriesCopy := make([]LogEntry, len(entries))
	copy(entriesCopy, entries)

	resp, err := n.transport.SendAppendEntries(peer, AppendEntriesReq{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entriesCopy,
		LeaderCommit: commitIndex,
	})
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if n.state != Leader || n.currentTerm != term {
		return
	}

	if resp.Success {
		if len(entriesCopy) > 0 {
			lastSent := entriesCopy[len(entriesCopy)-1].Index
			if lastSent+1 > n.nextIndex[peer] {
				n.nextIndex[peer] = lastSent + 1
			}
			if lastSent > n.matchIndex[peer] {
				n.matchIndex[peer] = lastSent
			}
		}
		n.advanceCommitIndex()

		// If there are more entries to send, continue immediately
		if n.raftLog.LastIndex() >= n.nextIndex[peer] {
			go n.replicateTo(peer)
		}
	} else {
		// Accelerated backtracking: use conflict info to skip terms
		if resp.ConflictTerm > 0 {
			// Find the last entry of ConflictTerm in our log
			found := false
			for i := n.raftLog.LastIndex(); i >= 1; i-- {
				if n.raftLog.TermAt(i) == resp.ConflictTerm {
					n.nextIndex[peer] = i + 1
					found = true
					break
				}
				if n.raftLog.TermAt(i) < resp.ConflictTerm {
					break
				}
			}
			if !found {
				// We don't have any entries with ConflictTerm — use ConflictIndex
				n.nextIndex[peer] = resp.ConflictIndex
			}
		} else if resp.ConflictIndex > 0 {
			// Follower's log is shorter
			n.nextIndex[peer] = resp.ConflictIndex
		} else {
			// Fallback: decrement by 1
			if n.nextIndex[peer] > 1 {
				n.nextIndex[peer]--
			}
		}

		// Retry with updated nextIndex
		go n.replicateTo(peer)
	}
}

// advanceCommitIndex checks if we can commit more entries. Must hold mu.
func (n *Node) advanceCommitIndex() {
	for idx := n.commitIndex + 1; idx <= n.raftLog.LastIndex(); idx++ {
		// Raft safety: only commit entries from current term (Section 5.4.2)
		if n.raftLog.TermAt(idx) != n.currentTerm {
			continue
		}

		replicatedCount := 1 // leader has it
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				replicatedCount++
			}
		}

		majority := (len(n.peers)+1)/2 + 1
		if replicatedCount >= majority {
			n.commitIndex = idx

			n.emitLocked("entry_committed", map[string]interface{}{
				"index":       idx,
				"term":        n.raftLog.TermAt(idx),
				"replicated":  replicatedCount,
				"total_nodes": len(n.peers) + 1,
			})

			// Notify commit waiters
			if chs, ok := n.commitWaiters[idx]; ok {
				for _, ch := range chs {
					select {
					case ch <- struct{}{}:
					default:
					}
				}
				delete(n.commitWaiters, idx)
			}
		}
	}

	n.applyCommitted()
}

// applyCommitted sends committed entries to the apply channel. Must hold mu.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.raftLog.GetEntry(n.lastApplied)
		if entry == nil {
			continue
		}

		// Skip no-op entries
		if entry.Command == nil {
			continue
		}

		select {
		case n.applyCh <- ApplyMsg{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		}:
		default:
		}

		n.emitLocked("entry_applied", map[string]interface{}{
			"index":   entry.Index,
			"command": string(entry.Command),
		})
	}

	// Check if it's time for an automatic snapshot
	n.maybeTakeSnapshot()
}

// sendSnapshot sends a snapshot to a far-behind follower via InstallSnapshot RPC.
func (n *Node) sendSnapshot(peer NodeID, snap *Snapshot, term uint64) {
	resp := n.transport.SendInstallSnapshot(peer, InstallSnapshotReq{
		Term:              term,
		LeaderID:          n.id,
		LastIncludedIndex: snap.LastIncludedIndex,
		LastIncludedTerm:  snap.LastIncludedTerm,
		Data:              snap.Data,
	})

	n.mu.Lock()
	defer n.mu.Unlock()

	if resp.Term > n.currentTerm {
		n.becomeFollower(resp.Term)
		return
	}

	if n.state == Leader {
		n.nextIndex[peer] = snap.LastIncludedIndex + 1
		n.matchIndex[peer] = snap.LastIncludedIndex
	}
}
