package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

// ReadIndex implements linearizable reads without going through the log.
//
// The problem: A leader might have been deposed but doesn't know it yet.
// If a client reads from this stale leader, they get stale data.
//
// The solution (ReadIndex protocol):
//   1. Leader records its current commitIndex as the "readIndex"
//   2. Leader sends heartbeats to confirm it's still the leader
//   3. If majority responds → leader is confirmed → safe to read at readIndex
//   4. Wait until state machine has applied up to readIndex → serve the read
//
// This is what etcd uses. It adds one heartbeat round-trip to reads
// but guarantees linearizability without appending to the log.

// readIndexState tracks a pending ReadIndex request.
type readIndexState struct {
	index    uint64
	acks     int32 // atomic counter
	majority int
	done     chan struct{}
}

// ReadIndex returns the current commit index after confirming leadership.
// The caller should wait until the state machine has applied up to this index
// before serving the read.
func (n *Node) ReadIndex(timeout time.Duration) (uint64, error) {
	n.mu.RLock()
	if n.state != Leader {
		leaderID := n.leaderID
		n.mu.RUnlock()
		return 0, fmt.Errorf("not leader (leader is node %d)", leaderID)
	}

	readIdx := n.commitIndex
	peers := n.peers
	term := n.currentTerm
	n.mu.RUnlock()

	// If using lease-based reads and lease is valid, skip heartbeat round
	if n.config.ReadOnlyOption == ReadOnlyLeaseBased {
		n.mu.RLock()
		leaseValid := time.Since(n.leaderLastContact) < n.config.LeaderLeaseTimeout
		n.mu.RUnlock()
		if leaseValid {
			return readIdx, nil
		}
	}

	// Send heartbeats and count confirmations
	majority := len(peers)/2 + 1
	state := &readIndexState{
		index:    readIdx,
		majority: majority,
		done:     make(chan struct{}),
	}

	for _, peer := range peers {
		go func(p NodeID) {
			n.mu.RLock()
			if n.state != Leader || n.currentTerm != term {
				n.mu.RUnlock()
				return
			}

			nextIdx := n.nextIndex[p]
			if nextIdx == 0 {
				nextIdx = 1
			}
			prevLogIndex := nextIdx - 1
			prevLogTerm := n.raftLog.TermAt(prevLogIndex)
			commitIndex := n.commitIndex
			n.mu.RUnlock()

			resp, err := n.transport.SendAppendEntries(p, AppendEntriesReq{
				Term:         term,
				LeaderID:     n.id,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      nil, // heartbeat
				LeaderCommit: commitIndex,
			})
			if err != nil {
				return
			}

			if resp.Term > term {
				n.mu.Lock()
				if resp.Term > n.currentTerm {
					n.becomeFollower(resp.Term)
				}
				n.mu.Unlock()
				return
			}

			if resp.Success {
				acks := atomic.AddInt32(&state.acks, 1)
				if int(acks) >= state.majority {
					select {
					case state.done <- struct{}{}:
					default:
					}
				}
			}
		}(peer)
	}

	// Wait for majority confirmation or timeout
	select {
	case <-state.done:
		return readIdx, nil
	case <-time.After(timeout):
		return 0, fmt.Errorf("ReadIndex timeout: could not confirm leadership within %v", timeout)
	case <-n.shutdownCh:
		return 0, fmt.Errorf("node shutting down")
	}
}
