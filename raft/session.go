package raft

import "sync"

// ClientSession tracks the last applied command per client for exactly-once semantics.
// Without this, a retried command (e.g., "INCREMENT counter") would be applied twice.
//
// How it works:
//   1. Client includes (ClientID, SequenceNum) with every command
//   2. Leader checks: have I already processed this (ClientID, SequenceNum)?
//      - Yes → return cached result (don't re-apply)
//      - No → apply and cache the result
//   3. Each client's sequence numbers are monotonically increasing
//      so we only need to store the LAST one per client.

// SessionEntry stores the last processed command for a client.
type SessionEntry struct {
	SequenceNum uint64
	Response    []byte // cached response
}

// SessionManager manages client sessions for deduplication.
type SessionManager struct {
	mu       sync.RWMutex
	sessions map[string]SessionEntry // clientID → last entry
}

// NewSessionManager creates a new session manager.
func NewSessionManager() *SessionManager {
	return &SessionManager{
		sessions: make(map[string]SessionEntry),
	}
}

// IsDuplicate checks if a command has already been processed.
// Returns (cached response, true) if duplicate, (nil, false) if new.
func (sm *SessionManager) IsDuplicate(clientID string, seqNum uint64) ([]byte, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	entry, exists := sm.sessions[clientID]
	if !exists {
		return nil, false
	}

	if seqNum <= entry.SequenceNum {
		return entry.Response, true
	}
	return nil, false
}

// Register records that a command was processed.
func (sm *SessionManager) Register(clientID string, seqNum uint64, response []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.sessions[clientID] = SessionEntry{
		SequenceNum: seqNum,
		Response:    response,
	}
}

// Snapshot serializes sessions for inclusion in Raft snapshots.
func (sm *SessionManager) Snapshot() map[string]SessionEntry {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	cp := make(map[string]SessionEntry, len(sm.sessions))
	for k, v := range sm.sessions {
		cp[k] = v
	}
	return cp
}

// Restore loads sessions from a snapshot.
func (sm *SessionManager) Restore(sessions map[string]SessionEntry) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.sessions = sessions
}
