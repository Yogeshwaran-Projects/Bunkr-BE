package raft

// RaftLog manages the replicated log entries.
// Supports log compaction: after a snapshot, entries before the snapshot
// are discarded. The offset tracks where the "virtual" log starts.
type RaftLog struct {
	entries []LogEntry

	// Offset for compacted logs. After a snapshot at index N,
	// entries[0] is at logical index N+1. offset = N.
	offset     uint64
	offsetTerm uint64 // term of the entry at offset (from snapshot)
}

// NewRaftLog creates an empty log.
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries: make([]LogEntry, 0),
	}
}

// SetOffset sets the log offset after a snapshot is installed.
func (l *RaftLog) SetOffset(index, term uint64) {
	l.offset = index
	l.offsetTerm = term
}

// Offset returns the current log offset (last compacted index).
func (l *RaftLog) Offset() uint64 {
	return l.offset
}

// toInternal converts a logical (1-based) index to internal slice index.
func (l *RaftLog) toInternal(logicalIndex uint64) int {
	return int(logicalIndex - l.offset - 1)
}

// Append adds a new entry to the end of the log.
func (l *RaftLog) Append(entry LogEntry) {
	l.entries = append(l.entries, entry)
}

// AppendEntries appends multiple entries starting after prevLogIndex.
// Overwrites any conflicting entries (same index but different term).
func (l *RaftLog) AppendEntries(prevLogIndex uint64, entries []LogEntry) {
	for _, entry := range entries {
		idx := l.toInternal(entry.Index)
		if idx < 0 {
			continue // already compacted
		}
		if idx < len(l.entries) {
			if l.entries[idx].Term != entry.Term {
				// Conflict: truncate from here and append
				l.entries = l.entries[:idx]
				l.entries = append(l.entries, entry)
			}
			// Same term = same entry, skip (idempotent)
		} else {
			l.entries = append(l.entries, entry)
		}
	}
}

// GetEntry returns the entry at the given logical index, or nil if not found.
func (l *RaftLog) GetEntry(index uint64) *LogEntry {
	idx := l.toInternal(index)
	if idx < 0 || idx >= len(l.entries) {
		return nil
	}
	return &l.entries[idx]
}

// GetEntries returns entries from startIndex (logical, inclusive) to end.
func (l *RaftLog) GetEntries(startIndex uint64) []LogEntry {
	idx := l.toInternal(startIndex)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(l.entries) {
		return nil
	}
	return l.entries[idx:]
}

// LastIndex returns the logical index of the last entry.
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.offset
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the last entry.
func (l *RaftLog) LastTerm() uint64 {
	if len(l.entries) == 0 {
		return l.offsetTerm
	}
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at the given logical index.
func (l *RaftLog) TermAt(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	if index == l.offset {
		return l.offsetTerm
	}
	entry := l.GetEntry(index)
	if entry == nil {
		return 0
	}
	return entry.Term
}

// Match checks if the log has an entry at prevLogIndex with prevLogTerm.
func (l *RaftLog) Match(prevLogIndex, prevLogTerm uint64) bool {
	if prevLogIndex == 0 {
		return true
	}
	if prevLogIndex == l.offset {
		return l.offsetTerm == prevLogTerm
	}
	return l.TermAt(prevLogIndex) == prevLogTerm
}

// Len returns the number of entries in the log (excluding compacted).
func (l *RaftLog) Len() int {
	return len(l.entries)
}

// TotalLen returns the total logical length including compacted entries.
func (l *RaftLog) TotalLen() uint64 {
	return l.offset + uint64(len(l.entries))
}

// CompactTo discards all entries up to and including the given index.
func (l *RaftLog) CompactTo(index uint64) {
	if index <= l.offset {
		return
	}

	idx := l.toInternal(index)
	if idx < 0 || idx >= len(l.entries) {
		// Compact everything
		l.offsetTerm = l.LastTerm()
		l.offset = l.LastIndex()
		l.entries = nil
		return
	}

	l.offsetTerm = l.entries[idx].Term
	l.offset = l.entries[idx].Index
	l.entries = l.entries[idx+1:]
}

// FirstIndex returns the first available logical index.
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) == 0 {
		return l.offset + 1
	}
	return l.entries[0].Index
}
