package raft

import "testing"

func TestRaftLog_AppendAndGet(t *testing.T) {
	l := NewRaftLog()

	l.Append(LogEntry{Index: 1, Term: 1, Command: []byte("SET x 1")})
	l.Append(LogEntry{Index: 2, Term: 1, Command: []byte("SET y 2")})
	l.Append(LogEntry{Index: 3, Term: 2, Command: []byte("SET z 3")})

	if l.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", l.Len())
	}
	if l.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", l.LastIndex())
	}
	if l.LastTerm() != 2 {
		t.Fatalf("expected last term 2, got %d", l.LastTerm())
	}

	entry := l.GetEntry(2)
	if entry == nil || entry.Term != 1 || string(entry.Command) != "SET y 2" {
		t.Fatalf("unexpected entry at index 2: %+v", entry)
	}

	if l.GetEntry(0) != nil {
		t.Fatal("expected nil for index 0")
	}
	if l.GetEntry(4) != nil {
		t.Fatal("expected nil for index 4")
	}
}

func TestRaftLog_Match(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1})
	l.Append(LogEntry{Index: 2, Term: 1})
	l.Append(LogEntry{Index: 3, Term: 2})

	// prevLogIndex=0 always matches (beginning of log)
	if !l.Match(0, 0) {
		t.Fatal("expected match at index 0")
	}

	// Correct match
	if !l.Match(2, 1) {
		t.Fatal("expected match at index 2, term 1")
	}

	// Wrong term
	if l.Match(2, 2) {
		t.Fatal("expected no match at index 2, term 2")
	}

	// Beyond log
	if l.Match(4, 2) {
		t.Fatal("expected no match at index 4")
	}
}

func TestRaftLog_AppendEntries_Conflict(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1, Command: []byte("a")})
	l.Append(LogEntry{Index: 2, Term: 1, Command: []byte("b")})
	l.Append(LogEntry{Index: 3, Term: 1, Command: []byte("c")})

	// Leader sends entries 2,3 with term 2 (conflict at index 2)
	l.AppendEntries(1, []LogEntry{
		{Index: 2, Term: 2, Command: []byte("B")},
		{Index: 3, Term: 2, Command: []byte("C")},
	})

	if l.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", l.Len())
	}

	e2 := l.GetEntry(2)
	if e2 == nil || e2.Term != 2 || string(e2.Command) != "B" {
		t.Fatalf("expected overwritten entry at index 2, got %+v", e2)
	}

	e3 := l.GetEntry(3)
	if e3 == nil || e3.Term != 2 || string(e3.Command) != "C" {
		t.Fatalf("expected overwritten entry at index 3, got %+v", e3)
	}
}

func TestRaftLog_AppendEntries_Extend(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1, Command: []byte("a")})

	l.AppendEntries(1, []LogEntry{
		{Index: 2, Term: 1, Command: []byte("b")},
		{Index: 3, Term: 1, Command: []byte("c")},
	})

	if l.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", l.Len())
	}
	if l.LastIndex() != 3 {
		t.Fatalf("expected last index 3, got %d", l.LastIndex())
	}
}

func TestRaftLog_AppendEntries_Idempotent(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1, Command: []byte("a")})
	l.Append(LogEntry{Index: 2, Term: 1, Command: []byte("b")})

	// Send same entries again — should be no-op
	l.AppendEntries(0, []LogEntry{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
	})

	if l.Len() != 2 {
		t.Fatalf("expected 2 entries after idempotent append, got %d", l.Len())
	}
}

func TestRaftLog_CompactTo(t *testing.T) {
	l := NewRaftLog()
	l.Append(LogEntry{Index: 1, Term: 1, Command: []byte("a")})
	l.Append(LogEntry{Index: 2, Term: 1, Command: []byte("b")})
	l.Append(LogEntry{Index: 3, Term: 2, Command: []byte("c")})
	l.Append(LogEntry{Index: 4, Term: 2, Command: []byte("d")})
	l.Append(LogEntry{Index: 5, Term: 3, Command: []byte("e")})

	// Compact up to index 3
	l.CompactTo(3)

	if l.Offset() != 3 {
		t.Fatalf("expected offset 3, got %d", l.Offset())
	}
	if l.Len() != 2 {
		t.Fatalf("expected 2 remaining entries, got %d", l.Len())
	}
	if l.LastIndex() != 5 {
		t.Fatalf("expected last index 5, got %d", l.LastIndex())
	}

	// Can still access entries 4 and 5
	e4 := l.GetEntry(4)
	if e4 == nil || string(e4.Command) != "d" {
		t.Fatalf("expected entry 4, got %+v", e4)
	}

	// Can't access compacted entries
	if l.GetEntry(2) != nil {
		t.Fatal("expected nil for compacted entry 2")
	}

	// Match at offset boundary
	if !l.Match(3, 2) {
		t.Fatal("expected match at offset boundary")
	}
}

func TestRaftLog_GetEntries_AfterCompaction(t *testing.T) {
	l := NewRaftLog()
	for i := uint64(1); i <= 10; i++ {
		l.Append(LogEntry{Index: i, Term: 1})
	}

	l.CompactTo(5)

	entries := l.GetEntries(6)
	if len(entries) != 5 {
		t.Fatalf("expected 5 entries from index 6, got %d", len(entries))
	}
	if entries[0].Index != 6 {
		t.Fatalf("expected first entry index 6, got %d", entries[0].Index)
	}
}
