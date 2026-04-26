package raft

import (
	"os"
	"testing"
)

func TestWAL_SaveAndReadState(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-*")
	defer os.RemoveAll(dir)

	wal, err := OpenWAL(dir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer wal.Close()

	// Save state
	if err := wal.SaveState(5, NodeID(3)); err != nil {
		t.Fatalf("save state: %v", err)
	}

	// Read back
	term, votedFor, entries, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if term != 5 {
		t.Fatalf("expected term 5, got %d", term)
	}
	if votedFor != NodeID(3) {
		t.Fatalf("expected votedFor 3, got %d", votedFor)
	}
	if len(entries) != 0 {
		t.Fatalf("expected 0 entries, got %d", len(entries))
	}
}

func TestWAL_SaveAndReadEntries(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-*")
	defer os.RemoveAll(dir)

	wal, err := OpenWAL(dir, 1)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	// Save entries
	entries := []LogEntry{
		{Index: 1, Term: 1, Command: []byte("SET x 1")},
		{Index: 2, Term: 1, Command: []byte("SET y 2")},
		{Index: 3, Term: 2, Command: nil}, // no-op
	}
	if err := wal.SaveEntries(entries); err != nil {
		t.Fatalf("save entries: %v", err)
	}
	wal.Close()

	// Reopen and read
	wal2, _ := OpenWAL(dir, 1)
	defer wal2.Close()

	_, _, readEntries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read all: %v", err)
	}

	if len(readEntries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(readEntries))
	}

	if readEntries[0].Index != 1 || readEntries[0].Term != 1 || string(readEntries[0].Command) != "SET x 1" {
		t.Fatalf("entry 0 mismatch: %+v", readEntries[0])
	}
	if readEntries[2].Index != 3 || readEntries[2].Command != nil {
		t.Fatalf("entry 2 (no-op) mismatch: %+v", readEntries[2])
	}
}

func TestWAL_MultipleStateUpdates(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-*")
	defer os.RemoveAll(dir)

	wal, _ := OpenWAL(dir, 1)

	// Multiple state updates — last one wins
	wal.SaveState(1, NodeID(2))
	wal.SaveState(2, NodeID(0))
	wal.SaveState(3, NodeID(5))
	wal.Close()

	wal2, _ := OpenWAL(dir, 1)
	defer wal2.Close()

	term, votedFor, _, _ := wal2.ReadAll()
	if term != 3 {
		t.Fatalf("expected term 3, got %d", term)
	}
	if votedFor != NodeID(5) {
		t.Fatalf("expected votedFor 5, got %d", votedFor)
	}
}

func TestWAL_Truncate(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-*")
	defer os.RemoveAll(dir)

	wal, _ := OpenWAL(dir, 1)

	// Write a bunch of stuff
	wal.SaveState(5, NodeID(3))
	wal.SaveEntries([]LogEntry{
		{Index: 1, Term: 1, Command: []byte("a")},
		{Index: 2, Term: 1, Command: []byte("b")},
		{Index: 3, Term: 2, Command: []byte("c")},
	})

	// Truncate to just entries 3+
	wal.Truncate(5, NodeID(3), []LogEntry{
		{Index: 3, Term: 2, Command: []byte("c")},
	})
	wal.Close()

	// Read back
	wal2, _ := OpenWAL(dir, 1)
	defer wal2.Close()

	term, votedFor, entries, _ := wal2.ReadAll()
	if term != 5 || votedFor != NodeID(3) {
		t.Fatalf("state mismatch: term=%d votedFor=%d", term, votedFor)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after truncate, got %d", len(entries))
	}
	if entries[0].Index != 3 {
		t.Fatalf("expected entry index 3, got %d", entries[0].Index)
	}
}

func TestWAL_CrashRecovery(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-*")
	defer os.RemoveAll(dir)

	// Write valid data
	wal, _ := OpenWAL(dir, 1)
	wal.SaveState(3, NodeID(2))
	wal.SaveEntries([]LogEntry{
		{Index: 1, Term: 1, Command: []byte("x")},
	})
	wal.Close()

	// Append garbage to simulate crash mid-write
	f, _ := os.OpenFile(dir+"/node-1.wal", os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xFF, 0xFF, 0xFF}) // corrupt bytes
	f.Close()

	// Should recover valid data and ignore corrupt tail
	wal2, _ := OpenWAL(dir, 1)
	defer wal2.Close()

	term, votedFor, entries, err := wal2.ReadAll()
	if err != nil {
		t.Fatalf("read all after crash: %v", err)
	}
	if term != 3 || votedFor != NodeID(2) {
		t.Fatalf("state mismatch after crash recovery: term=%d votedFor=%d", term, votedFor)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry after crash recovery, got %d", len(entries))
	}
}
