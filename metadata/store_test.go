package metadata

import (
	"fmt"
	"testing"
	"time"
)

func TestStore_RegisterAndGetFile(t *testing.T) {
	s := NewStore()

	file := &FileMeta{
		ID:        "file-001",
		Name:      "photo.png",
		Size:      1024 * 1024,
		ChunkSize: 4 * 1024 * 1024,
		Chunks: []ChunkMeta{
			{ID: "chunk-aaa", Index: 0, Size: 1024 * 1024, Nodes: []string{"node-1", "node-2", "node-3"}},
		},
		CreatedAt: time.Now(),
	}

	cmd := Command{Op: OpRegisterFile, FileMeta: file}
	result := s.Apply(cmd.Encode())
	if !result.Ok {
		t.Fatalf("expected ok, got error: %s", result.Error)
	}

	// Get by ID
	got, ok := s.GetFile("file-001")
	if !ok {
		t.Fatal("expected to find file by ID")
	}
	if got.Name != "photo.png" {
		t.Errorf("expected name photo.png, got %s", got.Name)
	}

	// Get by name
	got, ok = s.GetFileByName("photo.png")
	if !ok {
		t.Fatal("expected to find file by name")
	}
	if got.ID != "file-001" {
		t.Errorf("expected ID file-001, got %s", got.ID)
	}
}

func TestStore_DeleteFile(t *testing.T) {
	s := NewStore()

	file := &FileMeta{ID: "file-001", Name: "test.txt", Size: 100}
	s.Apply(Command{Op: OpRegisterFile, FileMeta: file}.Encode())

	result := s.Apply(Command{Op: OpDeleteFile, FileID: "file-001"}.Encode())
	if !result.Ok {
		t.Fatalf("delete failed: %s", result.Error)
	}

	_, ok := s.GetFile("file-001")
	if ok {
		t.Fatal("expected file to be deleted")
	}

	_, ok = s.GetFileByName("test.txt")
	if ok {
		t.Fatal("expected name lookup to be deleted")
	}
}

func TestStore_DeleteNonExistent(t *testing.T) {
	s := NewStore()
	result := s.Apply(Command{Op: OpDeleteFile, FileID: "nope"}.Encode())
	if result.Ok {
		t.Fatal("expected error deleting non-existent file")
	}
}

func TestStore_OverwriteByName(t *testing.T) {
	s := NewStore()

	s.Apply(Command{Op: OpRegisterFile, FileMeta: &FileMeta{ID: "v1", Name: "data.bin", Size: 100}}.Encode())
	s.Apply(Command{Op: OpRegisterFile, FileMeta: &FileMeta{ID: "v2", Name: "data.bin", Size: 200}}.Encode())

	// Should have only 1 file, the new one
	if s.FileCount() != 1 {
		t.Fatalf("expected 1 file, got %d", s.FileCount())
	}

	f, ok := s.GetFileByName("data.bin")
	if !ok {
		t.Fatal("expected to find data.bin")
	}
	if f.ID != "v2" || f.Size != 200 {
		t.Errorf("expected v2/200, got %s/%d", f.ID, f.Size)
	}
}

func TestStore_ListFiles(t *testing.T) {
	s := NewStore()

	for i := 0; i < 5; i++ {
		file := &FileMeta{
			ID:   fmt.Sprintf("file-%d", i),
			Name: fmt.Sprintf("file_%d.txt", i),
			Size: int64(i * 100),
		}
		s.Apply(Command{Op: OpRegisterFile, FileMeta: file}.Encode())
	}

	files := s.ListFiles()
	if len(files) != 5 {
		t.Fatalf("expected 5 files, got %d", len(files))
	}
}

func TestStore_AddRemoveReplica(t *testing.T) {
	s := NewStore()

	file := &FileMeta{
		ID:   "file-001",
		Name: "test.bin",
		Size: 1024,
		Chunks: []ChunkMeta{
			{ID: "chunk-aaa", Index: 0, Size: 1024, Nodes: []string{"node-1"}},
		},
	}
	s.Apply(Command{Op: OpRegisterFile, FileMeta: file}.Encode())

	// Add replica
	result := s.Apply(Command{Op: OpAddReplica, ChunkID: "chunk-aaa", Node: "node-2"}.Encode())
	if !result.Ok {
		t.Fatalf("add replica failed: %s", result.Error)
	}

	nodes, _ := s.GetChunkLocations("chunk-aaa")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}

	// Add duplicate — should be idempotent
	s.Apply(Command{Op: OpAddReplica, ChunkID: "chunk-aaa", Node: "node-2"}.Encode())
	nodes, _ = s.GetChunkLocations("chunk-aaa")
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes after duplicate add, got %d", len(nodes))
	}

	// Remove replica
	result = s.Apply(Command{Op: OpRemoveReplica, ChunkID: "chunk-aaa", Node: "node-1"}.Encode())
	if !result.Ok {
		t.Fatalf("remove replica failed: %s", result.Error)
	}

	nodes, _ = s.GetChunkLocations("chunk-aaa")
	if len(nodes) != 1 || nodes[0] != "node-2" {
		t.Fatalf("expected [node-2], got %v", nodes)
	}
}

func TestStore_SnapshotRestore(t *testing.T) {
	s := NewStore()

	file := &FileMeta{
		ID:   "file-001",
		Name: "snapshot_test.dat",
		Size: 8 * 1024 * 1024,
		Chunks: []ChunkMeta{
			{ID: "chunk-1", Index: 0, Size: 4 * 1024 * 1024, Nodes: []string{"node-1", "node-3"}},
			{ID: "chunk-2", Index: 1, Size: 4 * 1024 * 1024, Nodes: []string{"node-2", "node-3"}},
		},
		CreatedAt: time.Now(),
	}
	s.Apply(Command{Op: OpRegisterFile, FileMeta: file}.Encode())

	// Snapshot
	data, err := s.Snapshot()
	if err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	// Restore to new store
	s2 := NewStore()
	if err := s2.Restore(data); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	// Verify
	f, ok := s2.GetFileByName("snapshot_test.dat")
	if !ok {
		t.Fatal("expected to find file after restore")
	}
	if f.ID != "file-001" {
		t.Errorf("expected ID file-001, got %s", f.ID)
	}
	if len(f.Chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(f.Chunks))
	}
	if f.Chunks[0].ID != "chunk-1" {
		t.Errorf("expected chunk-1, got %s", f.Chunks[0].ID)
	}

	nodes, err := s2.GetChunkLocations("chunk-2")
	if err != nil {
		t.Fatalf("chunk locations failed: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes for chunk-2, got %d", len(nodes))
	}
}
