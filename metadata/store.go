package metadata

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Store is the file metadata state machine that sits on top of Raft.
// It tracks which files exist, their chunks, and where chunks are stored.
type Store struct {
	mu       sync.RWMutex
	files    map[string]*FileMeta // file ID → metadata
	names    map[string]string    // file name → file ID (for lookup by name)
	chunkIdx map[string]string    // chunk ID → file ID (reverse index for O(1) chunk lookups)
}

// NewStore creates an empty metadata store.
func NewStore() *Store {
	return &Store{
		files:    make(map[string]*FileMeta),
		names:    make(map[string]string),
		chunkIdx: make(map[string]string),
	}
}

// Apply executes a command against the store. Called when a Raft log entry is committed.
func (s *Store) Apply(data []byte) Result {
	cmd, err := DecodeCommand(data)
	if err != nil {
		return Result{Ok: false, Error: err.Error()}
	}

	switch cmd.Op {
	case OpRegisterFile:
		return s.applyRegisterFile(cmd)
	case OpDeleteFile:
		return s.applyDeleteFile(cmd)
	case OpAddReplica:
		return s.applyAddReplica(cmd)
	case OpRemoveReplica:
		return s.applyRemoveReplica(cmd)
	default:
		return Result{Ok: false, Error: "unknown op: " + cmd.Op}
	}
}

func (s *Store) applyRegisterFile(cmd Command) Result {
	if cmd.FileMeta == nil {
		return Result{Ok: false, Error: "missing file metadata"}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check for duplicate name — remove old file and its chunk index entries
	if existingID, exists := s.names[cmd.FileMeta.Name]; exists {
		if oldFile, ok := s.files[existingID]; ok {
			for _, c := range oldFile.Chunks {
				delete(s.chunkIdx, c.ID)
			}
		}
		delete(s.files, existingID)
	}

	s.files[cmd.FileMeta.ID] = cmd.FileMeta
	s.names[cmd.FileMeta.Name] = cmd.FileMeta.ID

	// Build chunk reverse index
	for _, c := range cmd.FileMeta.Chunks {
		s.chunkIdx[c.ID] = cmd.FileMeta.ID
	}

	return Result{Ok: true, File: cmd.FileMeta}
}

func (s *Store) applyDeleteFile(cmd Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	file, exists := s.files[cmd.FileID]
	if !exists {
		return Result{Ok: false, Error: "file not found"}
	}

	// Clean up chunk reverse index
	for _, c := range file.Chunks {
		delete(s.chunkIdx, c.ID)
	}

	delete(s.names, file.Name)
	delete(s.files, cmd.FileID)

	return Result{Ok: true}
}

func (s *Store) applyAddReplica(cmd Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	fileID, ok := s.chunkIdx[cmd.ChunkID]
	if !ok {
		return Result{Ok: false, Error: "chunk not found"}
	}

	file := s.files[fileID]
	for i, chunk := range file.Chunks {
		if chunk.ID == cmd.ChunkID {
			// Check if node already has this chunk (idempotent)
			for _, n := range chunk.Nodes {
				if n == cmd.Node {
					return Result{Ok: true}
				}
			}
			file.Chunks[i].Nodes = append(file.Chunks[i].Nodes, cmd.Node)
			return Result{Ok: true}
		}
	}

	return Result{Ok: false, Error: "chunk not found"}
}

func (s *Store) applyRemoveReplica(cmd Command) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	fileID, ok := s.chunkIdx[cmd.ChunkID]
	if !ok {
		return Result{Ok: false, Error: "chunk not found"}
	}

	file := s.files[fileID]
	for i, chunk := range file.Chunks {
		if chunk.ID == cmd.ChunkID {
			nodes := make([]string, 0, len(chunk.Nodes))
			for _, n := range chunk.Nodes {
				if n != cmd.Node {
					nodes = append(nodes, n)
				}
			}
			file.Chunks[i].Nodes = nodes
			return Result{Ok: true}
		}
	}

	return Result{Ok: false, Error: "chunk not found"}
}

// GetFile returns file metadata by ID.
func (s *Store) GetFile(id string) (*FileMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.files[id]
	return f, ok
}

// GetFileByName returns file metadata by name.
func (s *Store) GetFileByName(name string) (*FileMeta, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.names[name]
	if !ok {
		return nil, false
	}
	f, ok := s.files[id]
	return f, ok
}

// ListFiles returns all file metadata.
func (s *Store) ListFiles() []FileMeta {
	s.mu.RLock()
	defer s.mu.RUnlock()
	files := make([]FileMeta, 0, len(s.files))
	for _, f := range s.files {
		files = append(files, *f)
	}
	return files
}

// GetChunkLocations returns the nodes holding a specific chunk.
func (s *Store) GetChunkLocations(chunkID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, file := range s.files {
		for _, chunk := range file.Chunks {
			if chunk.ID == chunkID {
				nodes := make([]string, len(chunk.Nodes))
				copy(nodes, chunk.Nodes)
				return nodes, nil
			}
		}
	}

	return nil, fmt.Errorf("chunk %s not found", chunkID)
}

// FileCount returns the number of stored files.
func (s *Store) FileCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.files)
}

// storeSnapshot is the serialization format for snapshots.
type storeSnapshot struct {
	Files    map[string]*FileMeta `json:"files"`
	Names    map[string]string    `json:"names"`
	ChunkIdx map[string]string    `json:"chunk_idx"`
}

// Snapshot serializes the entire store for Raft log compaction.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := storeSnapshot{
		Files:    s.files,
		Names:    s.names,
		ChunkIdx: s.chunkIdx,
	}
	return json.Marshal(snap)
}

// Restore loads the store from a snapshot.
func (s *Store) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var snap storeSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}
	s.files = snap.Files
	s.names = snap.Names
	s.chunkIdx = snap.ChunkIdx
	if s.chunkIdx == nil {
		s.chunkIdx = make(map[string]string)
	}
	return nil
}
