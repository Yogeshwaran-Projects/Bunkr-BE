package chunk

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store is a content-addressable chunk storage.
// Chunks are stored on disk using their SHA256 hash as the path.
// Layout: root/ab/cd/<full-hash>
type Store struct {
	root string
	mu   sync.RWMutex
}

// NewStore creates a chunk store at the given root directory.
func NewStore(root string) *Store {
	return &Store{root: root}
}

// casPath converts a chunk ID (SHA256 hex) to a filesystem path.
// Example: "abcdef1234..." → "root/ab/cd/abcdef1234..."
func (s *Store) casPath(chunkID string) string {
	if len(chunkID) < 4 {
		return filepath.Join(s.root, chunkID)
	}
	return filepath.Join(s.root, chunkID[:2], chunkID[2:4], chunkID)
}

// Put stores a chunk on disk.
func (s *Store) Put(chunkID string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.casPath(chunkID)
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create chunk dir: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	return nil
}

// Get reads a chunk from disk.
func (s *Store) Get(chunkID string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.casPath(chunkID)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk %s: %w", chunkID, err)
	}
	return data, nil
}

// Delete removes a chunk from disk.
func (s *Store) Delete(chunkID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := s.casPath(chunkID)
	return os.Remove(path)
}

// Has checks if a chunk exists on disk.
func (s *Store) Has(chunkID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := s.casPath(chunkID)
	_, err := os.Stat(path)
	return err == nil
}

// List returns all chunk IDs stored on disk.
func (s *Store) List() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ids []string
	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			ids = append(ids, info.Name())
		}
		return nil
	})
	return ids, err
}

// DiskUsage returns total bytes used by chunks.
func (s *Store) DiskUsage() (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var total int64
	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// Root returns the store's root directory.
func (s *Store) Root() string {
	return s.root
}
