package replication

import (
	"fmt"
	"log/slog"

	"github.com/yogeshwaran/bunkr/backend/chunk"
	"github.com/yogeshwaran/bunkr/backend/raft"
)

// Manager handles chunk distribution across nodes.
type Manager struct {
	replicationFactor int
	getChunkStore     func(raft.NodeID) *chunk.Store
	getAliveNodes     func() []raft.NodeID
	logger            *slog.Logger
}

// NewManager creates a replication manager.
func NewManager(
	replicationFactor int,
	getChunkStore func(raft.NodeID) *chunk.Store,
	getAliveNodes func() []raft.NodeID,
) *Manager {
	return &Manager{
		replicationFactor: replicationFactor,
		getChunkStore:     getChunkStore,
		getAliveNodes:     getAliveNodes,
		logger:            slog.Default(),
	}
}

// ReplicateChunk stores encrypted chunk data on the target nodes.
func (m *Manager) ReplicateChunk(chunkID string, data []byte, targets []raft.NodeID) error {
	var lastErr error
	successCount := 0

	for _, nodeID := range targets {
		store := m.getChunkStore(nodeID)
		if store == nil {
			lastErr = fmt.Errorf("chunk store not found for node %d", nodeID)
			m.logger.Warn("chunk store not found", "node", nodeID)
			continue
		}

		if err := store.Put(chunkID, data); err != nil {
			lastErr = err
			m.logger.Error("failed to store chunk", "node", nodeID, "chunk", chunkID[:8], "err", err)
			continue
		}

		successCount++
		m.logger.Info("chunk replicated", "node", nodeID, "chunk", chunkID[:8])
	}

	if successCount == 0 {
		return fmt.Errorf("failed to replicate chunk %s to any node: %w", chunkID[:8], lastErr)
	}

	return nil
}

// FetchChunk retrieves a chunk from any of the source nodes.
func (m *Manager) FetchChunk(chunkID string, sources []raft.NodeID) ([]byte, error) {
	for _, nodeID := range sources {
		store := m.getChunkStore(nodeID)
		if store == nil {
			continue
		}

		data, err := store.Get(chunkID)
		if err == nil {
			return data, nil
		}

		m.logger.Warn("chunk fetch failed, trying next node", "node", nodeID, "chunk", chunkID[:8], "err", err)
	}

	return nil, fmt.Errorf("chunk %s not available on any source node", chunkID[:8])
}

// DeleteChunk removes a chunk from the specified nodes.
func (m *Manager) DeleteChunk(chunkID string, nodes []raft.NodeID) {
	for _, nodeID := range nodes {
		store := m.getChunkStore(nodeID)
		if store == nil {
			continue
		}
		store.Delete(chunkID) //nolint:errcheck
	}
}

// PlaceAndReplicate determines placement and replicates a chunk.
func (m *Manager) PlaceAndReplicate(chunkIndex int, chunkID string, data []byte) ([]raft.NodeID, error) {
	aliveNodes := m.getAliveNodes()

	targets, err := PlaceChunk(chunkIndex, aliveNodes, m.replicationFactor)
	if err != nil {
		return nil, err
	}

	if err := m.ReplicateChunk(chunkID, data, targets); err != nil {
		return nil, err
	}

	return targets, nil
}
