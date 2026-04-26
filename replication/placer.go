package replication

import (
	"fmt"

	"github.com/yogeshwaran/bunkr/backend/raft"
)

// DefaultReplicationFactor is the number of copies of each chunk.
const DefaultReplicationFactor = 3

// PlaceChunk selects nodes to store a chunk.
// Distributes evenly using round-robin based on chunk index.
func PlaceChunk(chunkIndex int, aliveNodes []raft.NodeID, replicationFactor int) ([]raft.NodeID, error) {
	if len(aliveNodes) == 0 {
		return nil, fmt.Errorf("no alive nodes available")
	}

	factor := replicationFactor
	if factor > len(aliveNodes) {
		factor = len(aliveNodes)
	}

	selected := make([]raft.NodeID, factor)
	start := chunkIndex % len(aliveNodes)
	for i := 0; i < factor; i++ {
		idx := (start + i) % len(aliveNodes)
		selected[i] = aliveNodes[idx]
	}

	return selected, nil
}
