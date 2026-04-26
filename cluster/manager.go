package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/yogeshwaran/bunkr/backend/chunk"
	"github.com/yogeshwaran/bunkr/backend/metadata"
	"github.com/yogeshwaran/bunkr/backend/raft"
	"github.com/yogeshwaran/bunkr/backend/transport"
)

// NodeInfo holds a node, its metadata store, and chunk store.
type NodeInfo struct {
	Node       *raft.Node
	MetaStore  *metadata.Store
	ChunkStore *chunk.Store
	Alive      bool
	StopFwd    chan struct{}
}

// Manager manages the entire Bunkr cluster.
type Manager struct {
	mu        sync.RWMutex
	nodes     map[raft.NodeID]*NodeInfo
	allIDs    []raft.NodeID
	transport *transport.ChannelTransport
	eventCh   chan raft.Event
	dataDir   string
}

// NewManager creates a cluster manager with the given number of nodes.
func NewManager(nodeCount int, dataDir string) *Manager {
	tp := transport.NewChannelTransport()
	allIDs := make([]raft.NodeID, nodeCount)
	for i := 0; i < nodeCount; i++ {
		allIDs[i] = raft.NodeID(i + 1)
	}

	m := &Manager{
		nodes:     make(map[raft.NodeID]*NodeInfo),
		allIDs:    allIDs,
		transport: tp,
		eventCh:   make(chan raft.Event, 1024),
		dataDir:   dataDir,
	}

	for _, id := range allIDs {
		peers := makePeers(allIDs, id)
		node := raft.NewNode(id, peers, tp, raft.WithConfig(raft.DefaultConfig()))
		metaStore := metadata.NewStore()
		chunkStore := chunk.NewStore(fmt.Sprintf("%s/node-%d/chunks", dataDir, id))

		m.nodes[id] = &NodeInfo{
			Node:       node,
			MetaStore:  metaStore,
			ChunkStore: chunkStore,
			Alive:      false,
			StopFwd:    make(chan struct{}),
		}
		tp.Register(id, node)
	}

	return m
}

// Start starts all nodes and begins consuming apply channels.
func (m *Manager) Start() {
	for _, id := range m.allIDs {
		info := m.nodes[id]
		info.Node.Start() //nolint:errcheck
		info.Alive = true
		m.startForwarding(info)
	}
}

// startForwarding starts event and apply forwarding goroutines for a node.
func (m *Manager) startForwarding(info *NodeInfo) {
	// Forward events
	go func() {
		for {
			select {
			case <-info.StopFwd:
				return
			case event, ok := <-info.Node.Events():
				if !ok {
					return
				}
				select {
				case m.eventCh <- event:
				default:
				}
			}
		}
	}()

	// Apply committed entries to metadata store
	go func() {
		for {
			select {
			case <-info.StopFwd:
				return
			case msg, ok := <-info.Node.ApplyCh():
				if !ok {
					return
				}
				info.MetaStore.Apply(msg.Command)
			}
		}
	}()
}

// Events returns the merged event channel from all nodes.
func (m *Manager) Events() <-chan raft.Event {
	return m.eventCh
}

// KillNode stops a node and makes it unreachable.
func (m *Manager) KillNode(id raft.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.nodes[id]
	if !ok {
		return fmt.Errorf("node %d not found", id)
	}
	if !info.Alive {
		return fmt.Errorf("node %d already dead", id)
	}

	info.Node.Stop()
	m.transport.Unregister(id)
	close(info.StopFwd)
	info.Alive = false
	return nil
}

// ReviveNode restarts a dead node.
func (m *Manager) ReviveNode(id raft.NodeID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	info, ok := m.nodes[id]
	if !ok {
		return fmt.Errorf("node %d not found", id)
	}
	if info.Alive {
		return fmt.Errorf("node %d already alive", id)
	}

	peers := makePeers(m.allIDs, id)
	newNode := raft.NewNode(id, peers, m.transport, raft.WithConfig(raft.DefaultConfig()))
	newMetaStore := metadata.NewStore()

	newInfo := &NodeInfo{
		Node:       newNode,
		MetaStore:  newMetaStore,
		ChunkStore: info.ChunkStore, // reuse existing chunk store (data persists on disk)
		Alive:      true,
		StopFwd:    make(chan struct{}),
	}

	m.nodes[id] = newInfo
	m.transport.Register(id, newNode)
	newNode.Start() //nolint:errcheck
	m.startForwarding(newInfo)

	return nil
}

// Submit sends a command to the current leader with proper commit waiting.
func (m *Manager) Submit(command []byte) (uint64, error) {
	m.mu.RLock()
	var leader *raft.Node
	for _, id := range m.allIDs {
		info := m.nodes[id]
		if info.Alive && info.Node.State() == raft.Leader {
			leader = info.Node
			break
		}
	}
	m.mu.RUnlock()

	if leader == nil {
		return 0, fmt.Errorf("no leader available")
	}

	return leader.SubmitAndWait(command, 5*time.Second)
}

// GetLeader returns the current leader ID, or 0 if none.
func (m *Manager) GetLeader() raft.NodeID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, id := range m.allIDs {
		info := m.nodes[id]
		if info.Alive && info.Node.State() == raft.Leader {
			return id
		}
	}
	return 0
}

// GetLeaderMetaStore returns the metadata store for the leader node.
func (m *Manager) GetLeaderMetaStore() *metadata.Store {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, id := range m.allIDs {
		info := m.nodes[id]
		if info.Alive && info.Node.State() == raft.Leader {
			return info.MetaStore
		}
	}
	return nil
}

// GetNodeChunkStore returns the chunk store for a specific node.
func (m *Manager) GetNodeChunkStore(id raft.NodeID) *chunk.Store {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, ok := m.nodes[id]
	if !ok {
		return nil
	}
	return info.ChunkStore
}

// GetAliveNodeIDs returns IDs of all alive nodes.
func (m *Manager) GetAliveNodeIDs() []raft.NodeID {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var ids []raft.NodeID
	for _, id := range m.allIDs {
		if m.nodes[id].Alive {
			ids = append(ids, id)
		}
	}
	return ids
}

// NodeStatus holds status info for API responses.
type NodeStatus struct {
	ID          raft.NodeID `json:"id"`
	State       string      `json:"state"`
	Term        uint64      `json:"term"`
	LogLen      int         `json:"log_length"`
	CommitIndex uint64      `json:"commit_index"`
	Alive       bool        `json:"alive"`
	IsLeader    bool        `json:"is_leader"`
	ChunkCount  int         `json:"chunk_count"`
	DiskUsage   int64       `json:"disk_usage"`
}

// ClusterStatus returns status of all nodes.
func (m *Manager) ClusterStatus() []NodeStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	statuses := make([]NodeStatus, 0, len(m.allIDs))
	for _, id := range m.allIDs {
		info := m.nodes[id]
		status := NodeStatus{
			ID:    id,
			Alive: info.Alive,
		}
		if info.Alive {
			status.State = info.Node.State().String()
			status.Term = info.Node.Term()
			status.LogLen = info.Node.LogLen()
			status.CommitIndex = info.Node.CommitIndex()
			status.IsLeader = info.Node.State() == raft.Leader
		} else {
			status.State = "Dead"
		}

		// Chunk stats (from disk, safe even if node is dead)
		chunks, _ := info.ChunkStore.List()
		status.ChunkCount = len(chunks)
		usage, _ := info.ChunkStore.DiskUsage()
		status.DiskUsage = usage

		statuses = append(statuses, status)
	}
	return statuses
}

// AllIDs returns all node IDs.
func (m *Manager) AllIDs() []raft.NodeID {
	return m.allIDs
}

// FileCount returns the file count from the leader's metadata store.
func (m *Manager) FileCount() int {
	store := m.GetLeaderMetaStore()
	if store == nil {
		return 0
	}
	return store.FileCount()
}

func makePeers(allIDs []raft.NodeID, exclude raft.NodeID) []raft.NodeID {
	peers := make([]raft.NodeID, 0, len(allIDs)-1)
	for _, id := range allIDs {
		if id != exclude {
			peers = append(peers, id)
		}
	}
	return peers
}
