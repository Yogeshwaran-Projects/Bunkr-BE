package transport

import (
	"fmt"
	"sync"
	"time"

	"github.com/yogeshwaran/bunkr/backend/raft"
)

// RPCHandler processes incoming RPCs on a node.
type RPCHandler interface {
	HandleRequestVote(req raft.RequestVoteReq) raft.RequestVoteResp
	HandleAppendEntries(req raft.AppendEntriesReq) raft.AppendEntriesResp
	HandleInstallSnapshot(req raft.InstallSnapshotReq) raft.InstallSnapshotResp
}

// ChannelTransport implements Transport using Go channels.
// All nodes run in-process as goroutines. No network involved.
type ChannelTransport struct {
	mu       sync.RWMutex
	handlers map[raft.NodeID]RPCHandler
	timeout  time.Duration
}

// NewChannelTransport creates a new channel-based transport.
func NewChannelTransport() *ChannelTransport {
	return &ChannelTransport{
		handlers: make(map[raft.NodeID]RPCHandler),
		timeout:  500 * time.Millisecond,
	}
}

// Register adds a node's RPC handler so other nodes can reach it.
func (t *ChannelTransport) Register(id raft.NodeID, handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handlers[id] = handler
}

// Unregister removes a node (simulates crash — node becomes unreachable).
func (t *ChannelTransport) Unregister(id raft.NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.handlers, id)
}

// getHandler returns the handler for a node, or nil if unreachable.
func (t *ChannelTransport) getHandler(target raft.NodeID) RPCHandler {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.handlers[target]
}

// SendRequestVote sends a vote request to the target node.
func (t *ChannelTransport) SendRequestVote(target raft.NodeID, req raft.RequestVoteReq) (raft.RequestVoteResp, error) {
	handler := t.getHandler(target)
	if handler == nil {
		return raft.RequestVoteResp{}, fmt.Errorf("node %d unreachable", target)
	}

	// Run in a goroutine with timeout to simulate network behavior
	type result struct {
		resp raft.RequestVoteResp
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp := handler.HandleRequestVote(req)
		ch <- result{resp: resp}
	}()

	select {
	case r := <-ch:
		return r.resp, r.err
	case <-time.After(t.timeout):
		return raft.RequestVoteResp{}, fmt.Errorf("RequestVote to node %d timed out", target)
	}
}

// SendAppendEntries sends an AppendEntries RPC to the target node.
func (t *ChannelTransport) SendAppendEntries(target raft.NodeID, req raft.AppendEntriesReq) (raft.AppendEntriesResp, error) {
	handler := t.getHandler(target)
	if handler == nil {
		return raft.AppendEntriesResp{}, fmt.Errorf("node %d unreachable", target)
	}

	type result struct {
		resp raft.AppendEntriesResp
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp := handler.HandleAppendEntries(req)
		ch <- result{resp: resp}
	}()

	select {
	case r := <-ch:
		return r.resp, r.err
	case <-time.After(t.timeout):
		return raft.AppendEntriesResp{}, fmt.Errorf("AppendEntries to node %d timed out", target)
	}
}

// SendInstallSnapshot sends a snapshot to the target node.
func (t *ChannelTransport) SendInstallSnapshot(target raft.NodeID, req raft.InstallSnapshotReq) raft.InstallSnapshotResp {
	handler := t.getHandler(target)
	if handler == nil {
		return raft.InstallSnapshotResp{}
	}

	type result struct {
		resp raft.InstallSnapshotResp
	}
	ch := make(chan result, 1)
	go func() {
		resp := handler.HandleInstallSnapshot(req)
		ch <- result{resp: resp}
	}()

	select {
	case r := <-ch:
		return r.resp
	case <-time.After(t.timeout):
		return raft.InstallSnapshotResp{}
	}
}
