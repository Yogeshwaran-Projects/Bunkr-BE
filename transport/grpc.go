package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/yogeshwaran/bunkr/backend/proto/raftpb"
	"github.com/yogeshwaran/bunkr/backend/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// GRPCTransport implements Transport using gRPC over the network.
// Each node runs as a separate process, communicating via TCP.
type GRPCTransport struct {
	mu      sync.RWMutex
	peers   map[raft.NodeID]string            // nodeID → "host:port"
	conns   map[raft.NodeID]*grpc.ClientConn  // cached connections
	handler RPCHandler                        // local node's handler
	server  *grpc.Server
	timeout time.Duration
}

// NewGRPCTransport creates a gRPC-based transport.
// peers maps each node ID to its "host:port" address.
func NewGRPCTransport(peers map[raft.NodeID]string, timeout time.Duration) *GRPCTransport {
	if timeout == 0 {
		timeout = 500 * time.Millisecond
	}
	return &GRPCTransport{
		peers:   peers,
		conns:   make(map[raft.NodeID]*grpc.ClientConn),
		timeout: timeout,
	}
}

// Serve starts the gRPC server on the given address.
func (t *GRPCTransport) Serve(addr string, handler RPCHandler) error {
	t.handler = handler

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	t.server = grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)
	raftpb.RegisterRaftServiceServer(t.server, &grpcServer{handler: handler})

	go t.server.Serve(lis)
	return nil
}

// Stop shuts down the gRPC server and closes all connections.
func (t *GRPCTransport) Stop() {
	if t.server != nil {
		t.server.GracefulStop()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, conn := range t.conns {
		conn.Close()
	}
	t.conns = make(map[raft.NodeID]*grpc.ClientConn)
}

// getConn returns a cached or new connection to the target node.
func (t *GRPCTransport) getConn(target raft.NodeID) (*grpc.ClientConn, error) {
	t.mu.RLock()
	conn, ok := t.conns[target]
	t.mu.RUnlock()
	if ok {
		return conn, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := t.conns[target]; ok {
		return conn, nil
	}

	addr, ok := t.peers[target]
	if !ok {
		return nil, fmt.Errorf("unknown peer %d", target)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: 3 * time.Second,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	t.conns[target] = conn
	return conn, nil
}

// --- Transport Interface Implementation ---

func (t *GRPCTransport) SendRequestVote(target raft.NodeID, req raft.RequestVoteReq) (raft.RequestVoteResp, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return raft.RequestVoteResp{}, err
	}

	client := raftpb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	resp, err := client.RequestVote(ctx, &raftpb.RequestVoteRequest{
		Term:         req.Term,
		CandidateId:  uint32(req.CandidateID),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		IsPreVote:    req.IsPreVote,
	})
	if err != nil {
		return raft.RequestVoteResp{}, err
	}

	return raft.RequestVoteResp{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (t *GRPCTransport) SendAppendEntries(target raft.NodeID, req raft.AppendEntriesReq) (raft.AppendEntriesResp, error) {
	conn, err := t.getConn(target)
	if err != nil {
		return raft.AppendEntriesResp{}, err
	}

	client := raftpb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), t.timeout)
	defer cancel()

	pbEntries := make([]*raftpb.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		pbEntries[i] = &raftpb.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Command,
		}
	}

	resp, err := client.AppendEntries(ctx, &raftpb.AppendEntriesRequest{
		Term:         req.Term,
		LeaderId:     uint32(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      pbEntries,
		LeaderCommit: req.LeaderCommit,
	})
	if err != nil {
		return raft.AppendEntriesResp{}, err
	}

	return raft.AppendEntriesResp{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

func (t *GRPCTransport) SendInstallSnapshot(target raft.NodeID, req raft.InstallSnapshotReq) raft.InstallSnapshotResp {
	conn, err := t.getConn(target)
	if err != nil {
		return raft.InstallSnapshotResp{}
	}

	client := raftpb.NewRaftServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // snapshots can be large
	defer cancel()

	resp, err := client.InstallSnapshot(ctx, &raftpb.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderId:          uint32(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	if err != nil {
		return raft.InstallSnapshotResp{}
	}

	return raft.InstallSnapshotResp{Term: resp.Term}
}

// --- gRPC Server Implementation ---

type grpcServer struct {
	raftpb.UnimplementedRaftServiceServer
	handler RPCHandler
}

func (s *grpcServer) RequestVote(ctx context.Context, req *raftpb.RequestVoteRequest) (*raftpb.RequestVoteResponse, error) {
	resp := s.handler.HandleRequestVote(raft.RequestVoteReq{
		Term:         req.Term,
		CandidateID:  raft.NodeID(req.CandidateId),
		LastLogIndex: req.LastLogIndex,
		LastLogTerm:  req.LastLogTerm,
		IsPreVote:    req.IsPreVote,
	})
	return &raftpb.RequestVoteResponse{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (s *grpcServer) AppendEntries(ctx context.Context, req *raftpb.AppendEntriesRequest) (*raftpb.AppendEntriesResponse, error) {
	entries := make([]raft.LogEntry, len(req.Entries))
	for i, e := range req.Entries {
		entries[i] = raft.LogEntry{
			Index:   e.Index,
			Term:    e.Term,
			Command: e.Command,
		}
	}

	resp := s.handler.HandleAppendEntries(raft.AppendEntriesReq{
		Term:         req.Term,
		LeaderID:     raft.NodeID(req.LeaderId),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      entries,
		LeaderCommit: req.LeaderCommit,
	})
	return &raftpb.AppendEntriesResponse{
		Term:          resp.Term,
		Success:       resp.Success,
		ConflictTerm:  resp.ConflictTerm,
		ConflictIndex: resp.ConflictIndex,
	}, nil
}

func (s *grpcServer) InstallSnapshot(ctx context.Context, req *raftpb.InstallSnapshotRequest) (*raftpb.InstallSnapshotResponse, error) {
	resp := s.handler.HandleInstallSnapshot(raft.InstallSnapshotReq{
		Term:              req.Term,
		LeaderID:          raft.NodeID(req.LeaderId),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Data:              req.Data,
	})
	return &raftpb.InstallSnapshotResponse{
		Term: resp.Term,
	}, nil
}
