package transport

import "github.com/yogeshwaran/bunkr/backend/raft"

// Transport defines how Raft nodes communicate with each other.
// Two implementations: ChannelTransport (in-process) and GRPCTransport (network).
type Transport interface {
	SendRequestVote(target raft.NodeID, req raft.RequestVoteReq) (raft.RequestVoteResp, error)
	SendAppendEntries(target raft.NodeID, req raft.AppendEntriesReq) (raft.AppendEntriesResp, error)
	SendInstallSnapshot(target raft.NodeID, req raft.InstallSnapshotReq) raft.InstallSnapshotResp
}
