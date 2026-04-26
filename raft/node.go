package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// Transport is the interface for node-to-node communication.
type Transport interface {
	SendRequestVote(target NodeID, req RequestVoteReq) (RequestVoteResp, error)
	SendAppendEntries(target NodeID, req AppendEntriesReq) (AppendEntriesResp, error)
	SendInstallSnapshot(target NodeID, req InstallSnapshotReq) InstallSnapshotResp
}

// Node is a single Raft node implementing the full Raft consensus algorithm.
type Node struct {
	mu sync.RWMutex

	// --- Persistent state (survives crash via WAL) ---
	id          NodeID
	currentTerm uint64
	votedFor    NodeID // 0 = nobody
	raftLog     *RaftLog

	// --- Volatile state on all servers ---
	state       NodeState
	commitIndex uint64
	lastApplied uint64
	leaderID    NodeID // known leader (0 = unknown)

	// --- Volatile state on leaders ---
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// --- Pre-Vote state ---
	// preVoteGranted tracks if this node has been granted a pre-vote.
	// Pre-vote doesn't increment term, preventing term inflation.

	// --- CheckQuorum state ---
	// Tracks which followers responded since last check.
	leaderLastContact time.Time

	// --- ReadIndex state ---
	readIndexQueue []readIndexRequest

	// --- Commit notification ---
	commitWaiters map[uint64][]chan struct{}

	// --- Snapshots ---
	snapshotStore *SnapshotStore
	snapshotFn    SnapshotFunc
	restoreFn     RestoreFunc

	// --- Client session deduplication ---
	sessions *SessionManager

	// --- Configuration ---
	config Config
	peers  []NodeID

	// --- Infrastructure ---
	transport     Transport
	wal           *WAL
	electionTimer *time.Timer
	shutdownCh    chan struct{}
	stopped       sync.Once
	eventCh       chan Event
	applyCh       chan ApplyMsg
}

type readIndexRequest struct {
	index    uint64
	resultCh chan uint64
}

// NewNode creates a new Raft node with the given config.
func NewNode(id NodeID, peers []NodeID, transport Transport, opts ...Option) *Node {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	n := &Node{
		id:            id,
		currentTerm:   0,
		votedFor:      0,
		raftLog:       NewRaftLog(),
		state:         Follower,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make(map[NodeID]uint64),
		matchIndex:    make(map[NodeID]uint64),
		commitWaiters: make(map[uint64][]chan struct{}),
		snapshotStore: NewSnapshotStore(),
		sessions:      NewSessionManager(),
		config:        cfg,
		peers:         peers,
		transport:     transport,
		shutdownCh:    make(chan struct{}),
		eventCh:       make(chan Event, 512),
		applyCh:       make(chan ApplyMsg, 512),
	}

	return n
}

// Option configures a Node.
type Option func(*Config)

// WithConfig sets the full config.
func WithConfig(cfg Config) Option {
	return func(c *Config) { *c = cfg }
}

// WithWAL sets the WAL for persistence.
func WithWAL(wal *WAL) Option {
	return func(c *Config) {
		// WAL is set separately since it's not part of Config
	}
}

// SetWAL attaches a write-ahead log for persistence.
func (n *Node) SetWAL(wal *WAL) {
	n.wal = wal
}

// Start begins the Raft node's main loop.
func (n *Node) Start() error {
	if err := n.config.Validate(); err != nil {
		return err
	}

	n.mu.Lock()
	n.restoreFromWAL()
	n.state = Follower
	n.leaderLastContact = time.Now()
	n.electionTimer = time.NewTimer(n.randomElectionTimeout())
	n.mu.Unlock()

	n.emit("node_started", nil)
	go n.run()
	return nil
}

// Stop gracefully shuts down the node.
func (n *Node) Stop() {
	n.stopped.Do(func() {
		close(n.shutdownCh)
		n.mu.Lock()
		for _, chs := range n.commitWaiters {
			for _, ch := range chs {
				close(ch)
			}
		}
		n.commitWaiters = make(map[uint64][]chan struct{})
		if n.electionTimer != nil {
			n.electionTimer.Stop()
		}
		if n.wal != nil {
			n.wal.Close()
		}
		n.mu.Unlock()
	})
}

// IsShutdown returns true if the node has been stopped.
func (n *Node) IsShutdown() bool {
	select {
	case <-n.shutdownCh:
		return true
	default:
		return false
	}
}

// --- Public Accessors ---

func (n *Node) Events() <-chan Event   { return n.eventCh }
func (n *Node) ApplyCh() <-chan ApplyMsg { return n.applyCh }
func (n *Node) ID() NodeID            { return n.id }

func (n *Node) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

func (n *Node) Term() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

func (n *Node) LeaderID() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

func (n *Node) LogLen() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raftLog.Len()
}

func (n *Node) CommitIndex() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.commitIndex
}

// --- WAL Persistence ---

// persistState saves term and vote to WAL. Must be called with mu held.
func (n *Node) persistState() {
	if n.wal == nil {
		return
	}
	if err := n.wal.SaveState(n.currentTerm, n.votedFor); err != nil {
		log.Printf("[Node %d] WAL SaveState error: %v", n.id, err)
	}
}

// persistEntries saves log entries to WAL. Must be called with mu held.
func (n *Node) persistEntries(entries []LogEntry) {
	if n.wal == nil || len(entries) == 0 {
		return
	}
	if err := n.wal.SaveEntries(entries); err != nil {
		log.Printf("[Node %d] WAL SaveEntries error: %v", n.id, err)
	}
}

// restoreFromWAL loads state from WAL on startup. Must be called with mu held.
func (n *Node) restoreFromWAL() {
	if n.wal == nil {
		return
	}
	term, votedFor, entries, err := n.wal.ReadAll()
	if err != nil {
		log.Printf("[Node %d] WAL ReadAll error: %v", n.id, err)
		return
	}
	n.currentTerm = term
	n.votedFor = votedFor
	if len(entries) > 0 {
		n.raftLog.entries = entries
	}
}

// --- Main Event Loop ---

func (n *Node) run() {
	checkQuorumTicker := time.NewTicker(n.config.ElectionTimeoutMin)
	defer checkQuorumTicker.Stop()

	for {
		select {
		case <-n.shutdownCh:
			n.emit("node_stopped", nil)
			return

		case <-n.electionTimer.C:
			n.mu.RLock()
			state := n.state
			n.mu.RUnlock()

			if state != Leader {
				if n.config.PreVote {
					n.startPreVote()
				} else {
					n.startElection()
				}
			}

		case <-checkQuorumTicker.C:
			if n.config.CheckQuorum {
				n.checkQuorum()
			}
		}
	}
}

// --- Pre-Vote (prevents term inflation from partitioned nodes) ---

func (n *Node) startPreVote() {
	n.mu.RLock()
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	n.mu.RUnlock()

	// Pre-vote uses the NEXT term (term+1) but doesn't actually increment
	preVoteTerm := term + 1
	votes := 1
	total := len(n.peers)
	majority := (total+1)/2 + 1

	type voteResult struct {
		resp RequestVoteResp
		err  error
	}
	voteCh := make(chan voteResult, total)

	for _, peer := range n.peers {
		go func(p NodeID) {
			resp, err := n.transport.SendRequestVote(p, RequestVoteReq{
				Term:         preVoteTerm,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
				IsPreVote:    true,
			})
			voteCh <- voteResult{resp: resp, err: err}
		}(peer)
	}

	for i := 0; i < total; i++ {
		result := <-voteCh
		if result.err != nil {
			continue
		}

		n.mu.RLock()
		if n.state != Follower || n.currentTerm != term {
			n.mu.RUnlock()
			return
		}
		n.mu.RUnlock()

		if result.resp.VoteGranted {
			votes++
			if votes >= majority {
				// Pre-vote passed — now run real election
				n.startElection()
				return
			}
		}
	}
	// Pre-vote failed — don't start election, don't increment term
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
}

// --- Election ---

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = 0
	n.persistState()
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	n.resetElectionTimer()
	n.mu.Unlock()

	n.emit("election_started", map[string]interface{}{"term": term})

	votes := 1
	total := len(n.peers)
	majority := (total+1)/2 + 1

	type voteResult struct {
		resp RequestVoteResp
		err  error
	}
	voteCh := make(chan voteResult, total)

	for _, peer := range n.peers {
		go func(p NodeID) {
			resp, err := n.transport.SendRequestVote(p, RequestVoteReq{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			voteCh <- voteResult{resp: resp, err: err}
		}(peer)
	}

	for i := 0; i < total; i++ {
		result := <-voteCh

		n.mu.Lock()
		if n.state != Candidate || n.currentTerm != term {
			n.mu.Unlock()
			return
		}

		if result.err != nil {
			n.mu.Unlock()
			continue
		}

		if result.resp.Term > n.currentTerm {
			n.becomeFollower(result.resp.Term)
			n.mu.Unlock()
			return
		}

		if result.resp.VoteGranted {
			votes++
			if votes >= majority {
				n.becomeLeader()
				n.mu.Unlock()
				return
			}
		}
		n.mu.Unlock()
	}
}

// --- State Transitions ---

// becomeFollower transitions to Follower. Must hold mu.
func (n *Node) becomeFollower(term uint64) {
	oldState := n.state
	changed := n.currentTerm != term || n.state != Follower
	n.state = Follower
	n.currentTerm = term
	n.votedFor = 0
	if changed {
		n.persistState()
	}
	n.resetElectionTimer()

	if oldState != Follower {
		n.emitLocked("state_change", map[string]interface{}{
			"from": oldState.String(),
			"to":   Follower.String(),
			"term": term,
		})
	}
}

// becomeLeader transitions to Leader. Must hold mu.
func (n *Node) becomeLeader() {
	n.state = Leader
	n.leaderID = n.id
	n.leaderLastContact = time.Now()
	n.electionTimer.Stop()

	// Initialize volatile leader state
	lastIdx := n.raftLog.LastIndex()
	for _, peer := range n.peers {
		n.nextIndex[peer] = lastIdx + 1
		n.matchIndex[peer] = 0
	}

	n.emitLocked("leader_elected", map[string]interface{}{"term": n.currentTerm})

	// Append no-op entry. This is critical: it allows the leader to commit
	// entries from previous terms by committing something in its own term.
	// Without this, the leader can't determine which previous-term entries
	// are committed (Raft paper Section 5.4.2).
	noop := LogEntry{
		Index:   n.raftLog.LastIndex() + 1,
		Term:    n.currentTerm,
		Command: nil,
	}
	n.raftLog.Append(noop)
	n.persistEntries([]LogEntry{noop})

	go n.heartbeatLoop()
}

// --- CheckQuorum ---
// Leader steps down if it hasn't heard from a majority recently.
// Prevents a partitioned leader from serving stale data.

func (n *Node) checkQuorum() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		return
	}

	elapsed := time.Since(n.leaderLastContact)
	if elapsed > n.config.ElectionTimeoutMin {
		// Haven't confirmed majority recently — step down
		n.emitLocked("check_quorum_failed", map[string]interface{}{
			"elapsed_ms": elapsed.Milliseconds(),
		})
		n.becomeFollower(n.currentTerm)
	}
}

// --- Heartbeat Loop ---

func (n *Node) heartbeatLoop() {
	ticker := time.NewTicker(n.config.HeartbeatTimeout)
	defer ticker.Stop()

	n.sendHeartbeats()

	for {
		select {
		case <-n.shutdownCh:
			return
		case <-ticker.C:
			n.mu.RLock()
			isLeader := n.state == Leader
			n.mu.RUnlock()
			if !isLeader {
				return
			}
			n.sendHeartbeats()
		}
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return
	}
	peers := n.peers
	n.mu.RUnlock()

	responded := make(chan bool, len(peers))

	for _, peer := range peers {
		go func(p NodeID) {
			n.replicateTo(p)
			responded <- true
		}(peer)
	}

	// Track responses for CheckQuorum
	go func() {
		count := 0
		for i := 0; i < len(peers); i++ {
			<-responded
			count++
		}
		majority := len(peers)/2 + 1
		if count >= majority {
			n.mu.Lock()
			n.leaderLastContact = time.Now()
			n.mu.Unlock()
		}
	}()
}

// --- RPC Handlers ---

// HandleRequestVote processes an incoming RequestVote RPC.
func (n *Node) HandleRequestVote(req RequestVoteReq) RequestVoteResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.IsShutdown() {
		return RequestVoteResp{Term: n.currentTerm, VoteGranted: false}
	}

	// For pre-vote: don't step down, just check if we would vote
	if req.IsPreVote {
		canGrant := req.Term > n.currentTerm ||
			(req.Term == n.currentTerm+1 && (n.votedFor == 0 || n.votedFor == req.CandidateID))

		lastIdx, lastTerm := n.lastLogInfo()
		logOk := req.LastLogTerm > lastTerm ||
			(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)

		// Don't grant pre-vote if we recently heard from a leader
		// (prevents unnecessary elections when leader is alive)
		if n.state == Follower && time.Since(n.leaderLastContact) < n.config.ElectionTimeoutMin {
			return RequestVoteResp{Term: n.currentTerm, VoteGranted: false}
		}

		return RequestVoteResp{Term: n.currentTerm, VoteGranted: canGrant && logOk}
	}

	// Stale term → reject
	if req.Term < n.currentTerm {
		return RequestVoteResp{Term: n.currentTerm, VoteGranted: false}
	}

	// Higher term → step down
	if req.Term > n.currentTerm {
		n.becomeFollower(req.Term)
	}

	// Already voted for someone else?
	if n.votedFor != 0 && n.votedFor != req.CandidateID {
		return RequestVoteResp{Term: n.currentTerm, VoteGranted: false}
	}

	// Election restriction: candidate's log must be at least as up-to-date
	lastIdx, lastTerm := n.lastLogInfo()
	logOk := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if !logOk {
		return RequestVoteResp{Term: n.currentTerm, VoteGranted: false}
	}

	// Grant vote
	n.votedFor = req.CandidateID
	n.persistState()
	n.resetElectionTimer()

	n.emitLocked("vote_granted", map[string]interface{}{
		"to":   req.CandidateID,
		"term": n.currentTerm,
	})

	return RequestVoteResp{Term: n.currentTerm, VoteGranted: true}
}

// HandleAppendEntries processes an incoming AppendEntries RPC.
func (n *Node) HandleAppendEntries(req AppendEntriesReq) AppendEntriesResp {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.IsShutdown() {
		return AppendEntriesResp{Term: n.currentTerm, Success: false}
	}

	// Stale leader → reject
	if req.Term < n.currentTerm {
		return AppendEntriesResp{Term: n.currentTerm, Success: false}
	}

	// Valid leader
	n.leaderID = req.LeaderID
	n.leaderLastContact = time.Now()
	n.becomeFollower(req.Term)

	// Log consistency check (Raft paper Figure 2)
	if !n.raftLog.Match(req.PrevLogIndex, req.PrevLogTerm) {
		// Accelerated backtracking: return conflicting term info
		// so leader can skip an entire term instead of one entry at a time.
		resp := AppendEntriesResp{
			Term:    n.currentTerm,
			Success: false,
		}

		if req.PrevLogIndex > n.raftLog.LastIndex() {
			// Follower's log is shorter
			resp.ConflictIndex = n.raftLog.LastIndex() + 1
			resp.ConflictTerm = 0
		} else {
			// Conflicting entry exists
			conflictTerm := n.raftLog.TermAt(req.PrevLogIndex)
			resp.ConflictTerm = conflictTerm
			// Find first index of conflicting term
			resp.ConflictIndex = req.PrevLogIndex
			for resp.ConflictIndex > 1 && n.raftLog.TermAt(resp.ConflictIndex-1) == conflictTerm {
				resp.ConflictIndex--
			}
		}

		return resp
	}

	// Append entries (handles conflicts: truncate + overwrite)
	if len(req.Entries) > 0 {
		n.raftLog.AppendEntries(req.PrevLogIndex, req.Entries)
		n.persistEntries(req.Entries)
	}

	// Advance commit index
	if req.LeaderCommit > n.commitIndex {
		lastNewIdx := n.raftLog.LastIndex()
		if req.LeaderCommit < lastNewIdx {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewIdx
		}
		n.applyCommitted()
	}

	return AppendEntriesResp{Term: n.currentTerm, Success: true}
}

// --- Helpers ---

func (n *Node) lastLogInfo() (uint64, uint64) {
	return n.raftLog.LastIndex(), n.raftLog.LastTerm()
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer == nil {
		n.electionTimer = time.NewTimer(n.randomElectionTimeout())
		return
	}
	n.electionTimer.Stop()
	select {
	case <-n.electionTimer.C:
	default:
	}
	n.electionTimer.Reset(n.randomElectionTimeout())
}

func (n *Node) randomElectionTimeout() time.Duration {
	min := n.config.ElectionTimeoutMin
	max := n.config.ElectionTimeoutMax
	return min + time.Duration(rand.Int63n(int64(max-min)))
}

// --- Events ---

func (n *Node) emit(eventType string, data map[string]interface{}) {
	n.mu.RLock()
	term := n.currentTerm
	n.mu.RUnlock()
	select {
	case n.eventCh <- Event{Type: eventType, NodeID: n.id, Term: term, Data: data}:
	default:
	}
}

func (n *Node) emitLocked(eventType string, data map[string]interface{}) {
	select {
	case n.eventCh <- Event{Type: eventType, NodeID: n.id, Term: n.currentTerm, Data: data}:
	default:
	}
}

func init() {
	log.SetFlags(log.Ltime | log.Lmicroseconds)
}
