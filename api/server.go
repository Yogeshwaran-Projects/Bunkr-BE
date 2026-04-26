package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/yogeshwaran/bunkr/backend/cluster"
	"github.com/yogeshwaran/bunkr/backend/raft"
	"github.com/yogeshwaran/bunkr/backend/replication"
)

// Server is the HTTP + WebSocket API server for Bunkr.
type Server struct {
	manager   *cluster.Manager
	replMgr   *replication.Manager
	wsClients map[*wsConn]bool
	wsMu      sync.Mutex
	port      int
}

// NewServer creates a new Bunkr API server.
func NewServer(manager *cluster.Manager, replMgr *replication.Manager, port int) *Server {
	return &Server{
		manager:   manager,
		replMgr:   replMgr,
		wsClients: make(map[*wsConn]bool),
		port:      port,
	}
}

// BuildHTTPServer creates the http.Server with all routes registered.
// Use this for graceful shutdown support.
func (s *Server) BuildHTTPServer() *http.Server {
	mux := http.NewServeMux()

	// File operations
	mux.HandleFunc("/api/files/upload", s.handleUpload)
	mux.HandleFunc("/api/files/download", s.handleDownload)
	mux.HandleFunc("/api/files/list", s.handleListFiles)
	mux.HandleFunc("/api/files/delete", s.handleDeleteFile)
	mux.HandleFunc("/api/files/info", s.handleFileInfo)

	// Cluster endpoints
	mux.HandleFunc("/api/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("/api/cluster/leader", s.handleLeader)

	// Node control
	mux.HandleFunc("/api/node/kill", s.handleKillNode)
	mux.HandleFunc("/api/node/revive", s.handleReviveNode)

	// WebSocket
	mux.HandleFunc("/ws/events", s.handleWebSocket)

	// Health
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		leader := s.manager.GetLeader()
		status := "healthy"
		if leader == 0 {
			status = "no_leader"
		}
		writeJSON(w, map[string]interface{}{
			"status":     status,
			"leader":     leader,
			"file_count": s.manager.FileCount(),
		})
	})

	// Broadcast events to WebSocket clients
	go s.broadcastEvents()

	return &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: corsMiddleware(mux),
	}
}

// Start starts the HTTP server and event broadcaster.
func (s *Server) Start() error {
	srv := s.BuildHTTPServer()
	log.Printf("Bunkr API server starting on %s", srv.Addr)
	return srv.ListenAndServe()
}

func (s *Server) broadcastEvents() {
	// Broadcast Raft events as they happen
	go func() {
		for event := range s.manager.Events() {
			data, err := json.Marshal(map[string]interface{}{
				"kind":    "event",
				"type":    event.Type,
				"node_id": event.NodeID,
				"term":    event.Term,
				"data":    event.Data,
				"time":    time.Now().UnixMilli(),
			})
			if err != nil {
				continue
			}
			s.broadcast(data)
		}
	}()

	// Broadcast full state snapshot every 2s (cluster + files + health)
	go func() {
		for {
			time.Sleep(2 * time.Second)

			leader := s.manager.GetLeader()
			status := "healthy"
			if leader == 0 {
				status = "no_leader"
			}

			store := s.manager.GetLeaderMetaStore()
			var files interface{}
			fileCount := 0
			if store != nil {
				fileList := store.ListFiles()
				files = fileList
				fileCount = len(fileList)
			}

			data, err := json.Marshal(map[string]interface{}{
				"kind":    "state",
				"cluster": s.manager.ClusterStatus(),
				"health": map[string]interface{}{
					"status":     status,
					"leader":     leader,
					"file_count": fileCount,
				},
				"files": files,
				"time":  time.Now().UnixMilli(),
			})
			if err != nil {
				continue
			}
			s.broadcast(data)
		}
	}()
}

func (s *Server) broadcast(data []byte) {
	s.wsMu.Lock()
	for client := range s.wsClients {
		client.send(data) //nolint:errcheck
	}
	s.wsMu.Unlock()
}

// --- Cluster Endpoints ---

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.manager.ClusterStatus())
}

func (s *Server) handleLeader(w http.ResponseWriter, r *http.Request) {
	leader := s.manager.GetLeader()
	writeJSON(w, map[string]interface{}{"leader_id": leader})
}

func (s *Server) handleKillNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	id, err := getNodeID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.manager.KillNode(raft.NodeID(id)); err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "killed": id})
}

func (s *Server) handleReviveNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	id, err := getNodeID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := s.manager.ReviveNode(raft.NodeID(id)); err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, map[string]interface{}{"ok": true, "revived": id})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func getNodeID(r *http.Request) (int, error) {
	idStr := r.URL.Query().Get("id")
	if idStr == "" {
		var body struct {
			ID int `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err == nil && body.ID > 0 {
			return body.ID, nil
		}
		return 0, fmt.Errorf("node id required")
	}
	return strconv.Atoi(idStr)
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
