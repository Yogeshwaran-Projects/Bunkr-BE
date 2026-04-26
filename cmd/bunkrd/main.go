package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yogeshwaran/bunkr/backend/api"
	"github.com/yogeshwaran/bunkr/backend/chunk"
	"github.com/yogeshwaran/bunkr/backend/cluster"
	"github.com/yogeshwaran/bunkr/backend/raft"
	"github.com/yogeshwaran/bunkr/backend/replication"
)

func main() {
	nodeCount := flag.Int("nodes", 5, "number of nodes in the cluster")
	httpPort := flag.Int("port", 8080, "HTTP API port")
	dataDir := flag.String("data", "./bunkr-data", "data directory for chunk storage")
	replFactor := flag.Int("replicas", 3, "replication factor for chunks")
	flag.Parse()

	fmt.Print(`
    ____              __
   / __ )__  ______  / /______
  / __  / / / / __ \/ //_/ ___/
 / /_/ / /_/ / / / / ,< / /
/_____/\__,_/_/ /_/_/|_/_/

  Distributed File System with Raft Consensus

`)

	log.Printf("Starting Bunkr cluster: %d nodes, replication factor %d", *nodeCount, *replFactor)
	log.Printf("Data directory: %s", *dataDir)
	log.Printf("API port: %d", *httpPort)

	// Create cluster manager
	mgr := cluster.NewManager(*nodeCount, *dataDir)

	// Create replication manager
	replMgr := replication.NewManager(
		*replFactor,
		func(id raft.NodeID) *chunk.Store {
			return mgr.GetNodeChunkStore(id)
		},
		func() []raft.NodeID {
			return mgr.GetAliveNodeIDs()
		},
	)

	// Start the cluster
	mgr.Start()
	log.Println("Cluster started, waiting for leader election...")

	// Wait for leader election
	for i := 0; i < 50; i++ {
		if mgr.GetLeader() != 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	leader := mgr.GetLeader()
	if leader == 0 {
		log.Println("Warning: no leader elected yet")
	} else {
		log.Printf("Leader elected: node-%d", leader)
	}

	// Start API server with graceful shutdown support
	srv := api.NewServer(mgr, replMgr, *httpPort)
	httpServer := srv.BuildHTTPServer()

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("API server error: %v", err)
		}
	}()

	log.Printf("Bunkr is ready! API at http://localhost:%d", *httpPort)
	log.Println("Endpoints:")
	log.Println("  POST   /api/files/upload    - Upload a file")
	log.Println("  GET    /api/files/download   - Download a file")
	log.Println("  GET    /api/files/list       - List all files")
	log.Println("  POST   /api/files/delete     - Delete a file")
	log.Println("  GET    /api/files/info       - File details")
	log.Println("  GET    /api/cluster/status   - Cluster status")
	log.Println("  GET    /api/health           - Health check")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down Bunkr...")

	// Graceful HTTP shutdown — drain in-flight requests (5s timeout)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("HTTP shutdown error: %v", err)
	}
	log.Println("HTTP server stopped")

	// Stop all Raft nodes
	for _, id := range mgr.AllIDs() {
		mgr.KillNode(id) //nolint:errcheck
	}
	log.Println("Raft cluster stopped")

	log.Println("Bunkr shut down cleanly")
}
