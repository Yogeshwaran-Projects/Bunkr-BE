package api

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strconv"

	"github.com/yogeshwaran/bunkr/backend/crypto"
	"github.com/yogeshwaran/bunkr/backend/metadata"
	"github.com/yogeshwaran/bunkr/backend/raft"
)

func (s *Server) handleDownload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST only", http.StatusMethodNotAllowed)
		return
	}

	fileID := r.URL.Query().Get("id")
	fileName := r.URL.Query().Get("name")

	// Read encryption key from header (preferred) or query param (fallback)
	encKeyB64 := r.Header.Get("X-Encryption-Key")
	if encKeyB64 == "" {
		encKeyB64 = r.URL.Query().Get("key")
	}

	if encKeyB64 == "" {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "encryption key required (X-Encryption-Key header)"})
		return
	}

	encKey, err := base64.StdEncoding.DecodeString(encKeyB64)
	if err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "invalid encryption key"})
		return
	}

	store := s.manager.GetLeaderMetaStore()
	if store == nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "no leader"})
		return
	}

	var file *metadata.FileMeta
	var ok bool

	if fileID != "" {
		file, ok = store.GetFile(fileID)
	} else if fileName != "" {
		file, ok = store.GetFileByName(fileName)
	} else {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "id or name required"})
		return
	}

	if !ok {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "file not found"})
		return
	}

	// Sort chunks by index
	sortedChunks := make([]metadata.ChunkMeta, len(file.Chunks))
	copy(sortedChunks, file.Chunks)
	sort.Slice(sortedChunks, func(i, j int) bool {
		return sortedChunks[i].Index < sortedChunks[j].Index
	})

	// Set headers for file download
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", file.Name))
	w.Header().Set("Content-Length", strconv.FormatInt(file.Size, 10))

	// Fetch, decrypt, and stream each chunk
	for _, c := range sortedChunks {
		// Convert node strings to IDs
		var nodeIDs []raft.NodeID
		for _, n := range c.Nodes {
			nodeIDs = append(nodeIDs, nodeIDFromString(n))
		}

		// Fetch from any available node
		encrypted, err := s.replMgr.FetchChunk(c.ID, nodeIDs)
		if err != nil {
			log.Printf("Failed to fetch chunk %s: %v", c.ID[:8], err)
			return
		}

		// Verify chunk integrity — hash must match chunk ID
		if actualHash := crypto.HashData(encrypted); actualHash != c.ID {
			log.Printf("Chunk integrity check failed: expected %s, got %s", c.ID[:8], actualHash[:8])
			return
		}

		// Derive per-chunk key with salt and decrypt
		chunkKey, err := crypto.DeriveChunkKey(encKey, file.Salt, c.Index)
		if err != nil {
			log.Printf("Key derivation failed for chunk %d: %v", c.Index, err)
			return
		}

		plaintext, err := crypto.Decrypt(chunkKey, encrypted)
		if err != nil {
			http.Error(w, "decryption failed (wrong key?)", http.StatusBadRequest)
			return
		}

		if _, err := w.Write(plaintext); err != nil {
			return // client disconnected
		}
	}

	log.Printf("File %s downloaded (%d chunks)", file.Name, len(sortedChunks))
}
