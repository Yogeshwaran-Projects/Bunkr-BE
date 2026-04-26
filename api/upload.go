package api

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/yogeshwaran/bunkr/backend/chunk"
	"github.com/yogeshwaran/bunkr/backend/crypto"
	"github.com/yogeshwaran/bunkr/backend/metadata"
	"github.com/yogeshwaran/bunkr/backend/raft"
)

// UploadResponse is returned after a successful file upload.
type UploadResponse struct {
	Ok            bool   `json:"ok"`
	FileID        string `json:"file_id"`
	FileName      string `json:"file_name"`
	Size          int64  `json:"size"`
	Chunks        int    `json:"chunks"`
	Replicas      int    `json:"replicas"`
	EncryptionKey string `json:"encryption_key"` // base64-encoded, returned to client
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form (max 512MB)
	if err := r.ParseMultipartForm(512 << 20); err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "failed to parse form: " + err.Error()})
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "file required"})
		return
	}
	defer file.Close()

	fileName := header.Filename

	// Generate encryption key + salt
	encKey, err := crypto.GenerateKey()
	if err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "key generation failed"})
		return
	}
	salt, err := crypto.GenerateSalt()
	if err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "salt generation failed"})
		return
	}

	fileID := crypto.GenerateID()

	// Stream through chunker — only 1 chunk in memory at a time
	var chunkMetas []metadata.ChunkMeta
	var totalSize int64

	err = chunk.SplitStream(file, chunk.DefaultChunkSize, func(c chunk.Chunk) error {
		totalSize += c.Size

		// Derive per-chunk key with salt
		chunkKey, err := crypto.DeriveChunkKey(encKey, salt, c.Index)
		if err != nil {
			return fmt.Errorf("key derivation failed for chunk %d: %w", c.Index, err)
		}

		// Encrypt
		encrypted, err := crypto.Encrypt(chunkKey, c.Data)
		if err != nil {
			return fmt.Errorf("encryption failed for chunk %d: %w", c.Index, err)
		}

		// Hash encrypted data → chunk ID
		chunkID := crypto.HashData(encrypted)

		// Place and replicate chunk to nodes
		targets, err := s.replMgr.PlaceAndReplicate(c.Index, chunkID, encrypted)
		if err != nil {
			return fmt.Errorf("replication failed for chunk %d: %w", c.Index, err)
		}

		// Convert node IDs to strings for metadata
		nodeStrs := make([]string, len(targets))
		for j, t := range targets {
			nodeStrs[j] = fmt.Sprintf("node-%d", t)
		}

		chunkMetas = append(chunkMetas, metadata.ChunkMeta{
			ID:    chunkID,
			Index: c.Index,
			Size:  int64(len(encrypted)),
			Nodes: nodeStrs,
		})

		return nil
	})
	if err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}

	log.Printf("Uploading %s (%d bytes, %d chunks)", fileName, totalSize, len(chunkMetas))

	// Build file metadata and commit via Raft
	fileMeta := &metadata.FileMeta{
		ID:        fileID,
		Name:      fileName,
		Size:      totalSize,
		ChunkSize: chunk.DefaultChunkSize,
		Chunks:    chunkMetas,
		Salt:      salt,
		CreatedAt: time.Now(),
	}

	cmd := metadata.Command{
		Op:       metadata.OpRegisterFile,
		FileMeta: fileMeta,
	}

	_, err = s.manager.Submit(cmd.Encode())
	if err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "metadata commit failed: " + err.Error()})
		return
	}

	log.Printf("File %s uploaded: %d chunks, %d bytes", fileName, len(chunkMetas), totalSize)

	replicas := 0
	if len(chunkMetas) > 0 {
		replicas = len(chunkMetas[0].Nodes)
	}

	writeJSON(w, UploadResponse{
		Ok:            true,
		FileID:        fileID,
		FileName:      fileName,
		Size:          totalSize,
		Chunks:        len(chunkMetas),
		Replicas:      replicas,
		EncryptionKey: base64.StdEncoding.EncodeToString(encKey),
	})
}

// nodeIDFromString extracts node ID number from "node-N" format.
func nodeIDFromString(s string) raft.NodeID {
	var id int
	fmt.Sscanf(s, "node-%d", &id)
	return raft.NodeID(id)
}

// handleListFiles returns all stored files.
func (s *Server) handleListFiles(w http.ResponseWriter, r *http.Request) {
	store := s.manager.GetLeaderMetaStore()
	if store == nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "no leader"})
		return
	}

	files := store.ListFiles()
	writeJSON(w, map[string]interface{}{
		"ok":    true,
		"files": files,
		"count": len(files),
	})
}

// handleFileInfo returns details about a specific file.
func (s *Server) handleFileInfo(w http.ResponseWriter, r *http.Request) {
	fileID := r.URL.Query().Get("id")
	fileName := r.URL.Query().Get("name")

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

	writeJSON(w, map[string]interface{}{"ok": true, "file": file})
}

// handleDeleteFile removes a file and its chunks.
func (s *Server) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodDelete {
		http.Error(w, "POST or DELETE only", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}

	// Try query params first, then body
	body.ID = r.URL.Query().Get("id")
	body.Name = r.URL.Query().Get("name")

	if body.ID == "" && body.Name == "" {
		json.NewDecoder(r.Body).Decode(&body) //nolint:errcheck
	}

	store := s.manager.GetLeaderMetaStore()
	if store == nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "no leader"})
		return
	}

	var file *metadata.FileMeta
	var ok bool

	if body.ID != "" {
		file, ok = store.GetFile(body.ID)
	} else if body.Name != "" {
		file, ok = store.GetFileByName(body.Name)
	} else {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "id or name required"})
		return
	}

	if !ok {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "file not found"})
		return
	}

	// Commit deletion via Raft FIRST — only delete chunks after metadata is committed
	cmd := metadata.Command{Op: metadata.OpDeleteFile, FileID: file.ID}
	_, err := s.manager.Submit(cmd.Encode())
	if err != nil {
		writeJSON(w, map[string]interface{}{"ok": false, "error": "delete failed: " + err.Error()})
		return
	}

	// Now safe to delete chunks — metadata is committed, file is gone from index
	for _, c := range file.Chunks {
		var nodeIDs []raft.NodeID
		for _, n := range c.Nodes {
			nodeIDs = append(nodeIDs, nodeIDFromString(n))
		}
		s.replMgr.DeleteChunk(c.ID, nodeIDs)
	}

	log.Printf("File %s deleted", file.Name)
	writeJSON(w, map[string]interface{}{"ok": true, "deleted": file.Name})
}
