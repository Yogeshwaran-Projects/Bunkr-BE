package metadata

import "time"

// FileMeta holds all metadata for a stored file.
type FileMeta struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Size      int64       `json:"size"`
	ChunkSize int64       `json:"chunk_size"`
	Chunks    []ChunkMeta `json:"chunks"`
	Salt      []byte      `json:"salt,omitempty"` // random salt for HKDF key derivation
	CreatedAt time.Time   `json:"created_at"`
}

// ChunkMeta holds metadata for a single chunk of a file.
type ChunkMeta struct {
	ID    string   `json:"id"`    // SHA256 of encrypted chunk data
	Index int      `json:"index"` // position in file (0-based)
	Size  int64    `json:"size"`  // actual chunk size in bytes
	Nodes []string `json:"nodes"` // node addresses holding this chunk
}

// Result is the response from applying a metadata command.
type Result struct {
	Ok    bool      `json:"ok"`
	Error string    `json:"error,omitempty"`
	File  *FileMeta `json:"file,omitempty"`
	Files []FileMeta `json:"files,omitempty"`
}
