package metadata

import "encoding/json"

// Command types for the Raft log.
const (
	OpRegisterFile = "REGISTER_FILE"
	OpDeleteFile   = "DELETE_FILE"
	OpAddReplica   = "ADD_REPLICA"
	OpRemoveReplica = "REMOVE_REPLICA"
)

// Command represents a metadata operation committed through Raft.
type Command struct {
	Op       string    `json:"op"`
	FileID   string    `json:"file_id,omitempty"`
	FileMeta *FileMeta `json:"file_meta,omitempty"`
	ChunkID  string    `json:"chunk_id,omitempty"`
	Node     string    `json:"node,omitempty"`
}

// Encode serializes a command to bytes for the Raft log.
// Panics on marshal failure (indicates a programming error — all fields are JSON-safe).
func (c Command) Encode() []byte {
	data, err := json.Marshal(c)
	if err != nil {
		panic("metadata.Command.Encode: " + err.Error())
	}
	return data
}

// DecodeCommand deserializes bytes from the Raft log into a Command.
func DecodeCommand(data []byte) (Command, error) {
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
