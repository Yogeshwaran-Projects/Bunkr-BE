package chunk

import (
	"bytes"
	"io"
)

// DefaultChunkSize is 4MB.
const DefaultChunkSize = 4 * 1024 * 1024

// Chunk represents a single piece of a file.
type Chunk struct {
	Index int
	Data  []byte
	Size  int64
}

// ChunkHandler is called for each chunk during streaming split.
// Return an error to abort chunking.
type ChunkHandler func(c Chunk) error

// Split reads from r and splits into chunks of chunkSize bytes.
// The last chunk may be smaller than chunkSize.
// All chunks are held in memory — use SplitStream for large files.
func Split(r io.Reader, chunkSize int64) ([]Chunk, error) {
	var chunks []Chunk
	err := SplitStream(r, chunkSize, func(c Chunk) error {
		chunks = append(chunks, c)
		return nil
	})
	return chunks, err
}

// SplitStream reads from r and calls handler for each chunk.
// Only one chunk is in memory at a time — safe for large files.
func SplitStream(r io.Reader, chunkSize int64, handler ChunkHandler) error {
	index := 0

	for {
		buf := make([]byte, chunkSize)
		n, err := io.ReadFull(r, buf)

		if n > 0 {
			if handlerErr := handler(Chunk{
				Index: index,
				Data:  buf[:n],
				Size:  int64(n),
			}); handlerErr != nil {
				return handlerErr
			}
			index++
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

// Reassemble writes chunks in order to w.
func Reassemble(w io.Writer, chunks []Chunk) (int64, error) {
	var total int64
	for _, c := range chunks {
		n, err := io.Copy(w, bytes.NewReader(c.Data))
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}
