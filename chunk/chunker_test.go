package chunk

import (
	"bytes"
	"testing"
)

func TestSplit_SmallFile(t *testing.T) {
	data := []byte("hello bunkr")
	chunks, err := Split(bytes.NewReader(data), DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if !bytes.Equal(chunks[0].Data, data) {
		t.Fatal("chunk data doesn't match")
	}
}

func TestSplit_ExactChunkSize(t *testing.T) {
	chunkSize := int64(1024)
	data := make([]byte, chunkSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks, err := Split(bytes.NewReader(data), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
}

func TestSplit_MultipleChunks(t *testing.T) {
	chunkSize := int64(100)
	data := make([]byte, 350)
	for i := range data {
		data[i] = byte(i % 256)
	}

	chunks, err := Split(bytes.NewReader(data), chunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 4 {
		t.Fatalf("expected 4 chunks (100+100+100+50), got %d", len(chunks))
	}

	// Last chunk should be 50 bytes
	if chunks[3].Size != 50 {
		t.Fatalf("expected last chunk 50 bytes, got %d", chunks[3].Size)
	}

	// Indices should be sequential
	for i, c := range chunks {
		if c.Index != i {
			t.Errorf("chunk %d has index %d", i, c.Index)
		}
	}
}

func TestSplit_EmptyFile(t *testing.T) {
	chunks, err := Split(bytes.NewReader(nil), DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatalf("expected 0 chunks for empty file, got %d", len(chunks))
	}
}

func TestReassemble(t *testing.T) {
	original := make([]byte, 350)
	for i := range original {
		original[i] = byte(i % 256)
	}

	chunks, err := Split(bytes.NewReader(original), 100)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	n, err := Reassemble(&buf, chunks)
	if err != nil {
		t.Fatal(err)
	}

	if n != 350 {
		t.Fatalf("expected 350 bytes reassembled, got %d", n)
	}

	if !bytes.Equal(buf.Bytes(), original) {
		t.Fatal("reassembled data doesn't match original")
	}
}

func TestSplitReassemble_LargeFile(t *testing.T) {
	// 10MB file with 4MB chunks = 3 chunks
	size := 10 * 1024 * 1024
	original := make([]byte, size)
	for i := range original {
		original[i] = byte(i % 251) // prime to avoid patterns
	}

	chunks, err := Split(bytes.NewReader(original), DefaultChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks for 10MB/4MB, got %d", len(chunks))
	}

	var buf bytes.Buffer
	_, err = Reassemble(&buf, chunks)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(buf.Bytes(), original) {
		t.Fatal("10MB round-trip failed")
	}
}
