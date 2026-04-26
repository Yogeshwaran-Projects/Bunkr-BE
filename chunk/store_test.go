package chunk

import (
	"bytes"
	"os"
	"testing"
)

func tempStore(t *testing.T) *Store {
	t.Helper()
	dir, err := os.MkdirTemp("", "bunkr-chunk-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return NewStore(dir)
}

func TestStore_PutGetDelete(t *testing.T) {
	s := tempStore(t)
	data := []byte("chunk data here")
	chunkID := "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	if err := s.Put(chunkID, data); err != nil {
		t.Fatal(err)
	}

	if !s.Has(chunkID) {
		t.Fatal("expected chunk to exist")
	}

	got, err := s.Get(chunkID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("data doesn't match")
	}

	if err := s.Delete(chunkID); err != nil {
		t.Fatal(err)
	}

	if s.Has(chunkID) {
		t.Fatal("expected chunk to be deleted")
	}
}

func TestStore_List(t *testing.T) {
	s := tempStore(t)

	ids := []string{
		"aaaa111122223333444455556666777788889999aaaabbbbccccddddeeee0000",
		"bbbb111122223333444455556666777788889999aaaabbbbccccddddeeee0000",
		"cccc111122223333444455556666777788889999aaaabbbbccccddddeeee0000",
	}

	for _, id := range ids {
		if err := s.Put(id, []byte("data")); err != nil {
			t.Fatal(err)
		}
	}

	listed, err := s.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(listed))
	}
}

func TestStore_DiskUsage(t *testing.T) {
	s := tempStore(t)

	data := make([]byte, 1024)
	s.Put("aaaa0000111122223333444455556666777788889999aaaabbbbccccddddeee1", data)
	s.Put("bbbb0000111122223333444455556666777788889999aaaabbbbccccddddeee2", data)

	usage, err := s.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage != 2048 {
		t.Fatalf("expected 2048 bytes, got %d", usage)
	}
}

func TestStore_GetNonExistent(t *testing.T) {
	s := tempStore(t)
	_, err := s.Get("nonexistent")
	if err == nil {
		t.Fatal("expected error for non-existent chunk")
	}
}
