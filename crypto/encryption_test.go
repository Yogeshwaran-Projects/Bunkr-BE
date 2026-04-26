package crypto

import (
	"bytes"
	"testing"
)

func TestEncryptDecrypt(t *testing.T) {
	key, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}

	plaintext := []byte("hello bunkr distributed file system")

	ciphertext, err := Encrypt(key, plaintext)
	if err != nil {
		t.Fatal(err)
	}

	// Ciphertext should be different from plaintext
	if bytes.Equal(ciphertext, plaintext) {
		t.Fatal("ciphertext should differ from plaintext")
	}

	decrypted, err := Decrypt(key, ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("decrypted doesn't match: got %q, want %q", decrypted, plaintext)
	}
}

func TestDecryptWrongKey(t *testing.T) {
	key1, _ := GenerateKey()
	key2, _ := GenerateKey()

	ciphertext, _ := Encrypt(key1, []byte("secret data"))

	_, err := Decrypt(key2, ciphertext)
	if err == nil {
		t.Fatal("expected error when decrypting with wrong key")
	}
}

func TestDecryptTampered(t *testing.T) {
	key, _ := GenerateKey()
	ciphertext, _ := Encrypt(key, []byte("important data"))

	// Flip a bit in the ciphertext (not the nonce)
	tampered := make([]byte, len(ciphertext))
	copy(tampered, ciphertext)
	tampered[len(tampered)-1] ^= 0xFF

	_, err := Decrypt(key, tampered)
	if err == nil {
		t.Fatal("expected error when decrypting tampered ciphertext")
	}
}

func TestDeriveChunkKey(t *testing.T) {
	masterKey, _ := GenerateKey()
	salt, _ := GenerateSalt()

	key0, err := DeriveChunkKey(masterKey, salt, 0)
	if err != nil {
		t.Fatal(err)
	}
	key1, err := DeriveChunkKey(masterKey, salt, 1)
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Equal(key0, key1) {
		t.Fatal("different chunk indices should produce different keys")
	}

	// Same index + same salt should produce same key (deterministic)
	key0Again, _ := DeriveChunkKey(masterKey, salt, 0)
	if !bytes.Equal(key0, key0Again) {
		t.Fatal("same chunk index should produce same key")
	}

	if len(key0) != 32 {
		t.Fatalf("expected 32-byte key, got %d", len(key0))
	}

	// Different salt should produce different key
	salt2, _ := GenerateSalt()
	key0DiffSalt, _ := DeriveChunkKey(masterKey, salt2, 0)
	if bytes.Equal(key0, key0DiffSalt) {
		t.Fatal("different salts should produce different keys")
	}
}

func TestHashData(t *testing.T) {
	hash1 := HashData([]byte("hello"))
	hash2 := HashData([]byte("hello"))
	hash3 := HashData([]byte("world"))

	if hash1 != hash2 {
		t.Fatal("same data should produce same hash")
	}
	if hash1 == hash3 {
		t.Fatal("different data should produce different hash")
	}
	if len(hash1) != 64 {
		t.Fatalf("expected 64-char hex hash, got %d", len(hash1))
	}
}

func TestEncryptLargeData(t *testing.T) {
	key, _ := GenerateKey()
	// 4MB chunk
	data := make([]byte, 4*1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	ciphertext, err := Encrypt(key, data)
	if err != nil {
		t.Fatal(err)
	}

	decrypted, err := Decrypt(key, ciphertext)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decrypted, data) {
		t.Fatal("4MB round-trip failed")
	}
}
