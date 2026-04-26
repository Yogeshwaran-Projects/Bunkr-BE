package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"golang.org/x/crypto/hkdf"
)

// GenerateKey creates a random 32-byte AES-256 encryption key.
func GenerateKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, fmt.Errorf("failed to generate key: %w", err)
	}
	return key, nil
}

// GenerateSalt creates a random 16-byte salt for HKDF key derivation.
func GenerateSalt() ([]byte, error) {
	salt := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, salt); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}
	return salt, nil
}

// GenerateID creates a random hex-encoded ID.
func GenerateID() string {
	buf := make([]byte, 16)
	io.ReadFull(rand.Reader, buf)
	return hex.EncodeToString(buf)
}

// Encrypt encrypts plaintext using AES-256-GCM. The nonce is prepended to the ciphertext.
func Encrypt(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Seal appends the ciphertext to nonce
	ciphertext := aead.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts ciphertext encrypted with Encrypt. Expects nonce prepended.
func Decrypt(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := aead.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ct := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed (tampered or wrong key): %w", err)
	}

	return plaintext, nil
}

// DeriveChunkKey derives a unique key for each chunk using HKDF with a per-file salt.
// The salt ensures two files encrypted with the same master key produce different chunk keys.
func DeriveChunkKey(masterKey, salt []byte, chunkIndex int) ([]byte, error) {
	info := []byte(fmt.Sprintf("bunkr-chunk-%d", chunkIndex))
	hkdfReader := hkdf.New(sha256.New, masterKey, salt, info)
	derived := make([]byte, 32)
	if _, err := io.ReadFull(hkdfReader, derived); err != nil {
		return nil, fmt.Errorf("key derivation failed: %w", err)
	}
	return derived, nil
}

// HashData returns the SHA256 hex hash of data.
func HashData(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
