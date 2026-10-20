// Package access provides user API Key generation and authentication (13).
package access

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
)

// GenerateKey returns a one-time credential, its full eight-byte prefix and
// SHA-256 fingerprint. Only prefix and hash may be persisted.
func GenerateKey() (raw, prefix, hash string, err error) {
	return generateKey(rand.Reader)
}

// The reader is local to this call, so failure tests need no shared random source.
func generateKey(reader io.Reader) (raw, prefix, hash string, err error) {
	var entropy [32]byte
	if _, err := io.ReadFull(reader, entropy[:]); err != nil {
		return "", "", "", fmt.Errorf("access: generate API Key: %w", err)
	}
	raw = "sk-" + base64.RawURLEncoding.EncodeToString(entropy[:])
	digest := sha256.Sum256([]byte(raw))
	return raw, raw[:8], hex.EncodeToString(digest[:]), nil
}
