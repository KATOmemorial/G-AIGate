package access

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateKeyShapeAndDigest(t *testing.T) {
	raw, prefix, hash, err := GenerateKey()
	require.NoError(t, err)
	require.Len(t, raw, 46)
	assert.True(t, strings.HasPrefix(raw, "sk-"))
	entropy, err := base64.RawURLEncoding.DecodeString(raw[3:])
	require.NoError(t, err)
	assert.Len(t, entropy, 32)
	assert.Equal(t, raw[:8], prefix)
	assert.Len(t, prefix, 8)
	digest := sha256.Sum256([]byte(raw))
	assert.Equal(t, hex.EncodeToString(digest[:]), hash)
}

func TestGenerateKeyUsesAllEntropy(t *testing.T) {
	entropy := make([]byte, 32)
	for i := range entropy {
		entropy[i] = byte(i)
	}
	raw, prefix, hash, err := generateKey(bytes.NewReader(entropy))
	require.NoError(t, err)
	assert.Equal(t, "sk-"+base64.RawURLEncoding.EncodeToString(entropy), raw)
	assert.Equal(t, raw[:8], prefix)
	digest := sha256.Sum256([]byte(raw))
	assert.Equal(t, hex.EncodeToString(digest[:]), hash)
}

type failingEntropyReader struct{ err error }

func (r failingEntropyReader) Read(p []byte) (int, error) {
	p[0] = 42 // Failure after partial entropy must not produce partial credentials.
	return 1, r.err
}

func TestGenerateKeyFailureHasNoPartialValues(t *testing.T) {
	failure := errors.New("entropy unavailable")
	for _, tc := range []struct {
		name string
		r    io.Reader
		err  error
	}{
		{"empty", bytes.NewReader(nil), io.EOF},
		{"short", bytes.NewReader(make([]byte, 31)), io.ErrUnexpectedEOF},
		{"reader failure", failingEntropyReader{failure}, failure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, prefix, hash, err := generateKey(tc.r)
			require.ErrorIs(t, err, tc.err)
			assert.Empty(t, raw)
			assert.Empty(t, prefix)
			assert.Empty(t, hash)
		})
	}
}

func TestGenerateKeyConcurrent(t *testing.T) {
	const count = 64
	type result struct {
		raw string
		err error
	}
	results := make(chan result, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			raw, _, _, err := GenerateKey()
			results <- result{raw, err}
		}()
	}
	wg.Wait()
	close(results)
	seen := make(map[string]bool, count)
	for got := range results {
		require.NoError(t, got.err)
		assert.False(t, seen[got.raw], "duplicate generated credential")
		seen[got.raw] = true
	}
	assert.Len(t, seen, count)
}
