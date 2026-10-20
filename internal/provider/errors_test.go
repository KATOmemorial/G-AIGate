package provider_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"gaigate/internal/provider"

	"github.com/stretchr/testify/require"
)

func TestUpstreamErrorClassificationAndSafeText(t *testing.T) {
	cause := errors.New("https://user:secret@host/private?token=secret")
	original := &provider.UpstreamError{StatusCode: 503, Body: []byte("secret"), Cause: cause, Retryable: true}
	err := fmt.Errorf("attempt: %w", original)
	var upstream *provider.UpstreamError
	require.ErrorAs(t, err, &upstream)
	require.Same(t, original, upstream)
	require.ErrorIs(t, err, cause)
	require.NotContains(t, err.Error(), "secret")
	require.NotContains(t, err.Error(), "https://")
	encoded, encodeErr := json.Marshal(provider.StreamEvent{Kind: provider.Error, Err: cause})
	require.NoError(t, encodeErr)
	require.NotContains(t, string(encoded), "Err")
	require.NotContains(t, string(encoded), "secret")
	for _, sentinel := range []error{provider.ErrStreamInterrupted, provider.ErrNoCandidate, provider.ErrUpstreamUnavailable} {
		require.ErrorIs(t, fmt.Errorf("wrapped: %w", sentinel), sentinel)
	}
}
