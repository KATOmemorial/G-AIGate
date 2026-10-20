package provider

import (
	"errors"
	"fmt"
)

var (
	ErrStreamInterrupted   = errors.New("upstream stream interrupted")
	ErrNoCandidate         = errors.New("no upstream candidate")
	ErrUpstreamUnavailable = errors.New("upstream unavailable")
)

// UpstreamError retains classification and bounded diagnostic bytes. Body is
// untrusted: truncated or unsafe JSON must never be forwarded to a client.
type UpstreamError struct {
	StatusCode    int
	Body          []byte
	BodyTruncated bool
	Retryable     bool
	Cause         error
}

// Error deliberately omits the body and cause text, which may contain secrets.
func (e *UpstreamError) Error() string {
	return fmt.Sprintf("upstream request failed (status=%d)", e.StatusCode)
}

func (e *UpstreamError) Unwrap() error { return e.Cause }
