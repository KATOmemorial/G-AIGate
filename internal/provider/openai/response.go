package openai

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"gaigate/internal/provider"
)

const (
	maxResponseBody = 10 << 20
	maxErrorBody    = 1024
)

func jsonObject(raw []byte) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return nil, errors.New("openai: expected one complete JSON object")
	}
	return fields, nil
}

func parseUsage(fields map[string]json.RawMessage) (*provider.Usage, error) {
	raw, ok := fields["usage"]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	usage, err := jsonObject(raw)
	if err != nil {
		return nil, err
	}
	counts := [2]int64{}
	for i, name := range []string{"prompt_tokens", "completion_tokens"} {
		var value *int64
		if err := json.Unmarshal(usage[name], &value); err != nil || value == nil || *value < 0 {
			return nil, errors.New("openai: usage tokens must be nonnegative int64 integers")
		}
		counts[i] = *value
	}
	return &provider.Usage{InputTokens: counts[0], OutputTokens: counts[1]}, nil
}

func transportError(cause error) error {
	return &provider.UpstreamError{Retryable: true, Cause: cause}
}

func protocolError(cause error) error {
	return &provider.UpstreamError{StatusCode: http.StatusOK, Cause: cause}
}

func httpError(resp *http.Response, key string) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
	truncated := len(body) > maxErrorBody
	if truncated {
		body = body[:maxErrorBody]
	}
	if !truncated && containsSecret(body, key) {
		body = nil
	}
	if err != nil {
		return &provider.UpstreamError{Body: body, BodyTruncated: truncated, Retryable: true, Cause: err}
	}
	return &provider.UpstreamError{
		StatusCode: resp.StatusCode, Body: body, BodyTruncated: truncated,
		Retryable: resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500 && resp.StatusCode <= 599,
		Cause:     provider.ErrUpstreamUnavailable,
	}
}

// Inspect decoded strings (including keys), so JSON escapes cannot hide a key.
// Do not decode numbers as float64: unrelated large numbers are valid JSON.
func containsSecret(body []byte, key string) bool {
	if key == "" || !json.Valid(body) {
		return false
	}
	d := json.NewDecoder(bytes.NewReader(body))
	d.UseNumber()
	for {
		token, err := d.Token()
		if err != nil {
			return false
		}
		if value, ok := token.(string); ok && strings.Contains(value, key) {
			return true
		}
	}
}
