package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gaigate/internal/provider"

	"github.com/stretchr/testify/require"
)

type observedBody struct {
	reader       io.Reader
	read, closes int
}

func (b *observedBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.read += n
	return n, err
}

func (b *observedBody) Close() error { b.closes++; return nil }

func withBody(t *testing.T, status int, media string, body *observedBody) provider.Provider {
	t.Helper()
	return newAdapter(t, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{media}}, Body: body}, nil
	})})
}

func TestChatBodyLimitAndOwnership(t *testing.T) {
	for _, size := range []int{maxResponseBody, maxResponseBody + 1, maxResponseBody + 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			raw := `{"padding":"` + strings.Repeat("x", size-len(`{"padding":""}`)) + `"}`
			require.Len(t, raw, size)
			body := &observedBody{reader: strings.NewReader(raw)}
			resp, _, err := withBody(t, 200, "application/json", body).Chat(t.Context(), endpoint("http://host"), basicRequest())
			if size == maxResponseBody {
				require.NoError(t, err)
				require.Equal(t, raw, string(resp.Body))
			} else {
				require.Error(t, err)
				require.Nil(t, resp)
			}
			require.Equal(t, min(size, maxResponseBody+1), body.read)
			require.Equal(t, 1, body.closes)
		})
	}
	for _, tc := range []struct{ raw, media string }{{`{}`, "application/json"}, {`bad`, "application/json"}, {`{"usage":{}}`, "application/json"}, {`unread`, "text/plain"}} {
		body := &observedBody{reader: strings.NewReader(tc.raw)}
		_, _, _ = withBody(t, 200, tc.media, body).Chat(t.Context(), endpoint("http://host"), basicRequest())
		require.Equal(t, 1, body.closes)
		if tc.media == "text/plain" {
			require.Zero(t, body.read)
		}
	}
}

func TestUsageValidatesBothTokenFields(t *testing.T) {
	for _, field := range []string{"prompt_tokens", "completion_tokens"} {
		for _, value := range []string{`null`, `true`, `"1"`, `-1`, `0.5`, `9223372036854775808`} {
			fields := map[string]json.RawMessage{
				"usage": json.RawMessage(fmt.Sprintf(`{"prompt_tokens":0,"completion_tokens":0,%q:%s}`, field, value)),
			}
			_, err := parseUsage(fields)
			require.Error(t, err, "%s=%s", field, value)
		}
	}
}

func TestHTTPErrorClassification(t *testing.T) {
	for _, status := range []int{201, 204, 400, 401, 403, 404, 422, 429, 500, 503, 599} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			body := &observedBody{reader: strings.NewReader(`{"error":{"message":"upstream failure"}}`)}
			_, _, err := withBody(t, status, "application/json", body).Chat(t.Context(), endpoint("http://host"), basicRequest())
			var upstream *provider.UpstreamError
			require.ErrorAs(t, err, &upstream)
			require.ErrorIs(t, err, provider.ErrUpstreamUnavailable)
			require.Equal(t, status, upstream.StatusCode)
			require.Equal(t, status == 429 || status >= 500, upstream.Retryable)
			require.Equal(t, `{"error":{"message":"upstream failure"}}`, string(upstream.Body))
			require.False(t, upstream.BodyTruncated)
			require.Equal(t, 1, body.closes)
		})
	}
}

func TestErrorBodyProbeAndSecrets(t *testing.T) {
	for _, size := range []int{0, 1023, 1024, 1025, 4096} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			body := &observedBody{reader: strings.NewReader(strings.Repeat("x", size))}
			_, _, err := withBody(t, 400, "application/json", body).Chat(t.Context(), endpoint("http://host"), basicRequest())
			var upstream *provider.UpstreamError
			require.ErrorAs(t, err, &upstream)
			require.Len(t, upstream.Body, min(size, maxErrorBody))
			require.Equal(t, size > maxErrorBody, upstream.BodyTruncated)
			require.Equal(t, min(size, maxErrorBody+1), body.read)
			require.Equal(t, 1, body.closes)
			require.NotContains(t, err.Error(), "xxxx")
		})
	}
	for _, raw := range []string{
		`{"error":{"message":"prefix sk-private-upstream-test suffix"}}`,
		`{"nested":[1,{"value":"sk-private-upstream-tes\u0074"}]}`,
		`{"sk-private-upstream-test":true}`,
		`"sk-private-upstream-test"`,
		`{"duplicate":"sk-private-upstream-test","duplicate":"safe"}`,
		`{"large":1e100000,"secret":"sk-private-upstream-test"}`,
	} {
		body := &observedBody{reader: strings.NewReader(raw)}
		_, _, err := withBody(t, 400, "application/json", body).Chat(t.Context(), endpoint("http://host"), basicRequest())
		var upstream *provider.UpstreamError
		require.ErrorAs(t, err, &upstream)
		require.Empty(t, upstream.Body, raw)
		require.NotContains(t, err.Error(), "sk-private")
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestReadAndNetworkErrorsPreserveCause(t *testing.T) {
	cause := errors.New("sensitive network details")
	for _, status := range []int{200, 503} {
		body := &observedBody{reader: failingReader{cause}}
		_, _, err := withBody(t, status, "application/json", body).Chat(t.Context(), endpoint("http://host"), basicRequest())
		var upstream *provider.UpstreamError
		require.ErrorAs(t, err, &upstream)
		require.ErrorIs(t, err, cause)
		require.Zero(t, upstream.StatusCode)
		require.True(t, upstream.Retryable)
		require.Equal(t, 1, body.closes)
		require.NotContains(t, err.Error(), "sensitive")
	}
	calls := 0
	p := newAdapter(t, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return nil, cause
	})})
	_, _, err := p.Chat(t.Context(), endpoint("http://host"), basicRequest())
	var upstream *provider.UpstreamError
	require.ErrorAs(t, err, &upstream)
	require.ErrorIs(t, err, cause)
	require.Zero(t, upstream.StatusCode)
	require.True(t, upstream.Retryable)
	require.Equal(t, 1, calls)
}

func TestContextCancelsDialAndDeadline(t *testing.T) {
	entered := make(chan struct{})
	release, exited := make(chan struct{}), make(chan struct{})
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		defer close(exited)
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return nil, errors.New("fixture dial released")
		}
	}}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	_, _, err := newAdapter(t, &http.Client{Transport: transport}).Chat(ctx, endpoint("http://host"), basicRequest())
	close(release)
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("fixture dial did not exit")
	}
	// net/http may detach DialContext cancellation to allow connection reuse;
	// the HTTP attempt must still obey its own deadline without an adapter worker.
	require.ErrorIs(t, err, context.DeadlineExceeded)
	select {
	case <-entered:
	default:
		t.Fatal("dial was not entered")
	}
}

func TestCallerTimeoutDoesNotOverrideContext(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer s.Close()
	client := s.Client()
	client.Timeout = time.Nanosecond
	_, _, err := newAdapter(t, client).Chat(t.Context(), endpoint(s.URL), basicRequest())
	require.NoError(t, err)
	require.Equal(t, time.Nanosecond, client.Timeout)
}
