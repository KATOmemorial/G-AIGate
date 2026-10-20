package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestStreamReadErrorTextIsSafeAndCausePreserved(t *testing.T) {
	const key = "sk-review-regression-secret"
	cause := &url.Error{
		Op: "read", URL: "https://user:private-password@upstream.test/path?key=" + key,
		Err: errors.New("sensitive read failure: " + key),
	}
	body := &observedBody{reader: io.MultiReader(strings.NewReader("data: {}\n\n"), failingReader{cause})}
	ep := endpoint("http://upstream.test")
	ep.APIKey = key
	stream, err := withBody(t, 200, "text/event-stream", body).ChatStream(t.Context(), ep, basicRequest())
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()
	events := collect(stream)
	require.Len(t, events, 2)
	require.Equal(t, provider.Chunk, events[0].Kind)
	require.Equal(t, provider.Error, events[1].Kind)
	streamErr := events[1].Err
	require.ErrorIs(t, streamErr, provider.ErrStreamInterrupted)
	require.ErrorIs(t, streamErr, cause)
	var original *url.Error
	require.ErrorAs(t, streamErr, &original)
	require.Same(t, cause, original)
	var log bytes.Buffer
	slog.New(slog.NewJSONHandler(&log, nil)).Error("upstream failure", "error", streamErr)
	for _, text := range []string{streamErr.Error(), fmt.Sprint(streamErr), log.String()} {
		for _, sensitive := range []string{key, "private-password", "https://", "upstream.test", "sensitive read failure"} {
			require.NotContains(t, text, sensitive)
		}
	}
	require.NoError(t, stream.Close())
	require.Equal(t, 1, body.closes)
}

func TestEscapedV1RootOverHTTP(t *testing.T) {
	for _, tc := range []struct{ base, escaped, decoded string }{
		{"/%76%31", "/%76%31/chat/completions", "/v1/chat/completions"},
		{"/v%31/", "/v%31/chat/completions", "/v1/chat/completions"},
		{"/prefix/%761///", "/prefix/%761/chat/completions", "/prefix/v1/chat/completions"},
		{"/pre%2Ffix/%76%31/", "/pre%2Ffix/%76%31/chat/completions", "/pre/fix/v1/chat/completions"},
	} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.base, streaming), func(t *testing.T) {
				s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, tc.escaped, r.URL.EscapedPath())
					if r.URL.Path != tc.decoded {
						w.WriteHeader(http.StatusNotFound)
						return
					}
					if streaming {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: [DONE]\n\n")
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, "{}")
					}
				}))
				defer s.Close()
				p := newAdapter(t, s.Client())
				if streaming {
					stream, err := p.ChatStream(t.Context(), endpoint(s.URL+tc.base), basicRequest())
					require.NoError(t, err)
					defer func() { _ = stream.Close() }()
					events := collect(stream)
					require.Len(t, events, 1)
					require.Equal(t, provider.Done, events[0].Kind)
				} else {
					response, _, err := p.Chat(t.Context(), endpoint(s.URL+tc.base), basicRequest())
					require.NoError(t, err)
					require.Equal(t, "{}", string(response.Body))
				}
			})
		}
	}
}

func TestChatStreamWirePreservesRequestAndInput(t *testing.T) {
	for _, options := range []string{"missing", "null", `{"include_usage":false,"other":{"a":[1,2]}}`} {
		t.Run(options, func(t *testing.T) {
			req := richRequest()
			if options == "missing" {
				delete(req.Extra, "stream_options")
			} else {
				req.Extra["stream_options"] = json.RawMessage(options)
			}
			before, err := json.Marshal(req)
			require.NoError(t, err)
			expected, err := serialize(endpoint(""), req, true)
			require.NoError(t, err)
			var attempts atomic.Int64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				body, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.JSONEq(t, string(expected), string(body))
				assert.Equal(t, "Bearer sk-private-upstream-test", r.Header.Get("Authorization"))
				assert.Empty(t, r.Header.Get("Cookie"))
				assert.Empty(t, r.Header.Get("X-Mock-Scenario"))
				w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
				_, _ = io.WriteString(w, "data: {}\n\ndata: [DONE]\n\n")
			}))
			defer s.Close()
			p := newAdapter(t, s.Client())
			var group errgroup.Group
			for range 16 {
				group.Go(func() error {
					stream, err := p.ChatStream(t.Context(), endpoint(s.URL), req)
					if err != nil {
						return err
					}
					defer func() { _ = stream.Close() }()
					events := collect(stream)
					if len(events) != 2 || events[0].Kind != provider.Chunk || events[1].Kind != provider.Done {
						return errors.New("unexpected events")
					}
					return nil
				})
			}
			require.NoError(t, group.Wait())
			require.EqualValues(t, 16, attempts.Load())
			after, err := json.Marshal(req)
			require.NoError(t, err)
			require.Equal(t, string(before), string(after))
		})
	}
}

func TestChatStreamReturnsAfterHeadersAndCancelsEmptyRead(t *testing.T) {
	for _, action := range []string{"data", "Close", "cancel"} {
		t.Run(action, func(t *testing.T) {
			release, exited := make(chan struct{}), make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(exited)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				w.(http.Flusher).Flush()
				select {
				case <-release:
					_, _ = io.WriteString(w, "data: [DONE]\n\n")
				case <-r.Context().Done():
				}
			}))
			defer s.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			stream, err := newAdapter(t, s.Client()).ChatStream(ctx, endpoint(s.URL), basicRequest())
			require.NoError(t, err, "headers are sufficient, no first-event wait")
			defer func() { _ = stream.Close() }()
			select {
			case <-stream.Events():
				t.Fatal("unexpected event before release")
			default:
			}
			started := time.Now()
			switch action {
			case "data":
				close(release)
				events := collect(stream)
				require.Len(t, events, 1)
				require.Equal(t, provider.Done, events[0].Kind, "Done-only and no usage are legal")
			case "Close":
				require.NoError(t, stream.Close())
			case "cancel":
				cancel()
			}
			require.NoError(t, stream.Close())
			select {
			case <-exited:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("upstream did not exit")
			}
			require.Less(t, time.Since(started), 500*time.Millisecond)
		})
	}
}

func TestChatStreamOpeningFailures(t *testing.T) {
	for _, tc := range []struct {
		status int
		media  string
	}{{200, "application/json"}, {200, ""}, {400, "application/json"}, {429, "application/json"}, {503, "application/json"}, {307, "text/plain"}} {
		t.Run(fmt.Sprintf("%d/%s", tc.status, tc.media), func(t *testing.T) {
			body := &observedBody{reader: strings.NewReader(`{"error":{"message":"failed"}}`)}
			stream, err := withBody(t, tc.status, tc.media, body).ChatStream(t.Context(), endpoint("http://host"), basicRequest())
			require.Nil(t, stream)
			var upstream *provider.UpstreamError
			require.ErrorAs(t, err, &upstream)
			require.Equal(t, tc.status, upstream.StatusCode)
			require.Equal(t, tc.status == 429 || tc.status == 503, upstream.Retryable)
			require.Equal(t, 1, body.closes)
		})
	}
	var calls atomic.Int64
	p := newAdapter(t, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected network")
	})})
	stream, err := p.ChatStream(t.Context(), endpoint("http://user:secret@host"), basicRequest())
	require.Error(t, err)
	require.Nil(t, stream)
	require.Zero(t, calls.Load())
}

func TestChatStreamRealEOFAndTruncation(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(fmt.Sprint(abort), func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {}\n\n")
				w.(http.Flusher).Flush()
				if abort {
					panic(http.ErrAbortHandler)
				}
			}))
			defer s.Close()
			stream, err := newAdapter(t, s.Client()).ChatStream(t.Context(), endpoint(s.URL), basicRequest())
			require.NoError(t, err)
			defer func() { _ = stream.Close() }()
			events := collect(stream)
			require.Len(t, events, 2)
			require.Equal(t, provider.Chunk, events[0].Kind)
			require.Equal(t, provider.Error, events[1].Kind)
			require.ErrorIs(t, events[1].Err, provider.ErrStreamInterrupted)
			if abort {
				require.ErrorIs(t, events[1].Err, io.ErrUnexpectedEOF)
			} else {
				require.ErrorIs(t, events[1].Err, io.EOF)
			}
		})
	}
}
