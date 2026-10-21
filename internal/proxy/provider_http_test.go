package proxy_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/provider/openai"
	"gaigate/internal/proxy"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type countedBody struct {
	io.ReadCloser
	closes      *atomic.Int64
	readEntered chan struct{}
	readOnce    sync.Once
}

func (b *countedBody) Read(p []byte) (int, error) {
	if b.readEntered != nil {
		b.readOnce.Do(func() { close(b.readEntered) })
	}
	return b.ReadCloser.Read(p)
}

func (b *countedBody) Close() error { b.closes.Add(1); return b.ReadCloser.Close() }

type countedStream struct {
	provider.Stream
	closes int
}

func (s *countedStream) Close() error { s.closes++; return s.Stream.Close() }

func actualAdapter(t *testing.T, client *http.Client) (provider.Provider, *atomic.Int64) {
	t.Helper()
	return actualAdapterWithReadBarrier(t, client, nil)
}

func actualAdapterWithReadBarrier(t *testing.T, client *http.Client, entered chan struct{}) (provider.Provider, *atomic.Int64) {
	t.Helper()
	var closes atomic.Int64
	transport := client.Transport
	clientCopy := *client
	clientCopy.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		response, err := transport.RoundTrip(r)
		if response != nil {
			response.Body = &countedBody{ReadCloser: response.Body, closes: &closes, readEntered: entered}
		}
		return response, err
	})
	p, err := openai.New(&clientCopy, 1)
	require.NoError(t, err)
	return p, &closes
}

func upstreamRequest() *provider.ChatRequest {
	return &provider.ChatRequest{Model: "public", Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"hello"}`)}}
}

func writerChain(t *testing.T, bottom *outputWriter, chain string) http.ResponseWriter {
	t.Helper()
	if chain == "native" {
		return bottom
	}
	c, _ := gin.CreateTestContext(bottom)
	if chain == "WrapWriter" {
		return middleware.WrapWriter(c.Writer)
	}
	return c.Writer
}

func TestForwardActualProviderSSEAndTermination(t *testing.T) {
	const raw = "{\n  \"choices\": [{\"delta\":{\"content\":\"汉字\"}}]\n}"
	const usageRaw = `{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":7}}`
	for _, tc := range []struct {
		name, wire  string
		want        []string
		chunks      int
		usage       bool
		interrupted bool
		abort       bool
	}{
		{"normal", strings.Repeat("data: {}\n\n", 8) + "data: " + usageRaw + "\n\ndata: [DONE]\n\n", append(append(makeFilled(8, "{}"), usageRaw), "[DONE]"), 8, true, false, false},
		{"Done only", "data: [DONE]\n\n", []string{"[DONE]"}, 0, false, false, false},
		{"comments and multiline", ": heartbeat\r\n\r\nevent: ignored\r\nid: 1\r\nretry: 5\r\ndata: {\r\ndata:   \"choices\": [{\"delta\":{\"content\":\"汉字\"}}]\r\ndata: }\r\n\r\ndata: [DONE]\r\n\r\n", []string{raw, "[DONE]"}, 1, false, false, false},
		{"first protocol error", "data: invalid\n\n", nil, 0, false, true, false},
		{"empty EOF", "", nil, 0, false, true, false},
		{"content then EOF", "data: {}\n\n", []string{"{}"}, 1, false, true, false},
		{"content then abort", "data: {}\n\n", []string{"{}"}, 1, false, true, true},
	} {
		for _, chain := range []string{"native", "Gin", "WrapWriter"} {
			t.Run(tc.name+"/"+chain, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, tc.wire)
					if tc.abort {
						w.(http.Flusher).Flush()
						panic(http.ErrAbortHandler)
					}
				}))
				defer server.Close()
				p, bodyCloses := actualAdapter(t, server.Client())
				ep := &provider.Endpoint{BaseURL: server.URL, Model: "upstream", APIKey: "sk-fixture"}
				bottom := newOutput()
				writer := writerChain(t, bottom, chain)
				var stream *countedStream
				callbacks := 0
				result, err := proxy.NewForwarder().Forward(t.Context(), writer, func(ctx context.Context) (provider.Stream, error) {
					opened, err := p.ChatStream(ctx, ep, upstreamRequest())
					if err != nil {
						return nil, err
					}
					stream = &countedStream{Stream: opened}
					return stream, nil
				}, options, func(provider.StreamEvent) { callbacks++ })
				if tc.interrupted {
					require.ErrorIs(t, err, provider.ErrStreamInterrupted)
					require.Equal(t, proxy.ResultStreamInterrupted, result.Kind)
				} else {
					require.NoError(t, err)
					require.Equal(t, proxy.ResultSuccess, result.Kind)
				}
				if tc.abort {
					require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				}
				if strings.Contains(tc.name, "EOF") {
					require.ErrorIs(t, err, io.EOF)
				}
				require.Equal(t, tc.chunks, result.Chunks)
				require.Equal(t, tc.chunks+btoi(tc.usage), callbacks)
				require.Equal(t, tc.usage, result.Usage != nil)
				if tc.usage {
					require.Equal(t, &provider.Usage{InputTokens: 4, OutputTokens: 7}, result.Usage)
				}
				if len(tc.want) == 0 {
					assertUncommitted(t, bottom, result)
				} else {
					require.Equal(t, tc.want, payloads(t, bottom.body.String()))
					require.True(t, result.WroteHeader)
				}
				require.NotContains(t, bottom.body.String(), "heartbeat")
				require.Equal(t, 1, stream.closes, "proxy owns one Close")
				require.NoError(t, stream.Stream.Close(), "provider Close stays idempotent after proxy returns")
				require.EqualValues(t, 1, bodyCloses.Load())
				_, open := <-stream.Events()
				require.False(t, open, "Close must have joined the provider worker")
				if tracked, ok := writer.(gin.ResponseWriter); ok {
					require.Equal(t, bottom.body.Len(), max(0, tracked.Size()))
					require.Equal(t, result.WroteHeader, tracked.Written())
				}
			})
		}
	}
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}
func makeFilled(n int, value string) []string {
	values := make([]string, n)
	for i := range values {
		values[i] = value
	}
	return values
}

// This is a provider/proxy fixture, not the future gateway's HTTP abort handler.
// A never-ending upstream also proves failure closes the real HTTP body promptly.
func TestForwardActualProviderCleanupOnOutputFailureAndCancel(t *testing.T) {
	for _, path := range []string{"Write", "Flush first", "Flush later", "cancel"} {
		t.Run(path, func(t *testing.T) {
			exited := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(exited)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {}\n\ndata: {}\n\n")
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p, bodyCloses := actualAdapter(t, server.Client())
			bottom := newOutput()
			bottom.cause = io.ErrClosedPipe
			switch path {
			case "Write":
				bottom.failWrite = 1
			case "Flush first":
				bottom.failFlush = 1
			case "Flush later":
				bottom.failFlush = 2
			}
			writer := writerChain(t, bottom, "WrapWriter")
			var stream *countedStream
			callbacks := 0
			result, err := proxy.NewForwarder().Forward(ctx, writer, func(attempt context.Context) (provider.Stream, error) {
				opened, err := p.ChatStream(attempt, &provider.Endpoint{BaseURL: server.URL, Model: "m"}, upstreamRequest())
				if err != nil {
					return nil, err
				}
				stream = &countedStream{Stream: opened}
				return stream, nil
			}, options, func(provider.StreamEvent) {
				callbacks++
				if path == "cancel" {
					cancel()
				}
			})
			if path == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, proxy.ResultClientCanceled, result.Kind)
				require.Equal(t, 1, callbacks)
			} else {
				require.ErrorIs(t, err, io.ErrClosedPipe)
				require.Equal(t, proxy.ResultWriteError, result.Kind)
				require.Equal(t, btoi(path == "Flush later"), callbacks)
			}
			require.True(t, result.WroteHeader)
			require.Equal(t, 1, stream.closes)
			require.EqualValues(t, 1, bodyCloses.Load())
			require.NoError(t, stream.Stream.Close())
			// Any bounded queued events are harmless; Close has closed the channel.
			for event := range stream.Events() {
				require.NotEqual(t, provider.Done, event.Kind)
			}
			select {
			case <-exited:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("upstream request did not observe cleanup cancellation")
			}
			require.NotContains(t, bottom.body.String(), "[DONE]")
			require.Nil(t, result.Usage)
		})
	}
}

func TestNonStreamActualProviderAndHTTPWriterChains(t *testing.T) {
	for _, chain := range []string{"native", "Gin", "WrapWriter"} {
		t.Run(chain, func(t *testing.T) {
			const raw = " \n{\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":7},\"extra\": 1e9999}\n"
			var requests atomic.Int64
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				var fields map[string]json.RawMessage
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&fields))
				assert.JSONEq(t, `false`, string(fields["stream"]), "ResponseOpener must call Chat, never ChatStream")
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Upstream-Private", "must-not-copy")
				_, _ = io.WriteString(w, raw)
			}))
			defer upstream.Close()
			p, bodyCloses := actualAdapter(t, upstream.Client())
			type outcome struct {
				result  proxy.ForwardResult
				err     error
				size    int
				written bool
			}
			finished := make(chan outcome, 1)
			forward := func(w http.ResponseWriter, r *http.Request) {
				result, err := proxy.NewForwarder().ForwardNonStream(r.Context(), w, func(ctx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
					return p.Chat(ctx, &provider.Endpoint{BaseURL: upstream.URL, Model: "m"}, upstreamRequest())
				}, options)
				got := outcome{result: result, err: err}
				if tracked, ok := w.(gin.ResponseWriter); ok {
					got.size = tracked.Size()
					got.written = tracked.Written()
				}
				finished <- got
			}
			var handler http.Handler = http.HandlerFunc(forward)
			if chain != "native" {
				router := gin.New()
				router.POST("/", func(c *gin.Context) {
					if chain == "WrapWriter" {
						c.Writer = middleware.WrapWriter(c.Writer)
					}
					forward(c.Writer, c.Request)
				})
				handler = router
			}
			downstream := httptest.NewServer(handler)
			defer downstream.Close()
			response, err := downstream.Client().Post(downstream.URL, "application/json", strings.NewReader(`{}`))
			require.NoError(t, err)
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, raw, string(body))
			require.Equal(t, 200, response.StatusCode)
			require.Equal(t, "application/json", response.Header.Get("Content-Type"))
			require.Empty(t, response.Header.Get("X-Upstream-Private"))
			got := <-finished
			require.NoError(t, got.err)
			require.Equal(t, proxy.ResultSuccess, got.result.Kind)
			require.True(t, got.result.WroteHeader)
			require.Equal(t, &provider.Usage{InputTokens: 4, OutputTokens: 7}, got.result.Usage)
			require.Zero(t, got.result.Chunks)
			if chain != "native" {
				require.Equal(t, len(raw), got.size)
				require.True(t, got.written)
			}
			require.EqualValues(t, 1, requests.Load())
			require.EqualValues(t, 1, bodyCloses.Load())
		})
	}
}

func TestForwardActualProviderOpeningErrorsStayUncommitted(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, status := range []int{400, 401, 429, 503} {
			t.Run(fmt.Sprintf("stream=%t/status=%d", streaming, status), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"error":{"message":"upstream denied"}}`)
				}))
				defer server.Close()
				p, closes := actualAdapter(t, server.Client())
				ep := &provider.Endpoint{BaseURL: server.URL, Model: "m"}
				w := newOutput()
				writer := writerChain(t, w, "WrapWriter")
				var result proxy.ForwardResult
				var err error
				if streaming {
					result, err = proxy.NewForwarder().Forward(t.Context(), writer, func(ctx context.Context) (provider.Stream, error) { return p.ChatStream(ctx, ep, upstreamRequest()) }, options, nil)
				} else {
					result, err = proxy.NewForwarder().ForwardNonStream(t.Context(), writer, func(ctx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
						return p.Chat(ctx, ep, upstreamRequest())
					}, options)
				}
				var upstreamErr *provider.UpstreamError
				require.ErrorAs(t, err, &upstreamErr)
				require.Equal(t, status, upstreamErr.StatusCode)
				require.Equal(t, proxy.ResultUpstreamError, result.Kind)
				assertUncommitted(t, w, result)
				require.EqualValues(t, 1, closes.Load())
				require.NotContains(t, err.Error(), "upstream denied")
			})
		}
	}
}

func TestNonStreamActualProviderCancelDuringBody(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	readEntered := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(exited)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"partial":`)
		w.(http.Flusher).Flush()
		close(entered)
		<-r.Context().Done()
	}))
	defer upstream.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p, closes := actualAdapterWithReadBarrier(t, upstream.Client(), readEntered)
	bottom := newOutput()
	writer := writerChain(t, bottom, "WrapWriter")
	var result proxy.ForwardResult
	var forwardErr error
	var group errgroup.Group
	group.Go(func() error {
		result, forwardErr = proxy.NewForwarder().ForwardNonStream(ctx, writer, func(attempt context.Context) (*provider.ChatResponse, *provider.Usage, error) {
			return p.Chat(attempt, &provider.Endpoint{BaseURL: upstream.URL, Model: "m"}, upstreamRequest())
		}, options)
		return nil
	})
	<-entered
	<-readEntered // Body ownership and the client's read are established before canceling.
	cancel()
	require.NoError(t, group.Wait())
	require.ErrorIs(t, forwardErr, context.Canceled)
	require.Equal(t, proxy.ResultClientCanceled, result.Kind)
	assertUncommitted(t, bottom, result)
	require.EqualValues(t, 1, closes.Load())
	select {
	case <-exited:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("non-stream upstream did not exit after cancellation")
	}
}

func TestForwardActualProviderOverHTTPWrapWriter(t *testing.T) {
	const usageRaw = `{"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":7}}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, strings.Repeat("data: {}\n\n", 8)+"data: "+usageRaw+"\n\ndata: [DONE]\n\n")
	}))
	defer upstream.Close()
	p, closes := actualAdapter(t, upstream.Client())
	type outcome struct {
		result          proxy.ForwardResult
		err             error
		size, callbacks int
		written         bool
	}
	finished := make(chan outcome, 1)
	router := gin.New()
	router.GET("/", func(c *gin.Context) {
		c.Writer = middleware.WrapWriter(c.Writer)
		callbacks := 0
		result, err := proxy.NewForwarder().Forward(c.Request.Context(), c.Writer, func(ctx context.Context) (provider.Stream, error) {
			return p.ChatStream(ctx, &provider.Endpoint{BaseURL: upstream.URL, Model: "m"}, upstreamRequest())
		}, options, func(provider.StreamEvent) { callbacks++ })
		finished <- outcome{result, err, c.Writer.Size(), callbacks, c.Writer.Written()}
	})
	downstream := httptest.NewServer(router)
	defer downstream.Close()
	response, err := downstream.Client().Get(downstream.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, append(append(makeFilled(8, "{}"), usageRaw), "[DONE]"), payloads(t, string(body)))
	require.Equal(t, "text/event-stream; charset=utf-8", response.Header.Get("Content-Type"))
	require.Equal(t, "no-cache", response.Header.Get("Cache-Control"))
	require.Equal(t, "no", response.Header.Get("X-Accel-Buffering"))
	got := <-finished
	require.NoError(t, got.err)
	require.Equal(t, proxy.ResultSuccess, got.result.Kind)
	require.True(t, got.result.WroteHeader)
	require.Equal(t, 8, got.result.Chunks)
	require.Equal(t, 9, got.callbacks)
	require.Equal(t, &provider.Usage{InputTokens: 4, OutputTokens: 7}, got.result.Usage)
	require.True(t, got.written)
	require.Equal(t, len(body), got.size)
	require.EqualValues(t, 1, closes.Load())
}
