package proxy_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestNonStreamPreservesJSONAndUsageCopies(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		usage      *provider.Usage
	}{
		{"missing", " \n{\"id\":\"汉字\",\"unknown\":1e10000,\"choices\":[]} \n", nil},
		{"null", `{"usage":null}`, nil},
		{"zero", `{"usage":{"prompt_tokens":0,"completion_tokens":0}}`, &provider.Usage{}},
		{"usage", ` {"usage":{"prompt_tokens":9,"completion_tokens":12,"total_tokens":21},"choices":[]} `, &provider.Usage{InputTokens: 9, OutputTokens: 12}},
	} {
		for _, chain := range []string{"native", "Gin", "WrapWriter"} {
			t.Run(tc.name+"/"+chain, func(t *testing.T) {
				bottom := newOutput()
				bottom.header.Set("Content-Length", "999")
				bottom.header.Set("X-Request-Id", "req_nonstream")
				var writer http.ResponseWriter = bottom
				if chain != "native" {
					c, _ := gin.CreateTestContext(bottom)
					writer = c.Writer
					if chain == "WrapWriter" {
						writer = middleware.WrapWriter(c.Writer)
					}
				}
				var attempt context.Context
				calls := 0
				response := &provider.ChatResponse{Body: []byte(tc.body), Usage: tc.usage}
				result, err := proxy.NewForwarder().ForwardNonStream(t.Context(), writer, func(ctx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
					calls++
					attempt = ctx
					require.NotSame(t, t.Context(), ctx)
					return response, tc.usage, nil
				}, options)
				require.NoError(t, err)
				require.Equal(t, 1, calls)
				require.ErrorIs(t, attempt.Err(), context.Canceled)
				require.Equal(t, proxy.ResultSuccess, result.Kind)
				require.True(t, result.WroteHeader)
				require.GreaterOrEqual(t, result.FirstByteMs, int64(0))
				require.Zero(t, result.Chunks)
				require.Equal(t, tc.body, bottom.body.String())
				require.Equal(t, tc.body, string(response.Body))
				require.Equal(t, "application/json", bottom.committed.Get("Content-Type"))
				require.Empty(t, bottom.committed.Get("Content-Length"))
				require.Equal(t, "req_nonstream", bottom.committed.Get("X-Request-Id"))
				require.Equal(t, 1, bottom.writes)
				require.Equal(t, 1, bottom.flushes)
				require.Equal(t, tc.usage, result.Usage)
				if tc.usage != nil {
					require.NotSame(t, tc.usage, result.Usage)
					before := *result.Usage
					tc.usage.InputTokens++
					require.Equal(t, before, *result.Usage)
					tc.usage.InputTokens--
				}
				if tracked, ok := writer.(gin.ResponseWriter); ok {
					require.True(t, tracked.Written())
					require.Equal(t, len(tc.body), tracked.Size())
					require.Equal(t, 200, tracked.Status())
				}
			})
		}
	}
}

func TestNonStreamRejectsInvalidResponsesBeforeCommit(t *testing.T) {
	counts := &provider.Usage{InputTokens: 1, OutputTokens: 2}
	for _, tc := range []struct {
		name, body                   string
		responseUsage, returnedUsage *provider.Usage
	}{
		{"empty", "", nil, nil}, {"null", "null", nil, nil}, {"array", "[]", nil, nil}, {"scalar", "1", nil, nil},
		{"bad", "bad", nil, nil}, {"partial", "{", nil, nil}, {"trailing JSON", "{} {}", nil, nil},
		{"error body", `{"error":{"message":"private upstream failure"}}`, nil, nil},
		{"missing return", `{"usage":{"prompt_tokens":1,"completion_tokens":2}}`, counts, nil},
		{"missing response usage", `{"usage":{"prompt_tokens":1,"completion_tokens":2}}`, nil, counts},
		{"unexpected usage", `{}`, counts, counts},
		{"null usage mismatch", `{"usage":null}`, counts, counts},
		{"body mismatch", `{"usage":{"prompt_tokens":3,"completion_tokens":2}}`, counts, counts},
		{"struct mismatch", `{"usage":{"prompt_tokens":1,"completion_tokens":2}}`, &provider.Usage{}, counts},
		{"usage missing fields", `{"usage":{}}`, &provider.Usage{}, &provider.Usage{}},
		{"usage wrong field case", `{"usage":{"PROMPT_TOKENS":0,"completion_tokens":0}}`, &provider.Usage{}, &provider.Usage{}},
		{"usage null count", `{"usage":{"prompt_tokens":null,"completion_tokens":0}}`, &provider.Usage{}, &provider.Usage{}},
		{"usage fraction", `{"usage":{"prompt_tokens":0.5,"completion_tokens":0}}`, &provider.Usage{}, &provider.Usage{}},
		{"usage overflow", `{"usage":{"prompt_tokens":9223372036854775808,"completion_tokens":0}}`, &provider.Usage{}, &provider.Usage{}},
		{"usage negative", `{"usage":{"prompt_tokens":-1,"completion_tokens":2}}`, &provider.Usage{InputTokens: -1, OutputTokens: 2}, &provider.Usage{InputTokens: -1, OutputTokens: 2}},
		{"usage array", `{"usage":[]}`, nil, nil},
		{"nil response", "", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newOutput()
			response := &provider.ChatResponse{Body: []byte(tc.body), Usage: tc.responseUsage}
			if tc.name == "nil response" {
				response = nil
			}
			result, err := proxy.NewForwarder().ForwardNonStream(t.Context(), w, func(context.Context) (*provider.ChatResponse, *provider.Usage, error) {
				return response, tc.returnedUsage, nil
			}, options)
			require.ErrorIs(t, err, provider.ErrUpstreamUnavailable)
			require.Equal(t, proxy.ResultUpstreamError, result.Kind)
			assertUncommitted(t, w, result)
			require.Nil(t, result.Usage)
			require.NotContains(t, err.Error(), "private upstream failure")
		})
	}
}

func TestNonStreamOutputFailuresAndOpenerError(t *testing.T) {
	for _, chain := range []string{"native", "Gin", "WrapWriter"} {
		for _, path := range []string{"Write", "short", "Flush", "opener"} {
			t.Run(chain+"/"+path, func(t *testing.T) {
				w := newOutput()
				cause := &url.Error{Op: "write", URL: "https://user:private-password@host?key=sk-private", Err: io.ErrClosedPipe}
				w.cause = cause
				switch path {
				case "Write":
					w.failWrite = 1
				case "short":
					w.short = 1
				case "Flush":
					w.failFlush = 1
				}
				var writer http.ResponseWriter = w
				if chain != "native" {
					c, _ := gin.CreateTestContext(w)
					writer = c.Writer
					if chain == "WrapWriter" {
						writer = middleware.WrapWriter(c.Writer)
					}
				}
				result, err := proxy.NewForwarder().ForwardNonStream(t.Context(), writer, func(context.Context) (*provider.ChatResponse, *provider.Usage, error) {
					if path == "opener" {
						return &provider.ChatResponse{Body: []byte(`{}`)}, nil, cause
					}
					return &provider.ChatResponse{Body: []byte(`{}`)}, nil, nil
				}, options)
				require.Error(t, err)
				if path == "short" {
					require.ErrorIs(t, err, io.ErrShortWrite)
				} else {
					require.ErrorIs(t, err, cause)
					var original *url.Error
					require.ErrorAs(t, err, &original)
					require.Same(t, cause, original)
				}
				require.NotContains(t, err.Error(), "private")
				if path == "opener" {
					require.Equal(t, proxy.ResultUpstreamError, result.Kind)
					assertUncommitted(t, w, result)
				} else {
					require.Equal(t, proxy.ResultWriteError, result.Kind)
					require.True(t, result.WroteHeader)
					require.Equal(t, 1, w.writes)
					if path == "Flush" {
						require.Equal(t, 1, w.flushes)
					} else {
						require.Zero(t, w.flushes)
					}
					if tracked, ok := writer.(gin.ResponseWriter); ok {
						require.True(t, tracked.Written())
						require.Equal(t, w.body.Len(), tracked.Size())
					}
				}
			})
		}
	}
}

func TestNonStreamArgumentsAndMissingWriterCapability(t *testing.T) {
	for _, path := range []string{"nil writer", "typed nil writer", "nil opener", "nil ctx", "TTFB zero", "Overall zero", "TTFB negative", "Overall negative", "opaque writer"} {
		t.Run(path, func(t *testing.T) {
			w := newOutput()
			var writer http.ResponseWriter = w
			ctx, opts, calls := t.Context(), options, 0
			var open proxy.ResponseOpener = func(context.Context) (*provider.ChatResponse, *provider.Usage, error) {
				calls++
				return &provider.ChatResponse{Body: []byte(`{}`)}, nil, nil
			}
			switch path {
			case "nil writer":
				writer = nil
			case "typed nil writer":
				writer = (*outputWriter)(nil)
			case "nil opener":
				open = nil
			case "nil ctx":
				ctx = nil
			case "TTFB zero":
				opts.TTFBTimeout = 0
			case "Overall zero":
				opts.OverallTimeout = 0
			case "TTFB negative":
				opts.TTFBTimeout = -time.Second
			case "Overall negative":
				opts.OverallTimeout = -time.Second
			case "opaque writer":
				writer = &voidFlusher{ResponseWriter: w}
			}
			result, err := proxy.NewForwarder().ForwardNonStream(ctx, writer, open, opts)
			require.Error(t, err)
			require.Zero(t, calls)
			assertUncommitted(t, w, result)
			if path == "opaque writer" {
				require.ErrorIs(t, err, http.ErrNotSupported)
			}
		})
	}
}

func TestNonStreamCancellationAndTiming(t *testing.T) {
	for _, stage := range []string{"before opener", "during opener", "ready JSON", "Write"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				cause := errors.New("request stopped")
				if stage == "before opener" {
					cancel(cause)
				}
				w := newOutput()
				if stage == "Write" {
					w.beforeWrite = func() { cancel(cause) }
				}
				calls := 0
				var group errgroup.Group
				group.Go(func() error {
					result, err := proxy.NewForwarder().ForwardNonStream(ctx, w, func(attempt context.Context) (*provider.ChatResponse, *provider.Usage, error) {
						calls++
						if stage == "during opener" {
							<-attempt.Done()
							return nil, nil, io.ErrUnexpectedEOF
						}
						if stage == "ready JSON" {
							cancel(cause)
						}
						return &provider.ChatResponse{Body: []byte(`{}`)}, nil, nil
					}, options)
					require.ErrorIs(t, err, context.Canceled)
					require.ErrorIs(t, err, cause)
					require.Equal(t, proxy.ResultClientCanceled, result.Kind)
					if stage == "Write" {
						require.True(t, result.WroteHeader)
					} else {
						assertUncommitted(t, w, result)
					}
					return nil
				})
				synctest.Wait()
				cancel(cause)
				require.NoError(t, group.Wait())
				if stage == "before opener" {
					require.Zero(t, calls)
				} else {
					require.Equal(t, 1, calls)
				}
			})
		})
	}
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		w := newOutput()
		result, err := proxy.NewForwarder().ForwardNonStream(ctx, w, func(attempt context.Context) (*provider.ChatResponse, *provider.Usage, error) {
			<-attempt.Done()
			return nil, nil, io.EOF
		}, options)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, proxy.ResultOverallTimeout, result.Kind)
		assertUncommitted(t, w, result)
	})
	synctest.Test(t, func(t *testing.T) {
		w := newOutput()
		w.beforeWrite = func() { time.Sleep(100 * time.Millisecond) }
		result, err := proxy.NewForwarder().ForwardNonStream(t.Context(), w, func(context.Context) (*provider.ChatResponse, *provider.Usage, error) {
			time.Sleep(40 * time.Millisecond)
			return &provider.ChatResponse{Body: []byte(`{}`)}, nil, nil
		}, options)
		require.NoError(t, err)
		require.EqualValues(t, 40, result.FirstByteMs)
	})
}
