package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Pause only after the real server has installed its write deadline. Nothing
// writes or flushes until Forward returns and the handler attempts its 502.
type armBoundaryWriter struct {
	http.ResponseWriter
	armed                    chan time.Time
	release                  <-chan struct{}
	deadlines                []time.Time
	writes, flushes, headers int
}

func (w *armBoundaryWriter) SetWriteDeadline(deadline time.Time) error {
	err := http.NewResponseController(w.ResponseWriter).SetWriteDeadline(deadline)
	w.deadlines = append(w.deadlines, deadline)
	if len(w.deadlines) == 2 && err == nil {
		w.armed <- deadline
		<-w.release
	}
	return err
}
func (w *armBoundaryWriter) WriteHeader(status int) {
	w.headers++
	w.ResponseWriter.WriteHeader(status)
}
func (w *armBoundaryWriter) Write(p []byte) (int, error) {
	w.writes++
	return w.ResponseWriter.Write(p)
}
func (w *armBoundaryWriter) FlushError() error {
	w.flushes++
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func TestPrecommitExitAfterArmingRealDeadlineLeavesErrorReplyWritable(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, chain := range []string{"native", "WrapWriter"} {
			for _, reason := range []string{"Overall", "parent deadline", "parent cancel"} {
				t.Run(fmt.Sprintf("stream=%t/%s/%s", streaming, chain, reason), func(t *testing.T) {
					type outcome struct {
						result                                             proxy.ForwardResult
						err, writeErr, flushErr                            error
						deadlines                                          []time.Time
						beforeWrites, beforeFlushes, beforeHeaders, closes int
						ginWrittenBefore                                   bool
						ginSizeBefore                                      int
					}
					armed, release := make(chan time.Time, 1), make(chan struct{})
					var releaseOnce sync.Once
					unblock := func() { releaseOnce.Do(func() { close(release) }) }
					finished := make(chan outcome, 1)
					cancelReady := make(chan context.CancelCauseFunc, 1)
					cause := errors.New("precommit parent canceled")
					run := func(w http.ResponseWriter, r *http.Request, boundary *armBoundaryWriter) {
						ctx, cancel := context.WithCancelCause(r.Context())
						defer cancel(nil)
						if reason == "parent deadline" {
							var stop context.CancelFunc
							ctx, stop = context.WithTimeout(ctx, 40*time.Millisecond)
							defer stop()
						}
						cancelReady <- cancel
						opts := proxy.ForwardOptions{TTFBTimeout: time.Second, OverallTimeout: 80 * time.Millisecond}
						if reason == "parent cancel" {
							opts.OverallTimeout = time.Second
						}
						got := outcome{}
						s := scripted(done())
						if streaming {
							got.result, got.err = proxy.NewForwarder().Forward(ctx, w, s.open, opts, nil)
							got.closes = s.closes
						} else {
							got.result, got.err = proxy.NewForwarder().ForwardNonStream(ctx, w, func(context.Context) (*provider.ChatResponse, *provider.Usage, error) {
								return &provider.ChatResponse{Body: []byte(`{}`)}, nil, nil
							}, opts)
						}
						got.beforeWrites, got.beforeFlushes, got.beforeHeaders = boundary.writes, boundary.flushes, boundary.headers
						got.deadlines = append([]time.Time(nil), boundary.deadlines...)
						if tracked, ok := w.(gin.ResponseWriter); ok {
							got.ginWrittenBefore, got.ginSizeBefore = tracked.Written(), tracked.Size()
						}
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(502)
						_, got.writeErr = io.WriteString(w, `{"error":"timeout"}`)
						got.flushErr = boundary.FlushError()
						finished <- got
					}
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						boundary := &armBoundaryWriter{ResponseWriter: w, armed: armed, release: release}
						if chain == "native" {
							run(boundary, r, boundary)
							return
						}
						router := gin.New()
						router.GET("/", func(c *gin.Context) { c.Writer = middleware.WrapWriter(c.Writer); run(c.Writer, c.Request, boundary) })
						router.ServeHTTP(boundary, r)
					}))
					t.Cleanup(server.Close)
					t.Cleanup(unblock)
					client := server.Client()
					client.Timeout = 2 * time.Second
					var requests errgroup.Group
					var response *http.Response
					var body []byte
					requests.Go(func() error {
						var err error
						response, err = client.Get(server.URL)
						if err != nil {
							return err
						}
						defer response.Body.Close()
						body, err = io.ReadAll(response.Body)
						return err
					})
					var deadline time.Time
					select {
					case deadline = <-armed:
					case <-time.After(time.Second):
						t.Fatal("real deadline was not installed")
					}
					cancel := <-cancelReady
					if reason == "parent cancel" {
						cancel(cause)
					} else {
						<-time.After(time.Until(deadline) + 5*time.Millisecond)
					}
					unblock() // Resume the exact arm/commit boundary after cancellation/expiry.
					got := <-finished
					require.NoError(t, requests.Wait(), "handler must produce a real response, not client EOF")
					require.NoError(t, got.writeErr)
					require.NoError(t, got.flushErr)
					require.Equal(t, 502, response.StatusCode)
					require.JSONEq(t, `{"error":"timeout"}`, string(body))
					require.False(t, got.result.WroteHeader)
					require.GreaterOrEqual(t, got.result.FirstByteMs, int64(0), "payload was ready before arming")
					require.Zero(t, got.beforeWrites)
					require.Zero(t, got.beforeFlushes)
					require.Zero(t, got.beforeHeaders)
					require.Len(t, got.deadlines, 3, "probe, arm, then precommit clear")
					require.True(t, got.deadlines[2].IsZero())
					if streaming {
						require.Equal(t, 1, got.closes)
					}
					if chain == "WrapWriter" {
						require.False(t, got.ginWrittenBefore)
						require.Equal(t, -1, got.ginSizeBefore)
					}
					if reason == "parent cancel" {
						require.Equal(t, proxy.ResultClientCanceled, got.result.Kind)
						require.ErrorIs(t, got.err, cause)
					} else {
						require.Equal(t, proxy.ResultOverallTimeout, got.result.Kind)
						require.ErrorIs(t, got.err, context.DeadlineExceeded)
					}
				})
			}
		}
	}
}

func TestPrecommitBudgetLeavesRealHTTPErrorReplyWritable(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, overallFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/overallFirst=%t", streaming, overallFirst), func(t *testing.T) {
				type outcome struct {
					result                  proxy.ForwardResult
					err, writeErr, flushErr error
				}
				finished := make(chan outcome, 1)
				opts := proxy.ForwardOptions{TTFBTimeout: 40 * time.Millisecond, OverallTimeout: time.Second}
				want := proxy.ResultTTFBTimeout
				if overallFirst {
					opts.OverallTimeout = 20 * time.Millisecond
					want = proxy.ResultOverallTimeout
				}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					result, err := forwardMode(r.Context(), w, streaming, opts, func(ctx context.Context) error { <-ctx.Done(); return io.ErrClosedPipe })
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(502)
					_, writeErr := io.WriteString(w, `{"error":"timeout"}`)
					flushErr := http.NewResponseController(w).Flush()
					finished <- outcome{result, err, writeErr, flushErr}
				}))
				defer server.Close()
				response, err := server.Client().Get(server.URL)
				require.NoError(t, err)
				defer response.Body.Close()
				body, err := io.ReadAll(response.Body)
				require.NoError(t, err)
				require.Equal(t, 502, response.StatusCode)
				require.JSONEq(t, `{"error":"timeout"}`, string(body))
				got := <-finished
				require.ErrorIs(t, got.err, context.DeadlineExceeded)
				require.Equal(t, want, got.result.Kind)
				require.False(t, got.result.WroteHeader)
				require.EqualValues(t, -1, got.result.FirstByteMs)
				require.NoError(t, got.writeErr)
				require.NoError(t, got.flushErr)
			})
		}
	}
}

func TestActualProviderBudgetIncludesHeadersBodyAndHeartbeats(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, stage := range []string{"response headers", "body", "Overall before headers"} {
			t.Run(fmt.Sprintf("stream=%t/%s", streaming, stage), func(t *testing.T) {
				entered, exited, readEntered := make(chan struct{}), make(chan struct{}), make(chan struct{})
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(exited)
					_, _ = io.Copy(io.Discard, r.Body)
					if stage == "body" {
						if streaming {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, ": heartbeat\n\n")
						} else {
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"partial":`)
						}
						w.(http.Flusher).Flush()
					}
					close(entered)
					if streaming && stage == "body" {
						ticker := time.NewTicker(10 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-r.Context().Done():
								return
							case <-ticker.C:
								_, _ = io.WriteString(w, ": heartbeat\n\n")
								w.(http.Flusher).Flush()
							}
						}
					}
					<-r.Context().Done()
				}))
				defer upstream.Close()
				p, closes := actualAdapterWithReadBarrier(t, upstream.Client(), readEntered)
				ep := &provider.Endpoint{BaseURL: upstream.URL, Model: "m"}
				opts := proxy.ForwardOptions{TTFBTimeout: 150 * time.Millisecond, OverallTimeout: time.Second}
				want := proxy.ResultTTFBTimeout
				budget := opts.TTFBTimeout
				if stage == "Overall before headers" {
					opts.OverallTimeout = 100 * time.Millisecond
					budget = opts.OverallTimeout
					want = proxy.ResultOverallTimeout
				}
				w := newDeadlineOutput("")
				var group errgroup.Group
				var result proxy.ForwardResult
				var forwardErr error
				started := time.Now()
				group.Go(func() error {
					if streaming {
						result, forwardErr = proxy.NewForwarder().Forward(t.Context(), w, func(ctx context.Context) (provider.Stream, error) { return p.ChatStream(ctx, ep, upstreamRequest()) }, opts, nil)
					} else {
						result, forwardErr = proxy.NewForwarder().ForwardNonStream(t.Context(), w, func(ctx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
							return p.Chat(ctx, ep, upstreamRequest())
						}, opts)
					}
					return nil
				})
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("upstream never entered target stage")
				}
				if stage == "body" {
					select {
					case <-readEntered:
					case <-time.After(time.Second):
						t.Fatal("client never entered body read")
					}
				}
				require.NoError(t, group.Wait())
				select {
				case <-exited:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("upstream did not exit after budget cancellation")
				}
				elapsed := time.Since(started)
				require.GreaterOrEqual(t, elapsed, budget)
				require.Less(t, elapsed-budget, 500*time.Millisecond, "budget trigger to full resource exit")
				require.ErrorIs(t, forwardErr, context.DeadlineExceeded)
				require.Equal(t, want, result.Kind)
				assertUncommitted(t, w.outputWriter, result)
				if stage == "body" {
					require.EqualValues(t, 1, closes.Load())
				}
				for _, deadline := range w.deadlines() {
					require.True(t, deadline.IsZero())
				}
				t.Logf("TTFB=%s Overall=%s stage=%s actual=%s trigger-to-exit=%s (target 100ms, limit 500ms)", opts.TTFBTimeout, opts.OverallTimeout, stage, elapsed, elapsed-budget)
			})
		}
	}
}
