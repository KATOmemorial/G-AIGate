package proxy_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

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
