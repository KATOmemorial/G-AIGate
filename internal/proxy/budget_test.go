package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func forwardMode(ctx context.Context, w http.ResponseWriter, streaming bool, opts proxy.ForwardOptions,
	open func(context.Context) error) (proxy.ForwardResult, error) {
	f := proxy.NewForwarder()
	if streaming {
		return f.Forward(ctx, w, func(attempt context.Context) (provider.Stream, error) {
			if err := open(attempt); err != nil {
				return nil, err
			}
			return scripted(done()), nil
		}, opts, nil)
	}
	return f.ForwardNonStream(ctx, w, func(attempt context.Context) (*provider.ChatResponse, *provider.Usage, error) {
		if err := open(attempt); err != nil {
			return nil, nil, err
		}
		return &provider.ChatResponse{Body: []byte(`{}`)}, nil, nil
	}, opts)
}

func TestBudgetsCoverOpenerAndPreservePriority(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, scenario := range []string{"TTFB", "Overall before TTFB", "equal budgets", "parent deadline", "parent cancel beats budgets", "ready at deadline", "upstream error at deadline"} {
			t.Run(fmt.Sprintf("stream=%t/%s", streaming, scenario), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancelCause(t.Context())
					defer cancel(nil)
					cause := errors.New("parent cause")
					readErr := errors.New("derived read failure")
					opts := proxy.ForwardOptions{TTFBTimeout: 80 * time.Millisecond, OverallTimeout: 200 * time.Millisecond}
					want := proxy.ResultTTFBTimeout
					if scenario == "Overall before TTFB" {
						opts.OverallTimeout = 40 * time.Millisecond
						want = proxy.ResultOverallTimeout
					}
					if scenario == "equal budgets" || scenario == "parent cancel beats budgets" {
						opts.OverallTimeout = opts.TTFBTimeout
						want = proxy.ResultOverallTimeout
					}
					if scenario == "parent cancel beats budgets" {
						want = proxy.ResultClientCanceled
					}
					if scenario == "parent deadline" {
						var stop context.CancelFunc
						ctx, stop = context.WithTimeoutCause(ctx, 30*time.Millisecond, cause)
						defer stop()
						want = proxy.ResultOverallTimeout
					}
					w := newDeadlineOutput("")
					started := time.Now()
					result, err := forwardMode(ctx, w, streaming, opts, func(attempt context.Context) error {
						require.NotSame(t, ctx, attempt)
						if scenario == "ready at deadline" || scenario == "upstream error at deadline" || scenario == "parent cancel beats budgets" {
							time.Sleep(opts.TTFBTimeout)
							if scenario == "parent cancel beats budgets" {
								cancel(cause)
							}
							if scenario == "upstream error at deadline" {
								return readErr
							}
							return nil // Deliberately ready concurrently with the expired budget.
						}
						<-attempt.Done()
						return readErr
					})
					require.Error(t, err)
					require.Equal(t, want, result.Kind)
					if want == proxy.ResultClientCanceled {
						require.ErrorIs(t, err, context.Canceled)
						require.ErrorIs(t, err, cause)
					} else {
						require.ErrorIs(t, err, context.DeadlineExceeded)
					}
					if scenario == "parent deadline" {
						require.ErrorIs(t, err, cause)
					}
					if scenario == "upstream error at deadline" {
						require.ErrorIs(t, err, readErr)
					}
					assertUncommitted(t, w.outputWriter, result)
					for _, deadline := range w.deadlines() {
						require.True(t, deadline.IsZero(), "waiting for opener must never expire downstream")
					}
					// This writer models sticky expired writes; one handler error is still writable.
					w.WriteHeader(502)
					_, writeErr := w.Write([]byte(`{"error":"timeout"}`))
					require.NoError(t, writeErr)
					require.NoError(t, w.FlushError())
					t.Logf("TTFB=%s Overall=%s actual=%s stage=opener result=%s", opts.TTFBTimeout, opts.OverallTimeout, time.Since(started), result.Kind)
				})
			})
		}
	}
}

func TestStreamOneClockAndTTFBStopsAtPayload(t *testing.T) {
	for _, scenario := range []string{"sum exceeds TTFB", "slow write after payload", "Overall after payload", "cleanup exceeds Overall"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				opts := proxy.ForwardOptions{TTFBTimeout: 80 * time.Millisecond, OverallTimeout: 240 * time.Millisecond}
				s := &eventStream{events: make(chan provider.StreamEvent)}
				var producer errgroup.Group
				s.onClose = func() {
					require.NoError(t, producer.Wait())
					if scenario == "cleanup exceeds Overall" {
						time.Sleep(240 * time.Millisecond)
					}
				}
				w := newDeadlineOutput("")
				if scenario == "slow write after payload" {
					w.beforeWrite = func() { time.Sleep(70 * time.Millisecond) }
				}
				started := time.Now()
				result, err := proxy.NewForwarder().Forward(t.Context(), w, func(ctx context.Context) (provider.Stream, error) {
					time.Sleep(40 * time.Millisecond)
					producer.Go(func() error {
						defer close(s.events)
						delay := 20 * time.Millisecond
						if scenario == "sum exceeds TTFB" {
							delay = 60 * time.Millisecond
						}
						select {
						case <-time.After(delay):
						case <-ctx.Done():
							return nil
						}
						select {
						case s.events <- chunk(`{}`):
						case <-ctx.Done():
							return nil
						}
						if scenario == "Overall after payload" {
							<-ctx.Done()
							return nil
						}
						select {
						case s.events <- done():
						case <-ctx.Done():
						}
						return nil
					})
					return s, nil
				}, opts, nil)
				if scenario == "sum exceeds TTFB" {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Equal(t, proxy.ResultTTFBTimeout, result.Kind)
					assertUncommitted(t, w.outputWriter, result)
				} else {
					require.True(t, result.WroteHeader)
					require.EqualValues(t, 60, result.FirstByteMs, "opener plus payload wait, once; excludes write and cleanup")
					if scenario == "slow write after payload" {
						require.NoError(t, err)
						require.Equal(t, proxy.ResultSuccess, result.Kind)
						require.Equal(t, 1, result.Chunks)
					} else {
						require.ErrorIs(t, err, context.DeadlineExceeded)
						require.Equal(t, proxy.ResultOverallTimeout, result.Kind)
					}
				}
				require.Equal(t, 1, s.closes)
				t.Logf("TTFB=%s Overall=%s actual=%s stage=%s", opts.TTFBTimeout, opts.OverallTimeout, time.Since(started), scenario)
			})
		})
	}
}

// Events itself is the boundary barrier: a queued event and an elapsed deadline
// are both ready when the forwarder resumes. It must not commit either outcome.
type delayedEvents struct {
	*eventStream
	delay time.Duration
}

func (s *delayedEvents) Events() <-chan provider.StreamEvent { time.Sleep(s.delay); return s.events }

func TestReadyEventCannotCrossExpiredDeadline(t *testing.T) {
	for _, event := range []provider.StreamEvent{chunk(`{}`), done(), {Kind: provider.Error, Err: io.ErrClosedPipe}} {
		t.Run(fmt.Sprint(event.Kind), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				w := newDeadlineOutput("")
				s := &delayedEvents{eventStream: scripted(event), delay: 80 * time.Millisecond}
				result, err := proxy.NewForwarder().Forward(t.Context(), w, func(context.Context) (provider.Stream, error) { return s, nil }, proxy.ForwardOptions{TTFBTimeout: 80 * time.Millisecond, OverallTimeout: time.Second}, nil)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, proxy.ResultTTFBTimeout, result.Kind)
				assertUncommitted(t, w.outputWriter, result)
				require.Equal(t, 1, len(s.events), "deadline wins before consuming a queued event")
				require.Equal(t, 1, s.closes)
			})
		})
	}
}
