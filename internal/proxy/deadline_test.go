package proxy_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/proxy"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// A bounded, interruptible writer with sticky expiry, explicit entry/exit and
// deadline-change barriers. Unlike Recorder it actually blocks Write/Flush.
type deadlineOutput struct {
	*outputWriter
	mu                  sync.Mutex
	deadline            time.Time
	sets                []time.Time
	changed             chan struct{}
	sticky              bool
	block               string
	entered, exited     chan struct{}
	enterOnce, exitOnce sync.Once
	afterSet            func(time.Time, int) error
}

func newDeadlineOutput(block string) *deadlineOutput {
	return &deadlineOutput{outputWriter: newOutput(), changed: make(chan struct{}), block: block,
		entered: make(chan struct{}), exited: make(chan struct{})}
}

func (w *deadlineOutput) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	w.deadline = deadline
	w.sets = append(w.sets, deadline)
	n := len(w.sets)
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		w.sticky = true
	}
	close(w.changed)
	w.changed = make(chan struct{})
	w.mu.Unlock()
	if w.afterSet != nil {
		return w.afterSet(deadline, n)
	}
	return nil
}

func (w *deadlineOutput) deadlines() []time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Time(nil), w.sets...)
}

func (w *deadlineOutput) outputError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.deadline.IsZero() && !time.Now().Before(w.deadline) {
		w.sticky = true
	}
	if w.sticky {
		return os.ErrDeadlineExceeded
	}
	return nil
}

func (w *deadlineOutput) wait() error {
	w.enterOnce.Do(func() { close(w.entered) })
	defer w.exitOnce.Do(func() { close(w.exited) })
	for {
		if err := w.outputError(); err != nil {
			return err
		}
		w.mu.Lock()
		deadline, changed := w.deadline, w.changed
		w.mu.Unlock()
		if deadline.IsZero() {
			<-changed
			continue
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-timer.C:
		case <-changed:
		}
		timer.Stop()
	}
}

func (w *deadlineOutput) Write(p []byte) (int, error) {
	if w.block == "Write" {
		w.writes++
		return 0, w.wait()
	}
	if err := w.outputError(); err != nil {
		return 0, err
	}
	return w.outputWriter.Write(p)
}

func (w *deadlineOutput) FlushError() error {
	if w.block == "Flush" {
		w.flushes++
		return w.wait()
	}
	if err := w.outputError(); err != nil {
		return err
	}
	return w.outputWriter.FlushError()
}

func TestControlledBlockedOutputCancellationAndDeadline(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, stage := range []string{"Write", "Flush"} {
			for _, reason := range []string{"Overall", "parent cancel", "parent deadline"} {
				t.Run(fmt.Sprintf("stream=%t/%s/%s", streaming, stage, reason), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancelCause(t.Context())
						defer cancel(nil)
						cause := errors.New("client stopped")
						if reason == "parent deadline" {
							var stop context.CancelFunc
							ctx, stop = context.WithTimeoutCause(ctx, 60*time.Millisecond, cause)
							defer stop()
						}
						w := newDeadlineOutput(stage)
						opts := proxy.ForwardOptions{TTFBTimeout: 20 * time.Millisecond, OverallTimeout: 100 * time.Millisecond}
						var result proxy.ForwardResult
						var forwardErr error
						var group errgroup.Group
						start := time.Now()
						group.Go(func() error {
							result, forwardErr = forwardMode(ctx, w, streaming, opts, func(context.Context) error { return nil })
							return nil
						})
						<-w.entered
						synctest.Wait() // The selected Write/Flush is now durably blocked.
						trigger := start.Add(opts.OverallTimeout)
						if reason == "parent deadline" {
							trigger = start.Add(60 * time.Millisecond)
						}
						if reason == "parent cancel" {
							trigger = time.Now()
							cancel(cause)
						}
						require.NoError(t, group.Wait())
						elapsed := time.Since(trigger)
						require.LessOrEqual(t, elapsed, 500*time.Millisecond)
						require.ErrorIs(t, forwardErr, os.ErrDeadlineExceeded)
						if reason == "parent cancel" {
							require.Equal(t, proxy.ResultClientCanceled, result.Kind)
							require.ErrorIs(t, forwardErr, context.Canceled)
						} else {
							require.Equal(t, proxy.ResultOverallTimeout, result.Kind)
							require.ErrorIs(t, forwardErr, context.DeadlineExceeded)
						}
						if reason != "Overall" {
							require.ErrorIs(t, forwardErr, cause)
						}
						require.True(t, result.WroteHeader)
						require.EqualValues(t, 0, result.FirstByteMs)
						require.Zero(t, result.Chunks)
						select {
						case <-w.exited:
						default:
							t.Fatal("output still active after return")
						}
						calls := w.deadlines()
						require.True(t, calls[0].IsZero(), "non-expiring capability probe")
						wantDeadline := start.Add(opts.OverallTimeout)
						if reason == "parent deadline" {
							wantDeadline = trigger
						}
						require.Equal(t, wantDeadline, calls[1], "write deadline is independent of TTFB")
						for _, deadline := range calls[2:] {
							require.False(t, deadline.IsZero(), "expired response must never be cleared")
						}
						cancel(cause)
						time.Sleep(time.Second)
						require.Equal(t, calls, w.deadlines(), "no watcher may access writer after return")
						t.Logf("TTFB=%s Overall=%s stage=%s trigger=%s exit latency=%s (target 100ms, limit 500ms)", opts.TTFBTimeout, opts.OverallTimeout, stage, reason, elapsed)
					})
				})
			}
		}
	}
}

func TestWatcherJoinedAndCleanupCannotExtendCanceledResponse(t *testing.T) {
	for _, scenario := range []string{"wake in progress", "cancel during clear", "deadline during clear", "normal return"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				w := newDeadlineOutput("")
				if scenario == "wake in progress" {
					w.block = "Write"
				}
				wakeEntered, release := make(chan struct{}), make(chan struct{})
				w.afterSet = func(deadline time.Time, call int) error {
					if scenario == "wake in progress" && !deadline.IsZero() && !time.Now().Before(deadline) {
						close(wakeEntered)
						<-release // Write can already exit; callback still owns the writer.
					}
					if call == 3 && deadline.IsZero() {
						if scenario == "cancel during clear" {
							cancel()
						}
						if scenario == "deadline during clear" {
							time.Sleep(100 * time.Millisecond)
						}
					}
					return nil
				}
				var returned atomic.Bool
				var result proxy.ForwardResult
				var forwardErr error
				var group errgroup.Group
				group.Go(func() error {
					result, forwardErr = forwardMode(ctx, w, false, proxy.ForwardOptions{TTFBTimeout: 20 * time.Millisecond, OverallTimeout: 100 * time.Millisecond}, func(context.Context) error { return nil })
					returned.Store(true)
					return nil
				})
				if scenario == "wake in progress" {
					<-w.entered
					cancel()
					<-wakeEntered
					<-w.exited
					synctest.Wait()
					require.False(t, returned.Load(), "must join a running deadline callback")
					close(release)
				}
				require.NoError(t, group.Wait())
				calls := w.deadlines()
				if scenario == "normal return" {
					require.NoError(t, forwardErr)
					require.Equal(t, proxy.ResultSuccess, result.Kind)
					require.True(t, calls[len(calls)-1].IsZero())
				} else {
					require.Error(t, forwardErr)
					require.False(t, calls[len(calls)-1].IsZero(), "cleanup must restore interruption when cancellation crosses clearing")
				}
				cancel()
				time.Sleep(time.Second)
				require.Equal(t, calls, w.deadlines(), "normal and failed returns stop all writer callbacks")
			})
		})
	}
}

type flushWithoutDeadline struct{ http.ResponseWriter }

func (*flushWithoutDeadline) FlushError() error { return nil }

func TestDeadlineCapabilityAndSetterFailures(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, scenario := range []string{"missing", "probe unsupported", "arm unsupported", "cleanup failure", "wake failure"} {
			t.Run(fmt.Sprintf("stream=%t/%s", streaming, scenario), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					w := newDeadlineOutput("")
					cause := errors.New("deadline setter failed")
					var writer http.ResponseWriter = w
					if scenario == "missing" {
						writer = &flushWithoutDeadline{ResponseWriter: w}
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					w.afterSet = func(deadline time.Time, n int) error {
						if scenario == "probe unsupported" && n == 1 || scenario == "arm unsupported" && n == 2 {
							return http.ErrNotSupported
						}
						if scenario == "cleanup failure" && n == 3 || scenario == "wake failure" && !deadline.IsZero() && !time.Now().Before(deadline) {
							return cause
						}
						return nil
					}
					if scenario == "wake failure" {
						w.beforeWrite = func() { cancel(); synctest.Wait() }
					}
					opened := 0
					result, err := forwardMode(ctx, writer, streaming, options, func(context.Context) error { opened++; return nil })
					require.Error(t, err)
					if scenario == "missing" || scenario == "probe unsupported" {
						require.Zero(t, opened)
						assertUncommitted(t, w.outputWriter, result)
					} else {
						require.Equal(t, 1, opened)
					}
					if scenario == "missing" || scenario == "probe unsupported" || scenario == "arm unsupported" {
						require.ErrorIs(t, err, http.ErrNotSupported)
						require.False(t, result.WroteHeader)
						require.Zero(t, w.writes)
						require.Zero(t, w.flushes)
					} else {
						require.ErrorIs(t, err, cause)
					}
				})
			})
		}
	}
}
