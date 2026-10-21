package proxy

import (
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAttributionControlledOrderingAndDeadlineProvenance(t *testing.T) {
	for _, scenario := range []string{
		"parent confirmed first", "Overall confirmed first", "simultaneous without write evidence",
		"socket timeout before budget callback", "independent parent cause", "future deadline not effective",
		"setter failed", "different deadline", "parent shortened deadline", "non timeout write error",
		"parent deadline earlier", "parent deadline equal", "parent deadline already canceled by socket",
	} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				start := time.Now()
				parent, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				custom := errors.New("independent parent reason")
				want := ResultOverallTimeout
				if scenario == "parent deadline earlier" || scenario == "parent deadline equal" || scenario == "parent deadline already canceled by socket" {
					delay := 100 * time.Millisecond
					if scenario == "parent deadline earlier" {
						delay = 50 * time.Millisecond
					}
					var stop context.CancelFunc
					parent, stop = context.WithDeadlineCause(parent, start.Add(delay), custom)
					defer stop()
				}
				// Intentionally no budget timer: force the OS deadline to return
				// before its callback. All synchronization/decisions are real code.
				attempt, stopAttempt := context.WithCancel(parent)
				defer stopAttempt()
				life := &lifetime{parent: parent, ctx: attempt, started: start, overall: start.Add(100 * time.Millisecond), ttfb: start.Add(time.Second), first: true}
				d := &downstream{deadline: life.overall, installed: true, fromOverall: true}
				outputErr := error(os.ErrDeadlineExceeded)
				if scenario == "parent confirmed first" || scenario == "parent shortened deadline" {
					cancel(nil)
					if scenario == "parent confirmed first" {
						kind, err := life.check()
						require.Equal(t, ResultClientCanceled, kind)
						require.ErrorIs(t, err, context.Canceled)
					} else {
						deadline := life.wakeDeadline(d)
						require.True(t, deadline.Before(life.overall))
						require.True(t, d.shortened)
					}
					want = ResultClientCanceled
				}
				if scenario == "parent deadline already canceled by socket" {
					cancel(nil)
				}
				if scenario == "future deadline not effective" {
					time.Sleep(50 * time.Millisecond)
				} else {
					time.Sleep(100 * time.Millisecond)
				}
				if scenario == "parent deadline earlier" || scenario == "parent deadline equal" {
					<-parent.Done()
				}
				switch scenario {
				case "Overall confirmed first":
					kind, err := life.check()
					require.Equal(t, ResultOverallTimeout, kind)
					require.ErrorIs(t, err, context.DeadlineExceeded)
					cancel(custom) // A later independent cause remains in the chain, not Kind.
				case "independent parent cause":
					cancel(custom)
				default:
					cancel(nil)
				}
				switch scenario {
				case "simultaneous without write evidence", "non timeout write error":
					outputErr = io.ErrClosedPipe
					want = ResultClientCanceled
				case "independent parent cause", "future deadline not effective":
					want = ResultClientCanceled
				case "setter failed":
					d.installed = false
					want = ResultClientCanceled
				case "different deadline":
					d.fromOverall = false
					want = ResultClientCanceled
				}
				life.outputFailed(d, outputErr)
				kind, err := life.check()
				require.Equal(t, want, kind)
				require.ErrorIs(t, err, parent.Err(), "retain the actual parent error, including deadline or ordinary Canceled")
				if want == ResultOverallTimeout {
					require.ErrorIs(t, err, context.DeadlineExceeded)
				}
				if scenario == "Overall confirmed first" || scenario == "independent parent cause" || scenario == "parent deadline earlier" || scenario == "parent deadline equal" {
					require.ErrorIs(t, err, custom)
				}
				// Cleanup sampling later cannot rewrite an already determined cause.
				time.Sleep(time.Second)
				kindAfter, errAfter := life.check()
				require.Equal(t, kind, kindAfter)
				require.ErrorIs(t, errAfter, context.Cause(parent))
			})
		})
	}
}

func TestBudgetWakeDeterminesOverallBeforeInterruptingOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		parent, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		ctx, stop := context.WithDeadline(parent, start.Add(100*time.Millisecond))
		defer stop()
		life := &lifetime{parent: parent, ctx: ctx, started: start, overall: start.Add(100 * time.Millisecond), first: true}
		d := &downstream{deadline: life.overall, installed: true, fromOverall: true}
		<-ctx.Done()
		require.Equal(t, life.overall, life.wakeDeadline(d))
		require.False(t, d.shortened)
		custom := errors.New("parent arrives after budget wake")
		cancel(custom)
		kind, err := life.check()
		require.Equal(t, ResultOverallTimeout, kind)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, err, custom)
	})
}
