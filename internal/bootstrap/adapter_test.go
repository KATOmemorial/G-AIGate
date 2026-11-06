package bootstrap

import (
	"context"
	"errors"
	"gaigate/internal/access"
	"gaigate/internal/provider"
	"gaigate/internal/router"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"
)

func TestAdapterSharesOneBudgetAndUsesAuto(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := &access.Principal{UserID: 7}
		req := testRequest(false)
		plan := &router.Plan{Model: req.Model}
		var routed context.Context
		planner := plannerFunc(func(ctx context.Context, model string, strategy router.RouteStrategy) (*router.Plan, error) {
			routed = ctx
			require.Equal(t, req.Model, model)
			require.Equal(t, router.Auto, strategy)
			time.Sleep(700 * time.Millisecond)
			return plan, nil
		})
		calls := 0
		run := executionFunc(func(ctx context.Context, w http.ResponseWriter, principal *access.Principal, got *router.Plan, r *provider.ChatRequest) error {
			calls++
			require.Same(t, routed, ctx)
			require.Same(t, p, principal)
			require.Same(t, req, r)
			require.Same(t, plan, got)
			deadline, _ := ctx.Deadline()
			require.Equal(t, 300*time.Millisecond, time.Until(deadline))
			<-ctx.Done()
			return &access.ExecutionError{Cause: ctx.Err(), TimedOut: true}
		})
		e, err := NewExecutor(planner, run, time.Second)
		require.NoError(t, err)
		start := time.Now()
		err = e.Execute(context.Background(), httptest.NewRecorder(), p, req)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, 1, calls)
	})
}
func TestAdapterLocalRouteDeadlineAndLogicalDeadlineClassification(t *testing.T) {
	for _, phase := range []string{"local", "logical", "parent-cancel", "parent-deadline"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				custom := errors.New("parent cause")
				var cancel context.CancelFunc
				if phase == "parent-deadline" {
					ctx, cancel = context.WithTimeoutCause(ctx, 100*time.Millisecond, custom)
					defer cancel()
				}
				routed := 0
				calls := 0
				planner := plannerFunc(func(ctx context.Context, _ string, _ router.RouteStrategy) (*router.Plan, error) {
					routed++
					if phase == "local" {
						return nil, context.DeadlineExceeded
					}
					<-ctx.Done()
					return nil, ctx.Err()
				})
				run := executionFunc(func(context.Context, http.ResponseWriter, *access.Principal, *router.Plan, *provider.ChatRequest) error {
					calls++
					return nil
				})
				if phase == "parent-cancel" {
					var stop context.CancelCauseFunc
					ctx, stop = context.WithCancelCause(ctx)
					stop(custom)
				}
				e, err := NewExecutor(planner, run, time.Second)
				require.NoError(t, err)
				err = e.Execute(ctx, httptest.NewRecorder(), &access.Principal{}, testRequest(false))
				require.Error(t, err)
				require.Zero(t, calls)
				var ex *access.ExecutionError
				if phase == "local" {
					require.False(t, errors.As(err, &ex))
					require.ErrorIs(t, err, context.DeadlineExceeded)
				} else {
					require.ErrorAs(t, err, &ex)
					require.Equal(t, phase != "parent-cancel", ex.TimedOut)
				}
				if phase == "parent-cancel" {
					require.Zero(t, routed)
					require.ErrorIs(t, err, custom)
				}
				if phase == "parent-deadline" {
					require.ErrorIs(t, err, custom)
				}
			})
		})
	}
}
func TestAdapterConstructorAndClassifiedErrorIdentity(t *testing.T) {
	planner := plannerFunc(func(context.Context, string, router.RouteStrategy) (*router.Plan, error) { return &router.Plan{}, nil })
	latched := &access.ExecutionError{Cause: errors.Join(context.Canceled, context.DeadlineExceeded), WroteHeader: true, TimedOut: true}
	run := executionFunc(func(context.Context, http.ResponseWriter, *access.Principal, *router.Plan, *provider.ChatRequest) error {
		return latched
	})
	var nilPlanner plannerFunc
	var nilRun executionFunc
	for _, fn := range []func() (access.ChatExecutor, error){func() (access.ChatExecutor, error) { return NewExecutor(nil, run, time.Second) }, func() (access.ChatExecutor, error) { return NewExecutor(nilPlanner, run, time.Second) }, func() (access.ChatExecutor, error) { return NewExecutor(planner, nil, time.Second) }, func() (access.ChatExecutor, error) { return NewExecutor(planner, nilRun, time.Second) }, func() (access.ChatExecutor, error) { return NewExecutor(planner, run, 0) }} {
		e, err := fn()
		require.Nil(t, e)
		require.Error(t, err)
	}
	e, err := NewExecutor(planner, run, time.Second)
	require.NoError(t, err)
	require.Same(t, latched, e.Execute(context.Background(), httptest.NewRecorder(), &access.Principal{}, testRequest(false)))
}
