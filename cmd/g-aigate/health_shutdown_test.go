package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/channel"
	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

type shutdownHealthRepo struct {
	store.ChannelRepo
	list func(context.Context) ([]store.Channel, error)
}

func (r *shutdownHealthRepo) List(ctx context.Context) ([]store.Channel, error) { return r.list(ctx) }

type shutdownHealthKeys struct{ store.KeyRepo }
type shutdownHealthState struct{ store.VersionedHealthState }
type shutdownHealthTester struct{ channel.ConnectivityTester }

func TestHealthShutdownJoinsOwnersBeforeStoresWithoutResettingDependencyBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, exited := make(chan struct{}), make(chan struct{})
		repo := &shutdownHealthRepo{list: func(ctx context.Context) ([]store.Channel, error) {
			close(entered)
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)
			close(exited)
			return nil, ctx.Err()
		}}
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		health, err := channel.NewHealthService(repo, &shutdownHealthKeys{}, &shutdownHealthState{}, &shutdownHealthTester{},
			channel.HealthOptions{ProbeInterval: time.Second, DisableDuration: time.Minute, ProbeTimeout: time.Second, StateTimeout: 500 * time.Millisecond, FailureThreshold: 5, ProbeConcurrency: 2}, logger)
		require.NoError(t, err)
		require.NoError(t, health.Start(context.Background()))
		<-entered
		var order []string
		start := time.Now()
		storeClose := func(name string) func(context.Context) error {
			return func(ctx context.Context) error {
				order = append(order, name)
				select {
				case <-exited:
				default:
					t.Fatal("health owner accessed resources after store close")
				}
				deadline, _ := ctx.Deadline()
				require.Equal(t, start.Add(10*time.Millisecond), deadline)
				require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded, "do not restart dependency budget after health join")
				return nil
			}
		}
		deps := buildDeps(storeClose("mysql"), storeClose("redis"), storeClose("usage"), func(ctx context.Context) error {
			order = append(order, "health")
			err := health.Close(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			return err
		})
		closeDependencies(10*time.Millisecond, deps, logger)
		require.Equal(t, 20*time.Millisecond, time.Since(start))
		require.Equal(t, []string{"health", "usage", "redis", "mysql"}, order)
		require.NoError(t, health.Close(context.Background()))
	})
}

func TestShutdownTimeoutCancelsRealHTTPRequestBeforeHealthTeardown(t *testing.T) {
	entered, canceled := make(chan struct{}), make(chan struct{})
	srv, base := startTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(canceled) }))
	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(base)
		if resp != nil {
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()
	<-entered
	deps := buildDeps(func(context.Context) error { return nil }, func(context.Context) error { return nil }, func(context.Context) error { return nil }, func(ctx context.Context) error {
		// HTTP and dependencies retain independent budgets. The fresh dependency
		// deadline does not allow the old HTTP connection to stay alive.
		require.NoError(t, ctx.Err())
		select {
		case <-canceled:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("HTTP request was not canceled before resource teardown")
		}
		return nil
	})
	shutdown(30*time.Millisecond, srv, deps, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	require.Error(t, <-requestDone)
}
