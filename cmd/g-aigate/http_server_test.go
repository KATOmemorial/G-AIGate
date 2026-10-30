package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/channel"
	"gaigate/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestHTTPHandlerOwnersJoinAllRequestsAndRejectLateStarts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		normal, abort := make(chan struct{}), make(chan struct{})
		entered, ended := make(chan struct{}, 2), make(chan any, 2)
		server := newHTTPServer("", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			if r.URL.Path == "/abort" {
				<-abort
				panic(http.ErrAbortHandler)
			}
			<-normal
		}))
		for _, path := range []string{"/normal", "/abort"} {
			go func() {
				defer func() { ended <- recover() }()
				server.Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
			}()
		}
		<-entered
		<-entered
		joined := make(chan struct{})
		go func() { server.handlers.wait(); close(joined) }()
		synctest.Wait()
		require.PanicsWithValue(t, http.ErrAbortHandler, func() {
			server.Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/late", nil))
		})
		require.Empty(t, entered, "late requests cannot acquire dependencies after sealing admission")
		close(normal)
		require.Nil(t, <-ended)
		synctest.Wait()
		select {
		case <-joined:
			t.Fatal("panic-path owner still active")
		default:
		}
		close(abort)
		require.Equal(t, http.ErrAbortHandler, <-ended)
		<-joined
		server.handlers.wait() // repeated joins remain safe, without a waiter goroutine
	})
}

func TestHTTPHandlerJoinConsumesOriginalDependencyBudget(t *testing.T) {
	for _, cleanup := range []time.Duration{5 * time.Millisecond, 20 * time.Millisecond} {
		t.Run(cleanup.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, exited := make(chan struct{}), make(chan struct{})
				server := newHTTPServer("", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					close(entered)
					time.Sleep(cleanup) // deterministic virtual-time bounded cleanup
					close(exited)
				}))
				go server.Handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
				<-entered
				start := time.Now()
				var order []string
				closer := func(name string) func(context.Context) error {
					return func(ctx context.Context) error {
						select {
						case <-exited:
						default:
							t.Fatal("dependency closed before whole handler cleanup")
						}
						deadline, ok := ctx.Deadline()
						require.True(t, ok)
						require.Equal(t, start.Add(10*time.Millisecond), deadline, "join and all closers share one deadline")
						if cleanup > 10*time.Millisecond {
							require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
						} else {
							require.NoError(t, ctx.Err())
						}
						order = append(order, name)
						return nil
					}
				}
				shutdown(10*time.Millisecond, server, buildDeps(closer("mysql"), closer("redis"), closer("usage"), closer("health")), slog.New(slog.NewJSONHandler(io.Discard, nil)))
				require.Equal(t, cleanup, time.Since(start), "expired deadline cannot detach an active handler or restart a timeout")
				require.Equal(t, []string{"health", "usage", "redis", "mysql"}, order)
			})
		})
	}
}

type httpShutdownRepo struct{ shutdownHealthRepo }

func (*httpShutdownRepo) UpdateHealthView(context.Context, int64, string, *time.Time) error {
	return nil
}

func TestShutdownJoinsCanceledHTTPHandlerBeforeHealthAndTerminalReport(t *testing.T) {
	mr := miniredis.RunT(t)
	client := store.NewRedis(store.RedisOptions{Addr: mr.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	state := store.NewVersionedHealthState(client)
	logs := newGatewayLogs()
	logger := newLogger(logs, "info")
	repo := &httpShutdownRepo{shutdownHealthRepo{list: func(context.Context) ([]store.Channel, error) { return nil, nil }}}
	health, err := channel.NewHealthService(repo, &shutdownHealthKeys{}, state, &shutdownHealthTester{}, channel.HealthOptions{
		ProbeInterval: time.Hour, DisableDuration: time.Minute, ProbeTimeout: time.Second, StateTimeout: 100 * time.Millisecond, FailureThreshold: 5, ProbeConcurrency: 2,
	}, logger)
	require.NoError(t, err)
	require.NoError(t, health.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, health.Close(context.Background())) })
	entered, canceled, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	result := make(chan error, 1)
	server, base := startTestServer(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		defer close(exited)
		close(entered)
		<-r.Context().Done()
		close(canceled)
		select {
		case <-release:
		case <-time.After(time.Second):
			result <- errors.New("controlled cleanup was not released")
			return
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 100*time.Millisecond)
		defer cancel()
		_, readErr := health.Read(ctx, 1)
		health.ReportRequestResult(ctx, 1, false)
		result <- readErr
	}))
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		resp, _ := (&http.Client{Timeout: 2 * time.Second}).Get(base)
		if resp != nil {
			_ = resp.Body.Close()
		}
	}()
	<-entered
	var order []string
	closer := func(name string, closeFn func(context.Context) error) func(context.Context) error {
		return func(ctx context.Context) error {
			select {
			case <-exited:
			default:
				t.Error("dependency teardown began before HTTP handler completed cleanup/report")
			}
			order = append(order, name)
			return closeFn(ctx)
		}
	}
	noop := func(context.Context) error { return nil }
	deps := buildDeps(closer("mysql", noop), closer("redis", noop), closer("usage", noop), closer("health", health.Close))
	shutdownDone := make(chan struct{})
	go func() { shutdown(300*time.Millisecond, server, deps, logger); close(shutdownDone) }()
	t.Cleanup(func() {
		_ = server.Close()
		releaseOnce.Do(func() { close(release) })
		<-shutdownDone
		<-requestDone
	})
	<-canceled
	waitProcessLog(t, logs, 0, func(entry map[string]any) bool { return entry["msg"] == "http_handlers_draining" })
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while bounded handler cleanup was held")
	default:
	}
	releaseOnce.Do(func() { close(release) })
	<-shutdownDone
	<-requestDone
	require.NoError(t, <-result, "real HealthService must remain usable during canceled handler cleanup")
	record, err := state.LoadVersioned(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, store.HealthRecord{Snapshot: store.HealthSnapshot{ConsecutiveFailures: 1}, Version: 1}, record, "terminal synchronous report must commit before health Close")
	require.Equal(t, []string{"health", "usage", "redis", "mysql"}, order)
	stopped, healthStopped := -1, -1
	for i, entry := range processLogEntries(t, logs) {
		if entry["msg"] == "http_handlers_stopped" {
			stopped = i
		}
		if entry["msg"] == "health_service_stopped" {
			healthStopped = i
		}
	}
	require.GreaterOrEqual(t, stopped, 0)
	require.Greater(t, healthStopped, stopped)
}
