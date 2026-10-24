package channel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

type healthRepoStub struct {
	store.ChannelRepo
	list   func(context.Context) ([]store.Channel, error)
	get    func(context.Context, int64) (*store.Channel, error)
	models func(context.Context, int64) ([]store.ChannelModel, error)
	view   func(context.Context, int64, string, *time.Time) error
}

func (r *healthRepoStub) List(ctx context.Context) ([]store.Channel, error) { return r.list(ctx) }
func (r *healthRepoStub) Get(ctx context.Context, id int64) (*store.Channel, error) {
	return r.get(ctx, id)
}
func (r *healthRepoStub) ListModels(ctx context.Context, id int64) ([]store.ChannelModel, error) {
	return r.models(ctx, id)
}
func (r *healthRepoStub) UpdateHealthView(ctx context.Context, id int64, status string, at *time.Time) error {
	return r.view(ctx, id, status, at)
}

type healthTesterFunc func(context.Context, *store.Channel, string) (ProbeResult, error)

func (f healthTesterFunc) Test(ctx context.Context, ch *store.Channel, model string) (ProbeResult, error) {
	return f(ctx, ch, model)
}

type healthLogBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *healthLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}
func (b *healthLogBuffer) String() string { b.mu.Lock(); defer b.mu.Unlock(); return b.b.String() }

func serviceOptions() HealthOptions {
	return HealthOptions{ProbeInterval: time.Second, DisableDuration: time.Minute, ProbeTimeout: 5 * time.Second,
		StateTimeout: 500 * time.Millisecond, FailureThreshold: 5, ProbeConcurrency: 2}
}

func serviceRepo() (*healthRepoStub, *keyFake) {
	r := &healthRepoStub{
		list: func(context.Context) ([]store.Channel, error) { return nil, nil },
		get: func(_ context.Context, id int64) (*store.Channel, error) {
			return &store.Channel{ID: uint64(id), Enabled: true, Provider: "openai", BaseURL: "http://owned.invalid"}, nil
		},
		models: func(_ context.Context, id int64) ([]store.ChannelModel, error) {
			return []store.ChannelModel{{ChannelID: uint64(id), Model: "a", UpstreamModel: "up"}}, nil
		},
		view: func(context.Context, int64, string, *time.Time) error { return nil },
	}
	k := &keyFake{list: func(_ context.Context, id int64) ([]store.ChannelKey, error) {
		return []store.ChannelKey{{ID: 7, ChannelID: uint64(id), Status: 1, ProviderKeyEnc: []byte("private-service-cipher")}}, nil
	}}
	return r, k
}

func newServiceTest(t *testing.T, r store.ChannelRepo, k store.KeyRepo, state store.VersionedHealthState, tester ConnectivityTester, opts HealthOptions, output io.Writer) *healthService {
	t.Helper()
	if output == nil {
		output = io.Discard
	}
	service, err := NewHealthService(r, k, state, tester, opts, slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			a.Key = "ts"
		}
		return a
	}})))
	require.NoError(t, err)
	h := service.(*healthService)
	t.Cleanup(func() { require.NoError(t, h.Close(context.Background())) })
	return h
}

func TestHealthServiceConstructionAndLifecycle(t *testing.T) {
	r, k := serviceRepo()
	var lists atomic.Int32
	r.list = func(context.Context) ([]store.Channel, error) { lists.Add(1); return nil, nil }
	tester := healthTesterFunc(func(context.Context, *store.Channel, string) (ProbeResult, error) {
		t.Error("no configured channels")
		return ProbeResult{}, nil
	})
	state := &healthMemory{}
	opts, logger := serviceOptions(), slog.Default()
	for _, edit := range []func(*HealthOptions){
		func(o *HealthOptions) { o.ProbeInterval = 0 }, func(o *HealthOptions) { o.DisableDuration = -1 },
		func(o *HealthOptions) { o.ProbeTimeout = time.Microsecond }, func(o *HealthOptions) { o.StateTimeout = time.Microsecond },
		func(o *HealthOptions) { o.FailureThreshold = 0 }, func(o *HealthOptions) { o.FailureThreshold = 1 << 31 },
		func(o *HealthOptions) { o.ProbeConcurrency = 0 }, func(o *HealthOptions) { o.ProbeConcurrency = 65 },
	} {
		bad := opts
		edit(&bad)
		_, err := NewHealthService(r, k, state, tester, bad, logger)
		require.Error(t, err)
	}
	var nilRepo *healthRepoStub
	var nilKey *keyFake
	var nilState *healthMemory
	var nilTester healthTesterFunc
	for _, deps := range []struct {
		r store.ChannelRepo
		k store.KeyRepo
		s store.VersionedHealthState
		t ConnectivityTester
		l *slog.Logger
	}{
		{nilRepo, k, state, tester, logger}, {r, nilKey, state, tester, logger}, {r, k, nilState, tester, logger}, {r, k, state, nilTester, logger}, {r, k, state, tester, nil},
	} {
		_, err := NewHealthService(deps.r, deps.k, deps.s, deps.t, opts, deps.l)
		require.Error(t, err)
	}
	h := newServiceTest(t, r, k, state, tester, opts, nil)
	require.Zero(t, lists.Load(), "constructor must not start any worker")
	require.NoError(t, h.Close(context.Background()))
	require.NoError(t, h.Close(context.Background()))
	require.Error(t, h.Start(context.Background()))
	_, err := h.Read(context.Background(), 1)
	require.Error(t, err)
	require.False(t, h.IsAvailable(context.Background(), 1))
	h.ReportRequestResult(context.Background(), 1, false)
	require.Zero(t, state.loads)
	require.Zero(t, state.cas)
	failedStart := newServiceTest(t, r, k, &healthMemory{}, tester, opts, nil)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, failedStart.Start(canceled), context.Canceled)
	require.Error(t, failedStart.Start(context.Background()), "Start remains a one-time operation even when its input was canceled")
	require.NoError(t, failedStart.Close(context.Background()))
	synctest.Test(t, func(t *testing.T) {
		h := newServiceTest(t, r, k, &healthMemory{}, tester, opts, nil)
		require.NoError(t, h.Start(context.Background()))
		require.Error(t, h.Start(context.Background()))
		synctest.Wait()
		require.Equal(t, int32(1), lists.Load())
		require.NoError(t, h.Close(context.Background()))
		time.Sleep(2 * time.Second)
		require.Equal(t, int32(1), lists.Load(), "no round after Close")
	})
}

func TestHealthServiceFixedWorkersBoundedQueueAndNoOverlappingRounds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, k := serviceRepo()
		var lists, active, maximum atomic.Int32
		var callsMu sync.Mutex
		var called []int64
		r.list = func(context.Context) ([]store.Channel, error) {
			lists.Add(1)
			rows := make([]store.Channel, 13)
			for i := range rows {
				rows[i] = store.Channel{ID: uint64(i + 1), Enabled: i != 12}
			}
			return rows, nil
		}
		release := make(chan struct{})
		tester := healthTesterFunc(func(ctx context.Context, ch *store.Channel, model string) (ProbeResult, error) {
			callsMu.Lock()
			called = append(called, int64(ch.ID))
			callsMu.Unlock()
			n := active.Add(1)
			for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
			}
			defer active.Add(-1)
			select {
			case <-release:
				return ProbeResult{OK: true}, nil
			case <-ctx.Done():
				return ProbeResult{}, ctx.Err()
			}
		})
		opts := serviceOptions()
		opts.ProbeInterval = 10 * time.Millisecond
		h := newServiceTest(t, r, k, &healthMemory{}, tester, opts, nil)
		require.NoError(t, h.Start(context.Background()))
		synctest.Wait()
		require.Equal(t, int32(2), active.Load())
		require.Equal(t, 2, cap(h.jobs))
		require.Equal(t, 2, len(h.jobs), "only bounded pending jobs, no per-channel goroutine")
		time.Sleep(100 * time.Millisecond)
		require.Equal(t, int32(1), lists.Load(), "unfinished round does not overlap or accumulate ticks")
		close(release)
		synctest.Wait()
		require.Equal(t, int32(0), active.Load())
		require.Equal(t, int32(2), maximum.Load())
		callsMu.Lock()
		require.Len(t, called, 12)
		require.NotContains(t, called, int64(13))
		callsMu.Unlock()
		time.Sleep(9 * time.Millisecond)
		require.Equal(t, int32(1), lists.Load())
		time.Sleep(time.Millisecond)
		synctest.Wait()
		require.Equal(t, int32(2), lists.Load())
		require.NoError(t, h.Close(context.Background()))
	})
}

func TestHealthServiceCancellationJoinsEveryBlockingStage(t *testing.T) {
	for _, stage := range []string{"enumeration", "channel", "models", "keys", "state", "provider", "display"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, k := serviceRepo()
				state := &healthMemory{}
				r.list = func(context.Context) ([]store.Channel, error) { return []store.Channel{{ID: 1, Enabled: true}}, nil }
				entered, exited := make(chan struct{}), make(chan struct{})
				var once sync.Once
				block := func(ctx context.Context) error {
					once.Do(func() { close(entered) })
					<-ctx.Done()
					close(exited)
					return ctx.Err()
				}
				tester := healthTesterFunc(func(context.Context, *store.Channel, string) (ProbeResult, error) { return ProbeResult{OK: true}, nil })
				switch stage {
				case "enumeration":
					r.list = func(ctx context.Context) ([]store.Channel, error) { return nil, block(ctx) }
				case "channel":
					r.get = func(ctx context.Context, _ int64) (*store.Channel, error) { return nil, block(ctx) }
				case "models":
					r.models = func(ctx context.Context, _ int64) ([]store.ChannelModel, error) { return nil, block(ctx) }
				case "keys":
					k.list = func(ctx context.Context, _ int64) ([]store.ChannelKey, error) { return nil, block(ctx) }
				case "state":
					state.loadFn = block
				case "provider":
					tester = healthTesterFunc(func(ctx context.Context, _ *store.Channel, _ string) (ProbeResult, error) {
						return ProbeResult{}, block(ctx)
					})
				case "display":
					r.view = func(ctx context.Context, _ int64, _ string, _ *time.Time) error { return block(ctx) }
				}
				h := newServiceTest(t, r, k, state, tester, serviceOptions(), nil)
				require.NoError(t, h.Start(context.Background()))
				<-entered
				require.NoError(t, h.Close(context.Background()))
				select {
				case <-exited:
				default:
					t.Fatal("Close returned before dependency operation exited")
				}
				select {
				case <-h.done:
				default:
					t.Fatal("Close returned before owners exited")
				}
				before := state.loads
				time.Sleep(10 * time.Second)
				require.Equal(t, before, state.loads, "no resource access after Close")
			})
		})
	}
}

func TestHealthServiceCloseDeadlineDoesNotDetachOwnerOrRestartBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, k := serviceRepo()
		entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		r.list = func(ctx context.Context) ([]store.Channel, error) {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return nil, ctx.Err()
		}
		h := newServiceTest(t, r, k, &healthMemory{}, healthTesterFunc(func(context.Context, *store.Channel, string) (ProbeResult, error) { return ProbeResult{}, nil }), serviceOptions(), nil)
		require.NoError(t, h.Start(context.Background()))
		<-entered
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		finished := make(chan error, 1)
		go func() { finished <- h.Close(ctx) }()
		<-canceled
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		select {
		case <-finished:
			t.Fatal("deadline must not detach still-owned operation")
		default:
		}
		close(release)
		require.ErrorIs(t, <-finished, context.DeadlineExceeded)
		require.NoError(t, h.Close(context.Background()))
	})
}

func TestHealthDisplayQueueCoalescesBoundsAndReadsLatestState(t *testing.T) {
	r, k := serviceRepo()
	var logs healthLogBuffer
	state := &healthMemory{}
	h := newServiceTest(t, r, k, state, healthTesterFunc(func(context.Context, *store.Channel, string) (ProbeResult, error) { return ProbeResult{}, nil }), serviceOptions(), &logs)
	for id := int64(1); id <= healthViewCapacity; id++ {
		h.enqueueView(context.Background(), id)
		h.enqueueView(context.Background(), id)
	}
	h.enqueueView(context.Background(), healthViewCapacity+1)
	require.Len(t, h.pending, healthViewCapacity)
	require.Len(t, h.views, healthViewCapacity)
	require.Contains(t, logs.String(), "display_queue_full")
	beforeWrites := state.writes
	h.ReportRequestResult(context.Background(), healthViewCapacity+1, false)
	require.Equal(t, beforeWrites+1, state.writes, "queue overflow must not drop the Redis health signal")
	require.Len(t, h.views, healthViewCapacity)
	synctest.Test(t, func(t *testing.T) {
		r, k := serviceRepo()
		state := &healthMemory{}
		opts := serviceOptions()
		opts.ProbeInterval = time.Hour
		h := newServiceTest(t, r, k, state, healthTesterFunc(func(context.Context, *store.Channel, string) (ProbeResult, error) { return ProbeResult{}, nil }), opts, nil)
		h.core.now = func() time.Time { return time.Unix(100, 0) }
		h.ReportRequestResult(context.Background(), 1, false)
		h.ReportRequestResult(context.Background(), 1, true)
		require.Len(t, h.views, 1)
		// Newer state exists before the worker runs: it must not mirror event data.
		state.record = store.HealthRecord{Snapshot: store.HealthSnapshot{ConsecutiveFailures: 5, DisabledUntil: 99, LastProbeAt: 80, LastProbeOk: false}, Version: 4}
		views := make(chan string, 1)
		r.view = func(ctx context.Context, id int64, status string, at *time.Time) error {
			require.Equal(t, int64(1), id)
			require.NotNil(t, at)
			require.Equal(t, time.Unix(80, 0).UTC(), *at)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, time.Now().Add(opts.StateTimeout), deadline)
			views <- status
			return errors.New("owned SQL display failure")
		}
		require.NoError(t, h.Start(context.Background()))
		require.Equal(t, "cooling", <-views)
		synctest.Wait()
		require.False(t, h.IsAvailable(context.Background(), 1), "SQL failure cannot change Redis truth")
		require.Equal(t, uint64(4), state.record.Version)
		require.NoError(t, h.Close(context.Background()))
	})
}

func TestHealthServiceCloseAlsoJoinsInFlightRequestStateOperation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, k := serviceRepo()
		state := &healthMemory{}
		entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		state.loadFn = func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			close(canceled)
			<-release
			return ctx.Err()
		}
		h := newServiceTest(t, r, k, state, healthTesterFunc(func(context.Context, *store.Channel, string) (ProbeResult, error) { return ProbeResult{}, nil }), serviceOptions(), nil)
		reported := make(chan struct{})
		go func() { h.ReportRequestResult(context.Background(), 1, false); close(reported) }()
		<-entered
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		closed := make(chan error, 1)
		go func() { closed <- h.Close(ctx) }()
		<-canceled
		time.Sleep(20 * time.Millisecond)
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("in-flight request state operation was detached")
		default:
		}
		close(release)
		require.ErrorIs(t, <-closed, context.DeadlineExceeded)
		<-reported
		require.Equal(t, 1, state.loads)
		require.Zero(t, state.writes)
	})
}

func TestHealthServiceEnumerationAndSingleChannelBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, k := serviceRepo()
		state := &healthMemory{}
		tester := healthTesterFunc(func(context.Context, *store.Channel, string) (ProbeResult, error) {
			t.Error("enumeration failure must not probe")
			return ProbeResult{}, nil
		})
		opts := serviceOptions()
		r.list = func(ctx context.Context) ([]store.Channel, error) {
			deadline, _ := ctx.Deadline()
			require.Equal(t, time.Now().Add(opts.ProbeTimeout), deadline)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		h := newServiceTest(t, r, k, state, tester, opts, nil)
		start := time.Now()
		h.round(context.Background(), h.jobs, make(chan struct{}))
		require.Equal(t, opts.ProbeTimeout, time.Since(start))
		require.Zero(t, state.loads)
	})
	synctest.Test(t, func(t *testing.T) {
		r, k := serviceRepo()
		state := &healthMemory{}
		opts := serviceOptions()
		start := time.Now()
		gets := 0
		originalGet := r.get
		r.get = func(ctx context.Context, id int64) (*store.Channel, error) {
			gets++
			deadline, _ := ctx.Deadline()
			if gets == 1 {
				require.Equal(t, start.Add(opts.ProbeTimeout), deadline)
				time.Sleep(3 * time.Second)
			} else {
				require.Equal(t, start.Add(opts.ProbeTimeout+opts.StateTimeout), deadline)
			}
			return originalGet(ctx, id)
		}
		tester := healthTesterFunc(func(ctx context.Context, _ *store.Channel, _ string) (ProbeResult, error) {
			deadline, _ := ctx.Deadline()
			require.Equal(t, 2*time.Second, time.Until(deadline))
			<-ctx.Done()
			return ProbeResult{OK: false}, nil
		})
		h := newServiceTest(t, r, k, state, tester, opts, nil)
		h.probeChannel(context.Background(), 1)
		require.Equal(t, opts.ProbeTimeout, time.Since(start), "SQL and network share one probe deadline")
		require.Equal(t, uint64(1), state.record.Version)
		require.Equal(t, int32(1), state.record.Snapshot.ConsecutiveFailures)
	})
}

func TestHealthDisplayViewValues(t *testing.T) {
	for _, tc := range []struct {
		s       store.HealthSnapshot
		want    string
		checked bool
	}{
		{store.HealthSnapshot{}, "unknown", false},
		{store.HealthSnapshot{ConsecutiveFailures: 1}, "unknown", false},
		{store.HealthSnapshot{LastProbeAt: 90, LastProbeOk: true}, "healthy", true},
		{store.HealthSnapshot{DisabledUntil: 101}, "disabled", false},
		{store.HealthSnapshot{DisabledUntil: 100, LastProbeAt: 90}, "cooling", true},
	} {
		status, at := healthView(tc.s, time.Unix(100, 0))
		require.Equal(t, tc.want, status)
		require.Equal(t, tc.checked, at != nil)
	}
}
