package channel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// All state-machine schedules are explicit; no short sleeps guess a race.
type healthMemory struct {
	mu     sync.Mutex
	record store.HealthRecord
	loads  int
	writes int
	cas    int
	loadFn func(context.Context) error
	casFn  func(context.Context) (bool, error)
}

func (s *healthMemory) LoadVersioned(ctx context.Context, _ int64) (store.HealthRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loads++
	if s.loadFn != nil {
		if err := s.loadFn(ctx); err != nil {
			return store.HealthRecord{}, err
		}
	}
	return s.record, nil
}

func (s *healthMemory) CompareAndSwap(ctx context.Context, _ int64, expected store.HealthRecord, next store.HealthSnapshot) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cas++
	if s.casFn != nil {
		ok, err := s.casFn(ctx)
		if !ok || err != nil {
			return false, err
		}
	}
	if s.record != expected {
		return false, nil
	}
	s.record = store.HealthRecord{Snapshot: next, Version: expected.Version + 1}
	s.writes++
	return true, nil
}

func healthForTest(t *testing.T, state store.VersionedHealthState, now func() time.Time, output io.Writer, policy healthPolicy) *healthCore {
	t.Helper()
	if output == nil {
		output = io.Discard
	}
	logger := slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			a.Key = "ts"
		}
		return a
	}}))
	h, err := newHealthCore(state, policy, logger, now)
	require.NoError(t, err)
	return h
}

func testFingerprint() probeFingerprint {
	return newProbeFingerprint(store.Channel{ID: 1, Enabled: true, Provider: "openai", BaseURL: "http://probe.invalid"},
		store.ChannelModel{ChannelID: 1, Model: "alias", UpstreamModel: "upstream"},
		store.ChannelKey{ID: 7, ChannelID: 1, Status: store.ChannelKeyStatusActive, ProviderKeyEnc: []byte("ciphertext-private-marker")})
}

func completeHealthProbe(t *testing.T, h *healthCore, state *healthMemory, ok bool, now time.Time) {
	t.Helper()
	ctx := context.Background()
	fingerprint := testFingerprint()
	generation, eligible, err := h.beginProbe(ctx, 1, fingerprint)
	require.NoError(t, err)
	require.True(t, eligible)
	before := state.writes
	committed, err := h.finishProbe(ctx, 1, generation, ProbeResult{OK: ok}, nil, now,
		func(context.Context) (probeFingerprint, error) { return fingerprint, nil })
	require.NoError(t, err)
	require.True(t, committed)
	require.Equal(t, before+1, state.writes)
}

func TestHealthCoreRequestTransitionsAndRecovery(t *testing.T) {
	now := time.Unix(100, 900_000_000).In(time.FixedZone("test", 8*60*60))
	state := &healthMemory{}
	h := healthForTest(t, state, func() time.Time { return now }, nil, healthPolicy{disableDuration: 200 * time.Millisecond})
	ctx := context.Background()
	require.True(t, h.IsAvailable(ctx, 1))
	require.Zero(t, state.writes)
	for i := 1; i < 5; i++ {
		h.ReportRequestResult(ctx, 1, false)
		require.Equal(t, int32(i), state.record.Snapshot.ConsecutiveFailures)
		require.True(t, h.IsAvailable(ctx, 1))
	}
	// Ordinary success clears failures and retains last probe data.
	completeHealthProbe(t, h, state, true, now)
	h.ReportRequestResult(ctx, 1, false)
	h.ReportRequestResult(ctx, 1, true)
	require.Equal(t, store.HealthSnapshot{LastProbeAt: 100, LastProbeOk: true}, state.record.Snapshot)
	for i := 0; i < 5; i++ {
		h.ReportRequestResult(ctx, 1, false)
	}
	require.Equal(t, store.HealthSnapshot{ConsecutiveFailures: 5, DisabledUntil: 102, LastProbeAt: 100, LastProbeOk: true}, state.record.Snapshot)
	require.False(t, h.IsAvailable(ctx, 1))
	before, writes, cas := state.record, state.writes, state.cas
	for _, ok := range []bool{true, false, true} {
		h.ReportRequestResult(ctx, 1, ok)
	}
	require.Equal(t, before, state.record)
	require.Equal(t, writes, state.writes)
	require.Equal(t, cas, state.cas, "disabled request results must not even attempt CAS")
	_, eligible, err := h.beginProbe(ctx, 1, testFingerprint())
	require.NoError(t, err)
	require.False(t, eligible, "network must be skipped before cooldown expires")
	now = time.Unix(102, 0)
	require.False(t, h.IsAvailable(ctx, 1), "expiry alone cannot restore availability")
	completeHealthProbe(t, h, state, false, now)
	require.Equal(t, store.HealthSnapshot{ConsecutiveFailures: 5, DisabledUntil: 103, LastProbeAt: 102}, state.record.Snapshot)
	now = time.Unix(103, 0)
	require.False(t, h.IsAvailable(ctx, 1))
	completeHealthProbe(t, h, state, true, now)
	require.True(t, h.IsAvailable(ctx, 1))
	require.Equal(t, store.HealthSnapshot{LastProbeAt: 103, LastProbeOk: true}, state.record.Snapshot)
}

func TestHealthCoreProbeSignalsAndSaturation(t *testing.T) {
	now := time.Unix(200, 0)
	state := &healthMemory{}
	h := healthForTest(t, state, func() time.Time { return now }, nil, healthPolicy{failureThreshold: 2})
	completeHealthProbe(t, h, state, false, now)
	require.Equal(t, store.HealthSnapshot{ConsecutiveFailures: 1, LastProbeAt: 200}, state.record.Snapshot)
	completeHealthProbe(t, h, state, true, now)
	require.Equal(t, int32(0), state.record.Snapshot.ConsecutiveFailures)
	completeHealthProbe(t, h, state, false, now)
	completeHealthProbe(t, h, state, false, now)
	require.Equal(t, store.HealthSnapshot{ConsecutiveFailures: 2, DisabledUntil: 260, LastProbeAt: 200}, state.record.Snapshot)
	state.record.Snapshot = store.HealthSnapshot{ConsecutiveFailures: math.MaxInt32}
	h.ReportRequestResult(context.Background(), 1, false)
	require.Equal(t, int32(2), state.record.Snapshot.ConsecutiveFailures)
	require.Equal(t, int64(260), state.record.Snapshot.DisabledUntil)
	h.policy.failureThreshold = math.MaxInt32
	state.record.Snapshot = store.HealthSnapshot{ConsecutiveFailures: math.MaxInt32}
	h.ReportRequestResult(context.Background(), 1, false)
	require.Equal(t, int32(math.MaxInt32), state.record.Snapshot.ConsecutiveFailures)
}

func TestHealthCoreCooldownRoundingAndOverflow(t *testing.T) {
	for _, tc := range []struct {
		name     string
		now      time.Time
		duration time.Duration
		want     int64
		invalid  bool
	}{
		{"whole", time.Unix(100, 0), time.Second, 101, false},
		{"nanosecond", time.Unix(100, 0), time.Nanosecond, 101, false},
		{"exact_boundary", time.Unix(100, 900_000_000), 100 * time.Millisecond, 101, false},
		{"cross_boundary", time.Unix(100, 900_000_000), 100*time.Millisecond + 1, 102, false},
		{"subsecond", time.Unix(100, 1), time.Nanosecond, 101, false},
		{"max_duration", time.Unix(100, 0), time.Duration(math.MaxInt64), 9223372137, false},
		{"max_seconds", time.Unix(math.MaxInt64, 0), time.Nanosecond, 0, true},
		{"ceil_overflow", time.Unix(math.MaxInt64-1, 1), time.Second, 0, true},
		{"last_second", time.Unix(math.MaxInt64-1, 0), time.Second, math.MaxInt64, false},
		{"before_epoch", time.Unix(-1, 0), time.Second, 0, true},
		{"zero_duration", time.Unix(100, 0), 0, 0, true},
		{"negative_duration", time.Unix(100, 0), -1, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := cooldownUntil(tc.now, tc.duration)
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			}
		})
	}
	state := &healthMemory{}
	var logs bytes.Buffer
	h := healthForTest(t, state, func() time.Time { return time.Unix(math.MaxInt64, 0) }, &logs, healthPolicy{failureThreshold: 1})
	h.ReportRequestResult(context.Background(), 1, false)
	require.Zero(t, state.cas)
	require.Zero(t, state.writes)
	require.Contains(t, logs.String(), "health_state_failed")
}

func TestHealthCoreConflictLimitAndRecalculation(t *testing.T) {
	var logs bytes.Buffer
	state := &healthMemory{casFn: func(context.Context) (bool, error) { return false, nil }}
	h := healthForTest(t, state, func() time.Time { return time.Unix(100, 0) }, &logs, healthPolicy{})
	ctx := middleware.WithRequestID(context.Background(), "req-health-test")
	h.ReportRequestResult(ctx, 1, false)
	require.Equal(t, 16, state.loads)
	require.Equal(t, 16, state.cas)
	require.Zero(t, state.writes)
	require.Contains(t, logs.String(), `"reason":"conflicts_exhausted"`)
	require.Contains(t, logs.String(), `"request_id":"req-health-test"`)
	require.Contains(t, logs.String(), `"ts":`)

	state = &healthMemory{}
	state.casFn = func(context.Context) (bool, error) {
		if state.cas == 1 {
			// A different client commits a failure before this CAS.
			state.record = store.HealthRecord{Snapshot: store.HealthSnapshot{ConsecutiveFailures: 1}, Version: 1}
			return false, nil
		}
		return true, nil
	}
	h = healthForTest(t, state, func() time.Time { return time.Unix(100, 0) }, nil, healthPolicy{})
	h.ReportRequestResult(ctx, 1, false)
	require.Equal(t, 2, state.loads)
	require.Equal(t, store.HealthRecord{Snapshot: store.HealthSnapshot{ConsecutiveFailures: 2}, Version: 2}, state.record)
}

func TestHealthCoreStateBudgetIsSharedAndParentWins(t *testing.T) {
	for _, parentBudget := range []time.Duration{0, 25 * time.Millisecond} {
		t.Run(parentBudget.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				state := &healthMemory{}
				var deadlines []time.Time
				state.loadFn = func(ctx context.Context) error {
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					deadlines = append(deadlines, deadline)
					select {
					case <-time.After(15 * time.Millisecond):
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				state.casFn = func(ctx context.Context) (bool, error) {
					deadline, ok := ctx.Deadline()
					require.True(t, ok)
					deadlines = append(deadlines, deadline)
					return false, nil
				}
				h := healthForTest(t, state, time.Now, nil, healthPolicy{stateTimeout: 50 * time.Millisecond})
				ctx := context.Background()
				want := 50 * time.Millisecond
				if parentBudget != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, parentBudget)
					defer cancel()
					want = parentBudget
				}
				start := time.Now()
				committed, err := h.report(ctx, 1, false)
				require.False(t, committed)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, want, time.Since(start))
				for _, deadline := range deadlines {
					require.Equal(t, start.Add(want), deadline)
				}
				require.Less(t, state.cas, 16)
				require.Zero(t, state.writes)
			})
		})
	}
}

func TestHealthCoreFaultsCloseAvailabilityAndPreserveReadError(t *testing.T) {
	for _, stage := range []string{"load", "cas"} {
		t.Run(stage, func(t *testing.T) {
			secretErr := errors.New("secret-key-plaintext URL credentials ciphertext-private-marker")
			state := &healthMemory{}
			if stage == "load" {
				state.loadFn = func(context.Context) error { return secretErr }
			} else {
				state.casFn = func(context.Context) (bool, error) { return false, secretErr }
			}
			var logs bytes.Buffer
			h := healthForTest(t, state, time.Now, &logs, healthPolicy{})
			ctx := middleware.WithRequestID(context.Background(), "req-fault")
			if stage == "load" {
				require.False(t, h.IsAvailable(ctx, 1))
				snapshot, err := h.Read(ctx, 1)
				require.ErrorIs(t, err, secretErr)
				require.Equal(t, store.HealthSnapshot{}, snapshot)
			}
			h.ReportRequestResult(ctx, 1, false)
			require.Contains(t, logs.String(), `"request_id":"req-fault"`)
			require.Contains(t, logs.String(), `"reason":"state_error"`)
			require.NotContains(t, logs.String(), secretErr.Error())
			require.NotContains(t, logs.String(), "secret-key")
			require.Zero(t, state.writes)
		})
	}
}

func TestHealthCoreDefaultsValidationAndCanceledCalls(t *testing.T) {
	h := healthForTest(t, &healthMemory{}, nil, nil, healthPolicy{})
	require.Equal(t, healthPolicy{5, 60 * time.Second, 500 * time.Millisecond}, h.policy)
	require.Equal(t, time.UTC, h.now().Location())
	for _, policy := range []healthPolicy{{failureThreshold: -1}, {failureThreshold: math.MaxInt32 + 1}, {disableDuration: -1}, {stateTimeout: time.Microsecond}} {
		_, err := newHealthCore(&healthMemory{}, policy, slog.Default(), nil)
		require.Error(t, err)
	}
	var state *healthMemory
	_, err := newHealthCore(state, healthPolicy{}, slog.Default(), nil)
	require.Error(t, err)
	_, err = newHealthCore(&healthMemory{}, healthPolicy{}, nil, nil)
	require.Error(t, err)
	memory := &healthMemory{}
	var logs bytes.Buffer
	h = healthForTest(t, memory, nil, &logs, healthPolicy{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, h.IsAvailable(ctx, 1))
	h.ReportRequestResult(ctx, 1, true)
	_, err = h.Read(ctx, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.Contains(t, logs.String(), `"reason":"canceled"`)
	require.Zero(t, memory.loads)
	for _, id := range []int64{0, -1} {
		require.False(t, h.IsAvailable(context.Background(), id))
		h.ReportRequestResult(context.Background(), id, true)
	}
	require.Zero(t, memory.cas)
}

func TestHealthCoreConcurrentOrdinarySignalsWithVersionedStore(t *testing.T) {
	mr := miniredis.RunT(t)
	client := store.NewRedis(store.RedisOptions{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	h := healthForTest(t, store.NewVersionedHealthState(client), func() time.Time { return time.Unix(100, 0) }, nil,
		healthPolicy{failureThreshold: 100, stateTimeout: 5 * time.Second})
	// Four independent reports each count once, with no saturation masking loss.
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		wg.Go(func() {
			<-start
			committed, err := h.report(context.Background(), 1, false)
			if err == nil && !committed {
				err = errors.New("ordinary signal did not commit")
			}
			errs <- err
		})
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	record, err := h.state.LoadVersioned(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, int32(4), record.Snapshot.ConsecutiveFailures)
	require.Equal(t, uint64(4), record.Version)
	// Redis fault: fail closed and return the classifiable error through Read.
	require.NoError(t, client.Close())
	require.False(t, h.IsAvailable(context.Background(), 1))
	_, err = h.Read(context.Background(), 1)
	require.ErrorIs(t, err, redis.ErrClosed)
}
