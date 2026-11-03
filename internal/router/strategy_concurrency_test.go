package router

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

func TestStrategyCapacityExactEvictionAndReadDoesNotRefresh(t *testing.T) {
	s := strategyFixture(t, nil)
	now := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	for i := 0; i < 5; i++ {
		s.ObserveSuccess(t.Context(), 1, "a", time.Nanosecond)
		s.ObserveSuccess(t.Context(), 1, "z", time.Nanosecond)
		s.ObserveSuccess(t.Context(), 2, "a", 2*time.Nanosecond)
	}
	// Insert backwards so deterministic eviction cannot rely on insertion/map order.
	for id := int64(4095); id >= 3; id-- {
		s.ObserveSuccess(t.Context(), id, "a", 0)
	}
	require.Len(t, s.entries, 4096)
	c := strategyCandidates(t, 2)
	now = now.Add(time.Nanosecond)
	observed := s.entries[latencyKey{1, "a"}].observed
	require.Equal(t, []uint64{1, 2}, orderedIDs(t, s, "a", LatencyFirst, c))
	require.Equal(t, observed, s.entries[latencyKey{1, "a"}].observed, "read must not refresh eviction time")
	s.ObserveSuccess(t.Context(), 4096, "a", 0)
	require.Len(t, s.entries, 4096)
	require.NotContains(t, s.entries, latencyKey{1, "a"}, "smallest channel/alias among equal oldest times")
	require.Contains(t, s.entries, latencyKey{1, "z"})
	require.Contains(t, s.entries, latencyKey{2, "a"})
	require.Contains(t, s.entries, latencyKey{4096, "a"})
	require.Equal(t, []uint64{2, 1}, orderedIDs(t, s, "a", LatencyFirst, c), "evicted channel is cold")
	now = now.Add(time.Nanosecond)
	s.ObserveSuccess(t.Context(), 4097, "a", 0)
	require.NotContains(t, s.entries, latencyKey{1, "z"}, "alias tie precedes the next channel")
	require.Contains(t, s.entries, latencyKey{2, "a"})
	// Re-admission starts from one sample, never resurrecting the old five.
	s.ObserveSuccess(t.Context(), 1, "a", 0)
	require.Equal(t, 1, s.entries[latencyKey{1, "a"}].count)
	require.False(t, s.latencies("a", c, now)[1].known)
	require.Len(t, s.entries, 4096)
}

func TestStrategyCapacityObservationRefreshesOldestAndExpiredKeys(t *testing.T) {
	s := strategyFixture(t, nil)
	now := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	for id := int64(1); id <= 4096; id++ {
		s.ObserveSuccess(t.Context(), id, "public", 0)
		now = now.Add(time.Nanosecond)
	}
	s.ObserveSuccess(t.Context(), 1, "public", 0)
	now = now.Add(time.Nanosecond)
	s.ObserveSuccess(t.Context(), 4097, "public", 0)
	require.Contains(t, s.entries, latencyKey{1, "public"}, "a write refreshes last observation")
	require.NotContains(t, s.entries, latencyKey{2, "public"})
	require.Contains(t, s.entries, latencyKey{3, "public"})
	now = now.Add(time.Minute)
	orderedIDs(t, s, "public", LatencyFirst, strategyCandidates(t, 3))
	require.Zero(t, s.entries[latencyKey{1, "public"}].count)
	require.Len(t, s.entries, 4096, "reads prune samples without changing key eviction history")
	s.ObserveSuccess(t.Context(), 4098, "public", 0)
	require.NotContains(t, s.entries, latencyKey{3, "public"}, "expiry/read must not rescue an older key")
	require.Len(t, s.entries, 4096)
}

func awaitStrategySignal(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("controlled operation did not make progress")
	}
}

// Releasing the blocked dependency precedes Wait even if a progress assertion fails.
func strategyBarrier(t *testing.T) (chan struct{}, *sync.WaitGroup) {
	t.Helper()
	release := make(chan struct{})
	wg := &sync.WaitGroup{}
	t.Cleanup(func() { close(release); wg.Wait() })
	return release, wg
}

func assertStrategyPeersProgress(t *testing.T, s *strategyEngine, wg *sync.WaitGroup) {
	t.Helper()
	require.True(t, s.mu.TryLock(), "dependency must not hold the sample table lock")
	s.mu.Unlock()
	done := make(chan struct{})
	var peerErr error
	var ids []uint64
	c := strategyCandidates(t, 2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < 5; i++ {
			s.ObserveSuccess(t.Context(), 2, "public", 0)
		}
		var out []Candidate
		out, peerErr = s.Order(t.Context(), "public", LatencyFirst, c)
		ids = candidateIDs(&Plan{Candidates: out})
	}()
	awaitStrategySignal(t, done)
	require.NoError(t, peerErr)
	require.Equal(t, []uint64{2, 1}, ids)
}

func TestStrategyBlockedPriceDoesNotHoldSamplesOrStopOtherOrders(t *testing.T) {
	entered := make(chan struct{})
	release, wg := strategyBarrier(t)
	s := strategyFixture(t, func(ctx context.Context, model string, _ time.Time) (*store.ModelPrice, error) {
		if model == "up-1" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &store.ModelPrice{Currency: "CNY"}, nil
	})
	blockedDone := make(chan struct{})
	c := strategyCandidates(t, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(blockedDone)
		_, err := s.Order(context.WithoutCancel(t.Context()), "public", CostFirst, c)
		if err != nil {
			t.Error(err)
		}
	}()
	awaitStrategySignal(t, entered)
	assertStrategyPeersProgress(t, s, wg)
	other := cloneCandidates(c)
	other[0].UpstreamModel = "up-2"
	require.Equal(t, []uint64{1}, orderedIDs(t, s, "public", CostFirst, other))
	select {
	case <-blockedDone:
		t.Fatal("blocked lookup unexpectedly completed")
	default:
	}
}

type strategyBlockingLog struct {
	entered chan struct{}
	release <-chan struct{}
}

func (h *strategyBlockingLog) Enabled(context.Context, slog.Level) bool { return true }
func (h *strategyBlockingLog) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *strategyBlockingLog) WithGroup(string) slog.Handler            { return h }
func (h *strategyBlockingLog) Handle(context.Context, slog.Record) error {
	close(h.entered)
	<-h.release
	return nil
}

func TestStrategyBlockedLogDoesNotHoldSamples(t *testing.T) {
	release, wg := strategyBarrier(t)
	h := &strategyBlockingLog{entered: make(chan struct{}), release: release}
	s := strategyFixture(t, nil)
	s.logger = slog.New(h)
	wg.Add(1)
	go func() { defer wg.Done(); s.ObserveSuccess(t.Context(), 0, "rejected", -1) }()
	awaitStrategySignal(t, h.entered)
	assertStrategyPeersProgress(t, s, wg)
}

func TestStrategyBlockedPriceHonorsEarlierContextAndCause(t *testing.T) {
	for _, parentTimeout := range []time.Duration{15 * time.Millisecond, 150 * time.Millisecond} {
		t.Run(parentTimeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				cause := errors.New("private deadline cause")
				ctx, cancel := context.WithTimeoutCause(t.Context(), parentTimeout, cause)
				defer cancel()
				started := time.Now()
				calls := 0
				s := strategyFixture(t, func(got context.Context, _ string, _ time.Time) (*store.ModelPrice, error) {
					calls++
					require.Equal(t, ctx, got)
					<-got.Done()
					return nil, got.Err()
				})
				_, err := s.Order(ctx, "public", CostFirst, strategyCandidates(t, 2))
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.ErrorIs(t, err, cause)
				require.NotContains(t, err.Error(), cause.Error())
				require.Equal(t, parentTimeout, time.Since(started))
				require.Equal(t, 1, calls)
			})
		})
	}
}

func TestStrategyConcurrentCapacityAndSampleBounds(t *testing.T) {
	s := strategyFixture(t, nil)
	var ticks atomic.Int64
	base := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return base.Add(time.Duration(ticks.Add(1))) }
	c := strategyCandidates(t, 3)
	before := cloneCandidates(c)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 16; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 300; i++ {
				id := int64(10000 + worker*300 + i)
				s.ObserveSuccess(t.Context(), id, "public", time.Duration(i))
				s.ObserveSuccess(t.Context(), int64(worker%3+1), "public", time.Duration(i))
				out, err := s.Order(t.Context(), "public", LatencyFirst, c)
				if err != nil {
					t.Error(err)
					return
				}
				ids := candidateIDs(&Plan{Candidates: out})
				slices.Sort(ids)
				if !slices.Equal(ids, []uint64{1, 2, 3}) {
					t.Error("incomplete permutation")
					return
				}
				*out[0].Channel.LastCheckedAt = time.Time{}
				s.mu.Lock()
				bounded := len(s.entries) <= 4096
				s.mu.Unlock()
				if !bounded {
					t.Error("key capacity exceeded")
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Len(t, s.entries, 4096)
	require.Equal(t, before, c)
	for _, e := range s.entries {
		require.LessOrEqual(t, e.count, 256)
	}
	for id := int64(1); id <= 3; id++ {
		require.Equal(t, 256, s.entries[latencyKey{id, "public"}].count)
	}
}

func TestStrategySamplesRetainOnlyBoundedScalarState(t *testing.T) {
	// Mechanism evidence: samples cannot retain the supplied context/request/Usage/Key.
	entry := reflect.TypeOf(latencyEntry{})
	require.Equal(t, 3, entry.NumField())
	require.Equal(t, reflect.Array, entry.Field(0).Type.Kind())
	require.Equal(t, 256, entry.Field(0).Type.Len())
	sample := entry.Field(0).Type.Elem()
	require.Equal(t, 2, sample.NumField())
	require.Equal(t, reflect.TypeOf(time.Time{}), sample.Field(0).Type)
	require.Equal(t, reflect.TypeOf(time.Duration(0)), sample.Field(1).Type)
	require.Equal(t, reflect.TypeOf(0), entry.Field(1).Type)
	require.Equal(t, reflect.TypeOf(time.Time{}), entry.Field(2).Type)
	key := reflect.TypeOf(latencyKey{})
	require.Equal(t, 2, key.NumField())
	require.Equal(t, reflect.Int64, key.Field(0).Type.Kind())
	require.Equal(t, reflect.String, key.Field(1).Type.Kind())
}
