package router

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

type strategyPrices func(context.Context, string, time.Time) (*store.ModelPrice, error)

func (f strategyPrices) Effective(ctx context.Context, model string, at time.Time) (*store.ModelPrice, error) {
	return f(ctx, model, at)
}

func strategyFixture(t *testing.T, prices strategyPrices) *strategyEngine {
	t.Helper()
	if prices == nil {
		prices = func(context.Context, string, time.Time) (*store.ModelPrice, error) {
			t.Error("unexpected price lookup")
			return nil, errors.New("unexpected price lookup")
		}
	}
	s, err := NewStrategyEngine(prices, testLogger())
	require.NoError(t, err)
	return s.(*strategyEngine)
}

func strategyCandidates(t *testing.T, count int) []Candidate {
	t.Helper()
	f := newFixture(t, Weighted, count)
	c := make([]Candidate, count)
	for i, row := range f.rows {
		c[i] = Candidate{Channel: row, UpstreamModel: fmt.Sprintf("up-%d", i+1)}
	}
	return c
}

func orderedIDs(t *testing.T, s *strategyEngine, model string, strategy RouteStrategy, c []Candidate) []uint64 {
	t.Helper()
	ordered, err := s.Order(t.Context(), model, strategy, c)
	require.NoError(t, err)
	ids := candidateIDs(&Plan{Candidates: ordered})
	all := append([]uint64(nil), ids...)
	slices.Sort(all)
	want := candidateIDs(&Plan{Candidates: c})
	slices.Sort(want)
	require.Equal(t, want, all, "must return exactly the supplied IDs")
	for _, out := range ordered {
		for _, in := range c {
			if out.Channel.ID == in.Channel.ID {
				require.Equal(t, in, out, "all candidate fields are preserved")
			}
		}
	}
	return ids
}

func TestStrategyConstructorAndInputValidation(t *testing.T) {
	var typedNil strategyPrices
	for _, tc := range []struct {
		name string
		repo store.PriceRepo
		log  *slog.Logger
	}{
		{"nil prices", nil, testLogger()},
		{"typed nil prices", typedNil, testLogger()},
		{"nil logger", strategyPrices(func(context.Context, string, time.Time) (*store.ModelPrice, error) { panic("constructor I/O") }), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewStrategyEngine(tc.repo, tc.log)
			require.Error(t, err)
			require.Nil(t, s)
		})
	}
	s := strategyFixture(t, nil) // Construction must not query prices.
	for _, strategy := range []RouteStrategy{Weighted, CostFirst, LatencyFirst, FallbackChain} {
		t.Run(string(strategy), func(t *testing.T) {
			empty, err := s.Order(t.Context(), "public", strategy, nil)
			require.NoError(t, err)
			require.Empty(t, empty)
			if strategy != CostFirst {
				require.Equal(t, []uint64{1}, orderedIDs(t, s, "public", strategy, strategyCandidates(t, 1)))
			}
		})
	}
	for _, tc := range []struct {
		name string
		edit func([]Candidate) []Candidate
	}{
		{"zero ID", func(c []Candidate) []Candidate { c[0].Channel.ID = 0; return c }},
		{"overflow ID", func(c []Candidate) []Candidate { c[0].Channel.ID = math.MaxUint64; return c }},
		{"disabled", func(c []Candidate) []Candidate { c[0].Channel.Enabled = false; return c }},
		{"name", func(c []Candidate) []Candidate { c[0].Channel.Name = ""; return c }},
		{"provider", func(c []Candidate) []Candidate { c[0].Channel.Provider = "mock"; return c }},
		{"URL", func(c []Candidate) []Candidate { c[0].Channel.BaseURL = "https://private:secret@host"; return c }},
		{"zero weight", func(c []Candidate) []Candidate { c[0].Channel.Weight = 0; return c }},
		{"negative weight", func(c []Candidate) []Candidate { c[0].Channel.Weight = -1; return c }},
		{"large weight", func(c []Candidate) []Candidate { c[0].Channel.Weight = 10001; return c }},
		{"mapping", func(c []Candidate) []Candidate { c[0].UpstreamModel = ""; return c }},
		{"invalid UTF8 mapping", func(c []Candidate) []Candidate { c[0].UpstreamModel = "\xff"; return c }},
		{"long mapping", func(c []Candidate) []Candidate { c[0].UpstreamModel = strings.Repeat("界", 129); return c }},
		{"duplicate", func(c []Candidate) []Candidate { return append(c, c[0]) }},
		{"65 candidates", func([]Candidate) []Candidate { return strategyCandidates(t, 65) }},
	} {
		for _, strategy := range []RouteStrategy{Weighted, CostFirst, LatencyFirst, FallbackChain} {
			t.Run(tc.name+"/"+string(strategy), func(t *testing.T) {
				out, err := s.Order(t.Context(), "public", strategy, tc.edit(strategyCandidates(t, 1)))
				require.Error(t, err)
				require.Nil(t, out)
				require.NotContains(t, err.Error(), "secret")
			})
		}
	}
	for _, model := range []string{"", "\xff", strings.Repeat("界", 129)} {
		_, err := s.Order(t.Context(), model, Weighted, nil)
		require.Error(t, err)
	}
	for _, strategy := range []RouteStrategy{Auto, "private-invalid"} {
		_, err := s.Order(t.Context(), "public", strategy, nil)
		require.Error(t, err)
	}
	c := strategyCandidates(t, 64)
	for i := range c {
		c[i].Channel.Weight = 10000
	}
	require.Len(t, orderedIDs(t, s, "public", Weighted, c), 64)
}

func TestStrategyWeightedExactTicketsAndConditionalPermutation(t *testing.T) {
	c := strategyCandidates(t, 3)
	c[0].Channel.Weight, c[1].Channel.Weight, c[2].Channel.Weight = 6, 3, 1
	weights := []int64{6, 3, 1}
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint(reverse), func(t *testing.T) {
			input := cloneCandidates(c)
			if reverse {
				slices.Reverse(input)
			}
			firstCounts := map[uint64]int{}
			for first := int64(0); first < 10; first++ {
				firstIndex := 0
				if first >= 6 {
					firstIndex = 1
				}
				if first == 9 {
					firstIndex = 2
				}
				firstCounts[uint64(firstIndex+1)]++
				remainingTotal := 10 - weights[firstIndex]
				secondCounts := map[uint64]int{}
				for second := int64(0); second < remainingTotal; second++ {
					s := strategyFixture(t, nil)
					step := 0
					s.ticket = func(n int64) int64 {
						defer func() { step++ }()
						switch step {
						case 0:
							require.Equal(t, int64(10), n)
							return first
						case 1:
							require.Equal(t, remainingTotal, n)
							return second
						default:
							return 0
						}
					}
					ids := orderedIDs(t, s, "public", Weighted, input)
					require.Equal(t, uint64(firstIndex+1), ids[0])
					ticket := second
					for j, weight := range weights {
						if j == firstIndex {
							continue
						}
						if ticket < weight {
							require.Equal(t, uint64(j+1), ids[1])
							break
						}
						ticket -= weight
					}
					secondCounts[ids[1]]++
					require.Equal(t, 3, step)
				}
				for j, weight := range weights {
					if j != firstIndex {
						require.Equal(t, int(weight), secondCounts[uint64(j+1)])
					}
				}
			}
			require.Equal(t, map[uint64]int{1: 6, 2: 3, 3: 1}, firstCounts)
		})
	}
}

func TestStrategyPriorityStrictChainAndSnapshots(t *testing.T) {
	for _, strategy := range []RouteStrategy{Weighted, CostFirst, LatencyFirst, FallbackChain} {
		t.Run(string(strategy), func(t *testing.T) {
			s := strategyFixture(t, func(context.Context, string, time.Time) (*store.ModelPrice, error) {
				return &store.ModelPrice{Currency: "CNY"}, nil
			})
			s.ticket = func(int64) int64 { return 0 }
			c := strategyCandidates(t, 3)
			c[0].Channel.Priority = -1
			c[0].Channel.Weight = 10000
			c[0].Channel.LatencyP99 = -1000
			c[1].Channel.Priority = 2
			c[2].Channel.Priority = 2
			before := cloneCandidates(c)
			want := []uint64{2, 3, 1}
			if strategy == FallbackChain {
				want = []uint64{1, 2, 3}
			}
			require.Equal(t, want, orderedIDs(t, s, "public", strategy, c))
			out, err := s.Order(t.Context(), "public", strategy, c)
			require.NoError(t, err)
			require.Equal(t, before, c)
			*out[0].Channel.LastCheckedAt = time.Time{}
			out[0].UpstreamModel = "changed output"
			require.Equal(t, before, c)
			out, err = s.Order(t.Context(), "public", strategy, c)
			require.NoError(t, err)
			stored := cloneCandidates(out)
			*c[0].Channel.LastCheckedAt = time.Time{}
			c[1].Channel.Name = "changed input"
			require.Equal(t, stored, out)
		})
	}
}

func TestStrategyCostExactSumUnknownAndSingleSnapshot(t *testing.T) {
	c := strategyCandidates(t, 8)
	c[0].UpstreamModel, c[1].UpstreamModel = "huge", "larger"
	c[2].UpstreamModel, c[3].UpstreamModel = "zero", "missing"
	c[4].UpstreamModel, c[5].UpstreamModel = "huge", "missing"
	c[6].UpstreamModel, c[7].UpstreamModel = "huge", "sumTie"
	c[6].Channel.Priority = -1 // Same model must not be queried again across groups.
	prices := map[string]*store.ModelPrice{
		"huge":   {Currency: "CNY", InputPricePerM: math.MaxInt64, OutputPricePerM: 1},
		"larger": {Currency: "CNY", InputPricePerM: math.MaxInt64, OutputPricePerM: math.MaxInt64},
		"zero":   {Currency: "CNY"},
		"sumTie": {Currency: "CNY", InputPricePerM: math.MaxInt64 - 1, OutputPricePerM: 2},
	}
	now := time.Date(2026, 11, 3, 1, 2, 3, 4, time.FixedZone("local", 8*3600))
	calls := map[string]int{}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	s := strategyFixture(t, func(got context.Context, model string, at time.Time) (*store.ModelPrice, error) {
		require.Equal(t, ctx, got, "the caller's context must reach the repository unchanged")
		require.Equal(t, now.UTC(), at)
		require.Same(t, time.UTC, at.Location())
		calls[model]++
		if p := prices[model]; p != nil {
			return p, nil
		}
		return nil, fmt.Errorf("missing: %w", store.ErrNoPrice)
	})
	clockCalls := 0
	s.now = func() time.Time { clockCalls++; return now.Add(time.Duration(clockCalls-1) * time.Hour) }
	out, err := s.Order(ctx, "public", CostFirst, c)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 1, 5, 8, 2, 4, 6, 7}, candidateIDs(&Plan{Candidates: out}))
	require.Equal(t, 1, clockCalls)
	require.Equal(t, map[string]int{"huge": 1, "larger": 1, "zero": 1, "missing": 1, "sumTie": 1}, calls)
	t.Run("all missing", func(t *testing.T) {
		s := strategyFixture(t, func(context.Context, string, time.Time) (*store.ModelPrice, error) { return nil, store.ErrNoPrice })
		c := strategyCandidates(t, 3)
		slices.Reverse(c)
		require.Equal(t, []uint64{1, 2, 3}, orderedIDs(t, s, "public", CostFirst, c))
	})
	t.Run("different mapped prices", func(t *testing.T) {
		s := strategyFixture(t, func(_ context.Context, model string, _ time.Time) (*store.ModelPrice, error) {
			p := int64(8)
			if model == "up-2" {
				p = 2
			}
			return &store.ModelPrice{Currency: "CNY", InputPricePerM: p}, nil
		})
		require.Equal(t, []uint64{2, 1}, orderedIDs(t, s, "public", CostFirst, strategyCandidates(t, 2)))
	})
}

func TestStrategyCostErrorsAndCancellation(t *testing.T) {
	sentinel := errors.New("https://secret@private invalid SQL body")
	for _, tc := range []struct {
		name string
		p    *store.ModelPrice
		err  error
	}{
		{"nil price", nil, nil},
		{"negative input", &store.ModelPrice{Currency: "CNY", InputPricePerM: -1}, nil},
		{"negative output", &store.ModelPrice{Currency: "CNY", OutputPricePerM: -1}, nil},
		{"currency", &store.ModelPrice{Currency: "USD"}, nil},
		{"SQL error", nil, sentinel},
		{"context error", nil, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			s := strategyFixture(t, func(context.Context, string, time.Time) (*store.ModelPrice, error) { calls++; return tc.p, tc.err })
			out, err := s.Order(t.Context(), "public", CostFirst, strategyCandidates(t, 2))
			require.Error(t, err)
			require.Nil(t, out)
			require.Equal(t, 1, calls)
			require.NotContains(t, err.Error(), "secret")
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			}
		})
	}
	for _, strategy := range []RouteStrategy{Weighted, CostFirst, LatencyFirst, FallbackChain} {
		t.Run("pre-canceled/"+string(strategy), func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			cancel(sentinel)
			s := strategyFixture(t, nil)
			s.ticket = func(int64) int64 { t.Fatal("random draw after cancel"); return 0 }
			out, err := s.Order(ctx, "public", strategy, strategyCandidates(t, 2))
			require.Nil(t, out)
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, sentinel)
			require.NotContains(t, err.Error(), "secret")
		})
	}
	t.Run("cancel during lookup wins over no price", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		calls := 0
		s := strategyFixture(t, func(context.Context, string, time.Time) (*store.ModelPrice, error) {
			calls++
			cancel(sentinel)
			return nil, store.ErrNoPrice
		})
		_, err := s.Order(ctx, "public", CostFirst, strategyCandidates(t, 2))
		require.ErrorIs(t, err, context.Canceled)
		require.ErrorIs(t, err, sentinel)
		require.Equal(t, 1, calls)
	})
}

// This fixture models the documented model-keyed snapshot protocol, not real SQL.
func TestStrategyCostCacheProtocolWithFakeOnly(t *testing.T) {
	now := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	backing := map[string]int64{"up-1": 10, "up-2": 20}
	cache := map[string]*store.ModelPrice{}
	queries := 0
	s := strategyFixture(t, func(_ context.Context, model string, _ time.Time) (*store.ModelPrice, error) {
		if p := cache[model]; p != nil {
			cp := *p
			return &cp, nil
		}
		queries++
		p := &store.ModelPrice{Currency: "CNY", InputPricePerM: backing[model]}
		cache[model] = p
		cp := *p
		return &cp, nil
	})
	s.now = func() time.Time { return now }
	c := strategyCandidates(t, 2)
	require.Equal(t, []uint64{1, 2}, orderedIDs(t, s, "public", CostFirst, c))
	require.Equal(t, 2, queries)
	backing["up-1"] = 30
	now = now.Add(time.Second)
	require.Equal(t, []uint64{1, 2}, orderedIDs(t, s, "public", CostFirst, c))
	require.Equal(t, 2, queries, "warm snapshot performs no fake backing read")
	clear(cache) // Explicit invalidation, no historical-time cache bypass.
	require.Equal(t, []uint64{2, 1}, orderedIDs(t, s, "public", CostFirst, c))
	require.Equal(t, 4, queries)
}

func TestStrategyRouteTimestampPrivateSeamAndExistingBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := time.Now().UTC()
		f := newFixture(t, CostFirst, 1)
		resolve := f.models.resolve
		f.models.resolve = func(ctx context.Context, name string) (*store.ModelAlias, error) {
			time.Sleep(50 * time.Millisecond) // synctest virtual time, no wall-clock sleep.
			return resolve(ctx, name)
		}
		calls := 0
		s := strategyFixture(t, func(ctx context.Context, model string, at time.Time) (*store.ModelPrice, error) {
			calls++
			require.Equal(t, started, at, "resolver time must not shift the price snapshot")
			d, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, 150*time.Millisecond, time.Until(d))
			return &store.ModelPrice{Currency: "CNY"}, nil
		})
		s.now = func() time.Time { t.Fatal("Route snapshot must override standalone clock"); return time.Time{} }
		r := fixtureRouter(t, f, s, 200*time.Millisecond)
		p, err := r.Route(t.Context(), "public", Auto)
		require.NoError(t, err)
		require.Equal(t, CostFirst, p.Strategy)
		require.Equal(t, []uint64{1}, candidateIDs(p))
		require.Equal(t, 1, calls)
	})
}

func TestStrategyLatencyThresholdIsolationAndDisplayIgnored(t *testing.T) {
	s := strategyFixture(t, nil)
	now := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return now }
	c := strategyCandidates(t, 3)
	c[0].Channel.LatencyP99, c[1].Channel.LatencyP99 = 0, math.MaxInt32
	c[1].Channel.ErrorRateCache = 1
	require.Equal(t, []uint64{1, 2, 3}, orderedIDs(t, s, "public", LatencyFirst, c))
	for i := 0; i < 4; i++ {
		s.ObserveSuccess(t.Context(), 2, "public", 10*time.Millisecond)
	}
	require.Equal(t, []uint64{1, 2, 3}, orderedIDs(t, s, "public", LatencyFirst, c))
	s.ObserveSuccess(t.Context(), 2, "public", 0)
	require.Equal(t, []uint64{2, 1, 3}, orderedIDs(t, s, "public", LatencyFirst, c))
	require.Equal(t, []uint64{1, 2, 3}, orderedIDs(t, s, "other", LatencyFirst, c))
	for i := 0; i < 5; i++ {
		s.ObserveSuccess(t.Context(), 3, "other", 0)
		s.ObserveSuccess(t.Context(), 1, "public", 10*time.Millisecond)
	}
	require.Equal(t, []uint64{3, 1, 2}, orderedIDs(t, s, "other", LatencyFirst, c))
	require.Equal(t, []uint64{1, 2, 3}, orderedIDs(t, s, "public", LatencyFirst, c), "equal p99 uses ID")
	for i := 0; i < 5; i++ {
		s.ObserveSuccess(t.Context(), 3, "public", 0)
	}
	require.Equal(t, []uint64{3, 1, 2}, orderedIDs(t, s, "public", LatencyFirst, c), "zero duration is known")
	c[2].Channel.Priority = -1
	require.Equal(t, []uint64{1, 2, 3}, orderedIDs(t, s, "public", LatencyFirst, c), "latency never overrides priority")
}

func TestStrategyLatencyNearestRankAnd256ArrivalBound(t *testing.T) {
	for _, tc := range []struct{ n, rank int }{{5, 5}, {100, 99}, {101, 100}, {256, 254}} {
		t.Run(fmt.Sprint(tc.n), func(t *testing.T) {
			s := strategyFixture(t, nil)
			now := time.Now()
			s.now = func() time.Time { return now }
			for i := tc.n; i >= 1; i-- {
				s.ObserveSuccess(t.Context(), 1, "public", time.Duration(i))
			}
			c := strategyCandidates(t, 1)
			ranks := s.latencies("public", c, now)
			require.Equal(t, strategyRank{known: true, value: uint64(tc.rank)}, ranks[1])
		})
	}
	s := strategyFixture(t, nil)
	now := time.Now()
	s.now = func() time.Time { return now }
	for i := 0; i < 256; i++ {
		s.ObserveSuccess(t.Context(), 1, "public", time.Hour)
	}
	for i := 0; i < 256; i++ {
		s.ObserveSuccess(t.Context(), 1, "public", time.Duration(i))
	}
	e := s.entries[latencyKey{1, "public"}]
	require.Equal(t, 256, e.count)
	for i := 0; i < e.count; i++ {
		require.Equal(t, time.Duration(i), e.samples[i].elapsed)
	}
	require.Equal(t, strategyRank{known: true, value: 253}, s.latencies("public", strategyCandidates(t, 1), now)[1])
}

func TestStrategyLatencyWindowExactBoundariesAndPruning(t *testing.T) {
	s := strategyFixture(t, nil)
	start := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	now := start
	s.now = func() time.Time { return now }
	for i := 0; i < 5; i++ {
		s.ObserveSuccess(t.Context(), 2, "public", time.Second)
	}
	c := strategyCandidates(t, 2)
	now = start.Add(time.Minute - time.Nanosecond)
	require.Equal(t, []uint64{2, 1}, orderedIDs(t, s, "public", LatencyFirst, c))
	now = start.Add(time.Minute)
	require.Equal(t, []uint64{1, 2}, orderedIDs(t, s, "public", LatencyFirst, c))
	require.Zero(t, s.entries[latencyKey{2, "public"}].count, "read removes exact cutoff samples")
	for i := 0; i < 5; i++ {
		s.ObserveSuccess(t.Context(), 2, "public", time.Second)
	}
	now = now.Add(time.Minute)
	s.ObserveSuccess(t.Context(), 2, "public", 0)
	require.Equal(t, 1, s.entries[latencyKey{2, "public"}].count, "write removes expired samples")
	for i := 0; i < 4; i++ {
		s.ObserveSuccess(t.Context(), 2, "public", 0)
	}
	require.Equal(t, []uint64{2, 1}, orderedIDs(t, s, "public", LatencyFirst, c), "now is included")
	now = now.Add(-time.Nanosecond)
	require.Equal(t, []uint64{1, 2}, orderedIDs(t, s, "public", LatencyFirst, c), "future samples are excluded after clock adjustment")
}

func TestStrategyObservationValidationSafeLogsAndCanceledContext(t *testing.T) {
	s := strategyFixture(t, nil)
	var logs bytes.Buffer
	s.logger = slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			a.Key = "ts"
		}
		return a
	}}))
	ctx := middleware.WithRequestID(t.Context(), "observation-request")
	for _, tc := range []struct {
		id       int64
		model    string
		duration time.Duration
	}{
		{0, "private-secret", 0}, {-1, "private-secret", 0}, {1, "", 0}, {1, "\xff", 0},
		{1, strings.Repeat("界", 129), 0}, {1, "private-secret", -1},
	} {
		s.ObserveSuccess(ctx, tc.id, tc.model, tc.duration)
	}
	require.Empty(t, s.entries)
	require.Equal(t, 6, strings.Count(logs.String(), "observation-request"))
	require.Contains(t, logs.String(), `"ts":`)
	require.NotContains(t, logs.String(), "private-secret")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	s.ObserveSuccess(canceled, math.MaxInt64, strings.Repeat("界", 128), 0)
	require.Len(t, s.entries, 1, "observation uses context values, not cancellation as a success verdict")
}

func TestStrategyConcurrentOrderAndObserveSnapshots(t *testing.T) {
	s := strategyFixture(t, func(context.Context, string, time.Time) (*store.ModelPrice, error) {
		return &store.ModelPrice{Currency: "CNY"}, nil
	})
	c := strategyCandidates(t, 3)
	before := cloneCandidates(c)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 24; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for n := 0; n < 20; n++ {
				s.ObserveSuccess(t.Context(), int64(n%3+1), "public", time.Duration(n))
				for _, strategy := range []RouteStrategy{Weighted, CostFirst, LatencyFirst, FallbackChain} {
					out, err := s.Order(t.Context(), "public", strategy, c)
					if err != nil {
						t.Error(err)
						return
					}
					ids := candidateIDs(&Plan{Candidates: out})
					slices.Sort(ids)
					if !slices.Equal(ids, []uint64{1, 2, 3}) {
						t.Error("invalid permutation")
						return
					}
					*out[0].Channel.LastCheckedAt = time.Time{}
					out[0].Channel.Name = "local output mutation"
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Equal(t, before, c)
	require.Len(t, s.entries, 3)
	for _, entry := range s.entries {
		require.LessOrEqual(t, entry.count, 256)
	}
}
