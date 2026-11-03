package router

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func strategyPriceRow(model string, input, output int64, at time.Time) store.ModelPrice {
	return store.ModelPrice{Model: model, InputPricePerM: input, OutputPricePerM: output, Currency: "CNY", EffectiveFrom: at, Enabled: true}
}

func TestStrategyRealMySQLPricesVersionsCacheAndInvalidation(t *testing.T) {
	db := routerTestDB(t)
	counter := &queryCounter{Interface: logger.Discard}
	db = db.Session(&gorm.Session{Logger: counter})
	cache := store.NewConfigCache(time.Minute)
	prices := store.NewPriceRepo(db, cache)
	now := time.Now().UTC().Truncate(time.Millisecond)
	rows := []store.ModelPrice{
		strategyPriceRow("up-a", 10, 20, now.Add(-2*time.Hour)),
		strategyPriceRow("up-a", 2, 3, now.Add(-time.Hour)),
		strategyPriceRow("up-a", 0, 0, now.Add(time.Hour)), // Not yet effective.
		strategyPriceRow("up-a", 0, 0, now.Add(-time.Minute)),
		strategyPriceRow("up-b", 3, 5, now.Add(-time.Hour)),
	}
	rows[3].Enabled = false
	require.NoError(t, db.Create(&rows).Error)
	engine, err := NewStrategyEngine(prices, testLogger())
	require.NoError(t, err)
	s := engine.(*strategyEngine)
	c := strategyCandidates(t, 5)
	c[0].UpstreamModel, c[1].UpstreamModel = "up-a", "up-b"
	c[2].UpstreamModel, c[3].UpstreamModel, c[4].UpstreamModel = "missing", "up-a", "missing"
	c[3].Channel.Priority = -1
	before := counter.count.Load()
	require.Equal(t, []uint64{1, 2, 3, 5, 4}, orderedIDs(t, s, "public", CostFirst, c))
	require.EqualValues(t, 3, counter.count.Load()-before, "one real query per distinct mapped model, including missing/cross-priority duplicates")
	p, err := prices.Effective(t.Context(), "up-a", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, rows[1].ID, p.ID, "newest enabled effective version, excluding future/disabled")
	require.Equal(t, int64(2), p.InputPricePerM)
	require.Equal(t, int64(3), p.OutputPricePerM)
	known := []Candidate{c[0], c[1], c[3]}
	before = counter.count.Load()
	for i := 0; i < 3; i++ {
		require.Equal(t, []uint64{1, 2, 4}, orderedIDs(t, s, "public", CostFirst, known))
	}
	require.Equal(t, before, counter.count.Load(), "known-price warm orders perform zero SQL")
	before = counter.count.Load()
	orderedIDs(t, s, "public", CostFirst, c)
	require.EqualValues(t, 1, counter.count.Load()-before, "ErrNoPrice is not negatively cached; duplicate missing still queries once per Order")

	// A real newly effective version remains hidden by the documented current-price cache.
	changed := strategyPriceRow("up-a", 30, 40, time.Now().UTC().Add(-time.Second).Truncate(time.Millisecond))
	require.NoError(t, db.Create(&changed).Error)
	before = counter.count.Load()
	require.Equal(t, []uint64{1, 2, 4}, orderedIDs(t, s, "public", CostFirst, known))
	require.Equal(t, before, counter.count.Load(), "live current-time Order still sees its warm snapshot without SQL")
	p.InputPricePerM = math.MaxInt64 // Repo returns independent values, not its cached pointer.
	p, err = prices.Effective(t.Context(), "up-a", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, int64(2), p.InputPricePerM)
	require.Equal(t, before, counter.count.Load())
	cache.InvalidateAll()
	before = counter.count.Load()
	require.Equal(t, []uint64{2, 1, 4}, orderedIDs(t, s, "public", CostFirst, known))
	require.EqualValues(t, 2, counter.count.Load()-before)
	p, err = prices.Effective(t.Context(), "up-a", time.Now().UTC())
	require.NoError(t, err)
	require.Equal(t, changed.ID, p.ID)
	require.Equal(t, int64(30), p.InputPricePerM)
	require.Equal(t, int64(40), p.OutputPricePerM)
}

func TestStrategyRealMySQLPriceErrorsCancellationAndNonCostZeroSQL(t *testing.T) {
	db := routerTestDB(t)
	counter := &queryCounter{Interface: logger.Discard}
	db = db.Session(&gorm.Session{Logger: counter})
	cache := store.NewConfigCache(time.Minute)
	prices := store.NewPriceRepo(db, cache)
	before := counter.count.Load()
	engine, err := NewStrategyEngine(prices, testLogger())
	require.NoError(t, err)
	s := engine.(*strategyEngine)
	c := strategyCandidates(t, 2)
	for _, strategy := range []RouteStrategy{Weighted, LatencyFirst, FallbackChain} {
		orderedIDs(t, s, "public", strategy, c)
	}
	require.Equal(t, before, counter.count.Load(), "constructor and non-cost orders never query SQL")
	cause := errors.New("private caller cancellation")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	_, err = s.Order(ctx, "public", CostFirst, c)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, cause)
	require.Equal(t, before, counter.count.Load(), "pre-cancel must not reach real PriceRepo")

	// Closing only this test's own pool gives a genuine database/sql failure.
	pool, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, pool.Close())
	_, repoErr := prices.Effective(t.Context(), c[0].UpstreamModel, time.Now().UTC())
	require.Error(t, repoErr)
	require.NotErrorIs(t, repoErr, store.ErrNoPrice)
	out, err := s.Order(t.Context(), "public", CostFirst, c)
	require.Nil(t, out)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.Unwrap(repoErr))
	require.NotErrorIs(t, err, store.ErrNoPrice)
	require.Equal(t, "router: price lookup failed", err.Error())
}

func TestStrategyRealMySQLBlockedPriceLockAndSnapshot(t *testing.T) {
	db := routerTestDB(t)
	row := strategyPriceRow("up-1", 1, 2, time.Now().UTC().Add(-time.Minute))
	require.NoError(t, db.Create(&row).Error)
	engine, err := NewStrategyEngine(store.NewPriceRepo(db, store.NewConfigCache(time.Minute)), testLogger())
	require.NoError(t, err)
	s := engine.(*strategyEngine)
	entered := make(chan struct{})
	release, wg := strategyBarrier(t)
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("162:block-price", func(tx *gorm.DB) {
		if tx.Statement.Table == "model_prices" {
			close(entered)
			<-release
		}
	}))
	c := strategyCandidates(t, 1)
	baseline := cloneCandidates(c)
	wg.Add(1)
	go func() {
		defer wg.Done()
		out, err := s.Order(context.WithoutCancel(t.Context()), "public", CostFirst, c)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(out, baseline) {
			t.Error("caller mutation crossed the captured candidate snapshot")
		}
	}()
	awaitStrategySignal(t, entered)
	assertStrategyPeersProgress(t, s, wg)
	// The real PriceRepo call has started after candidate copying; input changes are independent.
	c[0].Channel.Name = "mutated after price entry"
	*c[0].Channel.LastCheckedAt = time.Time{}
	c[0].UpstreamModel = "changed after snapshot"
}

func TestStrategyRealRouterMySQLFourStrategiesAndSharedSamples(t *testing.T) {
	db := routerTestDB(t)
	cache := store.NewConfigCache(time.Minute)
	channels, models := store.NewChannelRepo(db, cache), store.NewModelRepo(db, cache)
	ids := make([]uint64, 4)
	for i, upstream := range []string{"route-a", "route-b", "route-missing", "route-b"} {
		ch := store.Channel{Name: fmt.Sprintf("strategy-channel-%d", i), Provider: "openai", BaseURL: "https://strategy.invalid/v1", Priority: 10, Weight: int32(6 - i), Enabled: true, LatencyP99: math.MaxInt32}
		if i == 3 {
			ch.Priority, ch.Weight = -1, 10000
		}
		require.NoError(t, channels.Create(t.Context(), &ch, []store.ChannelModel{{Model: "strategy-public", UpstreamModel: upstream}}, nil))
		ids[i] = ch.ID
	}
	alias := store.ModelAlias{Name: "strategy-public", Strategy: string(CostFirst), DefaultChannelID: &ids[2], FallbackIDs: fmt.Sprintf("[%d,%d,%d]", ids[3], ids[0], ids[1])}
	require.NoError(t, models.Create(t.Context(), &alias))
	priceRows := []store.ModelPrice{strategyPriceRow("route-a", 10, 10, time.Now().UTC().Add(-time.Hour)), strategyPriceRow("route-b", 2, 3, time.Now().UTC().Add(-time.Hour))}
	require.NoError(t, db.Create(&priceRows).Error)
	var mu sync.Mutex
	var priceModels []string
	var priceTimes []time.Time
	prices := store.NewPriceRepo(db, cache)
	s := strategyFixture(t, func(ctx context.Context, model string, at time.Time) (*store.ModelPrice, error) {
		mu.Lock()
		priceModels = append(priceModels, model)
		priceTimes = append(priceTimes, at)
		mu.Unlock()
		return prices.Effective(ctx, model, at)
	})
	s.ticket = func(n int64) int64 { return n - 1 }
	health := &healthFixture{available: func(context.Context, int64) bool { return true }}
	inspector := &inspectorFixture{inspect: func(context.Context, int64) (BreakerView, error) { return BreakerView{BreakerClosed, true}, nil }}
	r, err := NewRouter(models, channels, health, inspector, s, RouterOptions{DefaultStrategy: Weighted}, testLogger())
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		s.ObserveSuccess(t.Context(), int64(ids[0]), alias.Name, 20*time.Millisecond)
		s.ObserveSuccess(t.Context(), int64(ids[1]), alias.Name, 10*time.Millisecond)
		s.ObserveSuccess(t.Context(), int64(ids[3]), alias.Name, 0)
	}
	for _, tc := range []struct {
		strategy RouteStrategy
		want     []uint64
	}{
		{Auto, []uint64{ids[1], ids[0], ids[2], ids[3]}},
		{Weighted, []uint64{ids[2], ids[1], ids[0], ids[3]}},
		{LatencyFirst, []uint64{ids[1], ids[0], ids[2], ids[3]}},
		{FallbackChain, []uint64{ids[2], ids[3], ids[0], ids[1]}},
	} {
		t.Run(string(tc.strategy), func(t *testing.T) {
			p, err := r.Route(t.Context(), alias.Name, tc.strategy)
			require.NoError(t, err)
			require.Equal(t, alias.Name, p.Model)
			require.Equal(t, tc.want, candidateIDs(p))
			for _, c := range p.Candidates {
				require.NotEqual(t, alias.Name, c.UpstreamModel)
			}
		})
	}
	require.Equal(t, []string{"route-a", "route-b", "route-missing"}, priceModels, "noncost strategies make no price calls; duplicate model crosses priority once")
	for _, at := range priceTimes {
		require.Equal(t, priceTimes[0], at)
		require.Same(t, time.UTC, at.Location())
	}
	// Snapshots remain independent on a later real Route and cannot contaminate SQL/cache.
	p, err := r.Route(t.Context(), alias.Name, LatencyFirst)
	require.NoError(t, err)
	p.Candidates[0].Channel.Name = "private mutation"
	slices.Reverse(p.Candidates)
	p, err = r.Route(t.Context(), alias.Name, LatencyFirst)
	require.NoError(t, err)
	require.Equal(t, []uint64{ids[1], ids[0], ids[2], ids[3]}, candidateIDs(p))
	require.Equal(t, "strategy-channel-1", p.Candidates[0].Channel.Name)
	require.Zero(t, health.reports.Load())
	require.Zero(t, inspector.acquires.Load())
	require.Zero(t, inspector.completes.Load())
}
