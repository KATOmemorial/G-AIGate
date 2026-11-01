package router

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/access"
	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// SQL tests are deliberately serial. Only the explicitly selected test host /
// port / password are used; the database is always fixed to gaigate_test.
func routerTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	host := os.Getenv("GAIGATE_TEST_MYSQL_HOST")
	portText := os.Getenv("GAIGATE_TEST_MYSQL_PORT")
	require.NotEmpty(t, host, "explicit GAIGATE_TEST_MYSQL_HOST is required")
	require.NotEmpty(t, portText, "explicit GAIGATE_TEST_MYSQL_PORT is required")
	port, err := strconv.Atoi(portText)
	require.NoError(t, err)
	opts := store.Options{Host: host, Port: port, User: "root", Password: os.Getenv("GAIGATE_TEST_MYSQL_PASSWORD"), Database: "gaigate_test"}
	adminOpts := opts
	adminOpts.Database = ""
	admin, err := sql.Open("mysql", adminOpts.DSN())
	require.NoError(t, err)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	require.NoError(t, admin.PingContext(ctx), "real MySQL required; no SKIP")
	_, err = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS gaigate_test")
	require.NoError(t, err)
	_, err = admin.ExecContext(ctx, "CREATE DATABASE gaigate_test")
	require.NoError(t, err)
	require.NoError(t, store.RunMigrations(opts, "../../migrations"))
	db, err := store.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { pool, err := db.DB(); require.NoError(t, err); require.NoError(t, pool.Close()) })
	return db
}

type queryCounter struct {
	logger.Interface
	count atomic.Int64
}

func (c *queryCounter) Trace(context.Context, time.Time, func() (string, int64), error) {
	c.count.Add(1)
}

func TestRouterRealMySQLAliasMappingDeletionAndCacheIsolation(t *testing.T) {
	db := routerTestDB(t)
	counter := &queryCounter{Interface: logger.Discard}
	db = db.Session(&gorm.Session{Logger: counter})
	cache := store.NewConfigCache(time.Minute)
	models := store.NewModelRepo(db, cache)
	channels := store.NewChannelRepo(db, cache)
	checked := time.Date(2026, 10, 30, 3, 4, 5, 0, time.UTC)
	first := store.Channel{Name: "router-primary", Provider: "openai", BaseURL: "https://primary.invalid/v1", Priority: 10, Weight: 3, Enabled: true, HealthStatus: "disabled", LastCheckedAt: &checked}
	second := store.Channel{Name: "router-backup", Provider: "openai", BaseURL: "https://backup.invalid/v1", Priority: 1, Weight: 1, Enabled: true}
	require.NoError(t, channels.Create(t.Context(), &first, []store.ChannelModel{{Model: "router-public", UpstreamModel: "mapped-primary"}}, nil))
	require.NoError(t, channels.Create(t.Context(), &second, []store.ChannelModel{{Model: "router-public", UpstreamModel: "mapped-backup"}}, nil))
	ids, err := json.Marshal([]uint64{second.ID})
	require.NoError(t, err)
	alias := store.ModelAlias{Name: "router-public", DefaultChannelID: &first.ID, FallbackIDs: string(ids), Strategy: string(Weighted)}
	require.NoError(t, models.Create(t.Context(), &alias))
	cachedAlias, err := models.Resolve(t.Context(), alias.Name)
	require.NoError(t, err)
	cachedRows, err := channels.ListActiveByModel(t.Context(), alias.Name)
	require.NoError(t, err)
	require.Len(t, cachedRows, 2)
	baseline := []store.Channel{cloneChannel(cachedRows[0]), cloneChannel(cachedRows[1])}
	var retained []Candidate
	var orders, inspections atomic.Int64
	health := &healthFixture{available: func(context.Context, int64) bool { return true }}
	inspector := &inspectorFixture{inspect: func(context.Context, int64) (BreakerView, error) {
		inspections.Add(1)
		return BreakerView{BreakerClosed, true}, nil
	}}
	r, err := NewRouter(models, channels, health, inspector, orderFixture(func(_ context.Context, model string, strategy RouteStrategy, c []Candidate) ([]Candidate, error) {
		orders.Add(1)
		require.Equal(t, alias.Name, model)
		if strategy == Weighted {
			slices.Reverse(c)
		}
		retained = c
		return c, nil
	}), RouterOptions{DefaultStrategy: CostFirst}, testLogger())
	require.NoError(t, err)
	before := counter.count.Load()
	plan, err := r.Route(t.Context(), alias.Name, Auto)
	require.NoError(t, err)
	require.EqualValues(t, 2, counter.count.Load()-before, "cached alias/list, but both mappings must be read from SQL")
	require.Equal(t, Weighted, plan.Strategy)
	require.Equal(t, []uint64{second.ID, first.ID}, candidateIDs(plan))
	require.Equal(t, "mapped-backup", plan.Candidates[0].UpstreamModel)
	require.Equal(t, "mapped-primary", plan.Candidates[1].UpstreamModel)
	require.EqualValues(t, 1, orders.Load())
	require.EqualValues(t, 2, inspections.Load())
	*retained[1].Channel.LastCheckedAt = checked.Add(time.Hour)
	retained[1].Channel.Name = "orderer changed"
	*plan.Candidates[1].Channel.LastCheckedAt = checked.Add(2 * time.Hour)
	plan.Candidates[1].Channel.Name = "plan changed"
	plan.Candidates[0].UpstreamModel = "plan changed"
	*cachedAlias.DefaultChannelID = 999
	before = counter.count.Load()
	readAlias, err := models.Resolve(t.Context(), alias.Name)
	require.NoError(t, err)
	readRows, err := channels.ListActiveByModel(t.Context(), alias.Name)
	require.NoError(t, err)
	require.Equal(t, before, counter.count.Load(), "verification must read the existing cache, not reload pristine SQL")
	require.Equal(t, first.ID, *readAlias.DefaultChannelID)
	require.Equal(t, baseline, readRows)
	before = counter.count.Load()
	chain, err := r.Route(t.Context(), alias.Name, FallbackChain)
	require.NoError(t, err)
	require.EqualValues(t, 4, counter.count.Load()-before, "real Get and GetModel for both chain references")
	require.Equal(t, []uint64{first.ID, second.ID}, candidateIDs(chain))
	require.Equal(t, "mapped-primary", chain.Candidates[0].UpstreamModel)
	require.NoError(t, channels.Delete(t.Context(), int64(second.ID)))
	readAlias, err = models.Resolve(t.Context(), alias.Name)
	require.NoError(t, err)
	require.Equal(t, string(ids), readAlias.FallbackIDs, "deleting a channel retains its alias reference")
	chain, err = r.Route(t.Context(), alias.Name, FallbackChain)
	require.NoError(t, err)
	require.Equal(t, []uint64{first.ID}, candidateIDs(chain))
	// The stale List cache still contains the deleted channel: its real missing
	// GetModel mapping must filter it, rather than fabricate an endpoint.
	weighted, err := r.Route(t.Context(), alias.Name, Weighted)
	require.NoError(t, err)
	require.Equal(t, []uint64{first.ID}, candidateIDs(weighted))
	stale, err := channels.ListActiveByModel(t.Context(), alias.Name)
	require.NoError(t, err)
	require.Len(t, stale, 2)
	cache.InvalidateChannels(alias.Name)
	fresh, err := channels.ListActiveByModel(t.Context(), alias.Name)
	require.NoError(t, err)
	require.Len(t, fresh, 1)
	require.Equal(t, first.ID, fresh[0].ID)
	missing, err := r.Route(t.Context(), "router-not-configured", Auto)
	require.Nil(t, missing)
	require.ErrorIs(t, err, access.ErrModelNotFound)
	require.ErrorIs(t, err, store.ErrNotFound)
	require.Zero(t, health.reports.Load())
	require.Zero(t, inspector.acquires.Load())
	require.Zero(t, inspector.completes.Load())
}
