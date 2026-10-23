package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func channelAdminDB(t *testing.T) *gorm.DB {
	t.Helper()
	opts := testOptions(t)
	opts.Database = ""
	conn, err := sql.Open("mysql", opts.DSN())
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, conn.PingContext(ctx), "14.1 requires real MySQL, never SKIP")
	return migratedTestDB(t)
}

func adminRepoChannel(t *testing.T, r ChannelRepo, name string) *Channel {
	t.Helper()
	ch := &Channel{Name: name, Provider: "openai", BaseURL: "http://localhost", Enabled: true, Weight: 1}
	require.NoError(t, r.Create(context.Background(), ch, []ChannelModel{{Model: "old", UpstreamModel: "old-up"}}, [][]byte{[]byte("cipher-old")}))
	return ch
}

func TestChannelAdminRepositoryListsAndReplacement(t *testing.T) {
	db := channelAdminDB(t)
	cache := testCache()
	r, k := NewChannelRepo(db, cache), NewKeyRepo(db)
	ctx := context.Background()
	all, err := r.List(ctx)
	require.NoError(t, err)
	assert.NotNil(t, all)
	assert.Empty(t, all)
	a := adminRepoChannel(t, r, "a")
	b := adminRepoChannel(t, r, "b")
	checked := time.Now().UTC().Truncate(time.Millisecond)
	require.NoError(t, db.Model(&Channel{}).Where("id = ?", a.ID).Updates(map[string]any{
		"health_status": "cooling", "error_rate_cache": .5, "latency_p99_cache": 99, "last_checked_at": checked,
	}).Error)
	before, err := r.Get(ctx, int64(a.ID))
	require.NoError(t, err)
	keysBefore, err := k.ListByChannel(ctx, int64(a.ID))
	require.NoError(t, err)
	_, err = r.ListActiveByModel(ctx, "old") // Fill old cache; CRUD must stay fresh.
	require.NoError(t, err)
	before.Name, before.Provider, before.BaseURL = "updated", "openai", "https://localhost/prefix"
	before.Priority, before.Weight, before.Enabled = 0, 10000, false
	mappings := []ChannelModel{{ChannelID: b.ID, Model: "z", UpstreamModel: "new-z"}, {Model: "a", UpstreamModel: "new-a"}}
	require.NoError(t, r.UpdateConfig(ctx, before, mappings))
	assert.Equal(t, b.ID, mappings[0].ChannelID, "repo must not mutate mapping input")
	got, err := r.Get(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, "updated", got.Name)
	assert.Equal(t, before.CreatedAt, got.CreatedAt)
	assert.Equal(t, "cooling", got.HealthStatus)
	assert.Equal(t, float32(.5), got.ErrorRateCache)
	assert.Equal(t, int32(99), got.LatencyP99)
	assert.Equal(t, checked, *got.LastCheckedAt)
	assert.False(t, got.Enabled)
	models, err := r.ListModels(ctx, int64(a.ID))
	require.NoError(t, err)
	require.Len(t, models, 2)
	assert.Equal(t, []string{"a", "z"}, []string{models[0].Model, models[1].Model})
	assert.Equal(t, a.ID, models[0].ChannelID)
	keysAfter, err := k.ListByChannel(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, keysBefore, keysAfter, "complete PUT leaves Key IDs, ciphertext and timestamps untouched")
	all, err = r.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, a.ID, all[0].ID)
	assert.Equal(t, "updated", all[0].Name)
	all[0].Name = "caller"
	*all[0].LastCheckedAt = time.Time{}
	models[0].UpstreamModel = "caller"
	again, err := r.List(ctx)
	require.NoError(t, err)
	assert.Equal(t, "updated", again[0].Name)
	assert.Equal(t, checked, *again[0].LastCheckedAt)
	models, err = r.ListModels(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, "new-a", models[0].UpstreamModel)
	var noChange atomic.Bool
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("141:freeze-updated-at", func(tx *gorm.DB) {
		if tx.Statement.Table == "channels" {
			tx.Statement.Dest.(map[string]any)["updated_at"] = got.UpdatedAt
		}
	}))
	require.NoError(t, db.Callback().Update().After("gorm:update").Register("141:observe-no-change", func(tx *gorm.DB) {
		if tx.Statement.Table == "channels" && tx.Error == nil && tx.RowsAffected == 0 {
			noChange.Store(true)
		}
	}))
	require.NoError(t, r.UpdateConfig(ctx, got, mappings), "same config succeeds")
	assert.True(t, noChange.Load(), "new UpdateConfig actually tolerates changed rows=0")
	require.NoError(t, db.Callback().Update().Remove("141:freeze-updated-at"))
	require.NoError(t, db.Callback().Update().Remove("141:observe-no-change"))
	require.NoError(t, r.UpdateConfig(ctx, got, nil))
	models, err = r.ListModels(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.NotNil(t, models)
	assert.Empty(t, models)
	_, err = r.ListModels(ctx, 99999)
	assert.ErrorIs(t, err, ErrNotFound)
	assert.ErrorIs(t, r.UpdateConfig(ctx, &Channel{ID: 99999}, mappings), ErrNotFound)
}

func TestChannelAdminOwnedKeys(t *testing.T) {
	db := channelAdminDB(t)
	r, k := NewChannelRepo(db, testCache()), NewKeyRepo(db)
	ctx := context.Background()
	a, b := adminRepoChannel(t, r, "a"), adminRepoChannel(t, r, "b")
	enc := []byte("unchanged-private-ciphertext")
	created, err := k.CreateOwned(ctx, int64(a.ID), enc)
	require.NoError(t, err)
	require.NotZero(t, created.ID)
	assert.Equal(t, int32(1), created.Weight)
	assert.Equal(t, int8(1), created.Status)
	assert.False(t, created.CreatedAt.IsZero())
	enc[0] = 'X'
	assert.Equal(t, byte('u'), created.ProviderKeyEnc[0])
	before, err := k.ListByChannel(ctx, int64(a.ID))
	require.NoError(t, err)
	var zeroRows atomic.Bool
	require.NoError(t, db.Callback().Update().After("gorm:update").Register("141:noop", func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_keys" && tx.RowsAffected == 0 && tx.Error == nil {
			zeroRows.Store(true)
		}
	}))
	defer db.Callback().Update().Remove("141:noop")
	updated, err := k.UpdateOwned(ctx, int64(a.ID), int64(created.ID), 1, 1)
	require.NoError(t, err)
	assert.True(t, zeroRows.Load(), "actually observe MySQL changed rows=0")
	assert.Equal(t, created.ProviderKeyEnc, updated.ProviderKeyEnc)
	updated, err = k.UpdateOwned(ctx, int64(a.ID), int64(created.ID), 10000, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(10000), updated.Weight)
	assert.Zero(t, updated.Status)
	after, err := k.ListByChannel(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, before[1].ProviderKeyEnc, after[1].ProviderKeyEnc)
	assert.Equal(t, before[1].CreatedAt, after[1].CreatedAt)
	_, err = k.UpdateOwned(ctx, int64(b.ID), int64(created.ID), 1, 1)
	assert.ErrorIs(t, err, ErrNotFound)
	assert.ErrorIs(t, k.RemoveOwned(ctx, int64(b.ID), int64(created.ID)), ErrNotFound)
	_, err = k.UpdateOwned(ctx, int64(a.ID), 9999, 1, 1)
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = k.CreateOwned(ctx, 99999, []byte("enc"))
	assert.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, k.RemoveOwned(ctx, int64(a.ID), int64(created.ID)))
	assert.ErrorIs(t, k.RemoveOwned(ctx, int64(a.ID), int64(created.ID)), ErrNotFound)
	assert.ErrorIs(t, r.Delete(ctx, 99999), ErrNotFound)
}

func TestChannelAdminRollbackAndReferences(t *testing.T) {
	db := channelAdminDB(t)
	r, k := NewChannelRepo(db, testCache()), NewKeyRepo(db)
	ctx := context.Background()
	a := adminRepoChannel(t, r, "rollback")
	before, err := r.Get(ctx, int64(a.ID))
	require.NoError(t, err)
	keysBefore, err := k.ListByChannel(ctx, int64(a.ID))
	require.NoError(t, err)
	var reached atomic.Int32
	require.NoError(t, db.Callback().Create().After("gorm:create").Register("141:observe-insert", func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_models" {
			_, ok := tx.Statement.ConnPool.(*sql.Tx)
			assert.True(t, ok, "mapping SQL must run in actual transaction")
			reached.Add(1)
		}
	}))
	defer db.Callback().Create().Remove("141:observe-insert")
	changed := *before
	changed.Name = "rolled-back-name"
	err = r.UpdateConfig(ctx, &changed, []ChannelModel{{Model: "duplicate", UpstreamModel: "one"}, {Model: "duplicate", UpstreamModel: "two"}})
	var driverErr *mysqldriver.MySQLError
	require.ErrorAs(t, err, &driverErr)
	assert.Equal(t, uint16(1062), driverErr.Number)
	assert.Equal(t, int32(2), reached.Load(), "first replacement inserted before second failed")
	after, err := r.Get(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, before, after, "including unchanged management updated_at after rollback")
	models, err := r.ListModels(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, []ChannelModel{{ChannelID: a.ID, Model: "old", UpstreamModel: "old-up"}}, models)
	keysAfter, err := k.ListByChannel(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, keysBefore, keysAfter)

	// A late delete failure must roll back already executed child deletes.
	injected := errors.New("controlled delete failure")
	var childDeletes atomic.Int32
	require.NoError(t, db.Callback().Delete().After("gorm:delete").Register("141:delete-child", func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_keys" || tx.Statement.Table == "channel_models" {
			childDeletes.Add(1)
		}
	}))
	require.NoError(t, db.Callback().Delete().Before("gorm:delete").Register("141:delete-parent", func(tx *gorm.DB) {
		if tx.Statement.Table == "channels" {
			tx.AddError(injected)
		}
	}))
	assert.ErrorIs(t, r.Delete(ctx, int64(a.ID)), injected)
	assert.Equal(t, int32(2), childDeletes.Load())
	require.NoError(t, db.Callback().Delete().Remove("141:delete-parent"))
	require.NoError(t, db.Callback().Delete().Remove("141:delete-child"))
	keysAfter, err = k.ListByChannel(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Equal(t, keysBefore, keysAfter)
	models, err = r.ListModels(ctx, int64(a.ID))
	require.NoError(t, err)
	assert.Len(t, models, 1)
	alias := &ModelAlias{Name: "alias", DefaultChannelID: &a.ID, FallbackIDs: "[1,2]"}
	mr := NewModelRepo(db, testCache())
	require.NoError(t, mr.Create(ctx, alias))
	require.NoError(t, r.Delete(ctx, int64(a.ID)))
	preserved, err := mr.Resolve(ctx, alias.Name)
	require.NoError(t, err)
	assert.Equal(t, a.ID, *preserved.DefaultChannelID)
	assert.Equal(t, []uint64{1, 2}, ParseFallbackIDs(preserved.FallbackIDs))
	for _, table := range []string{"channels", "channel_models", "channel_keys"} {
		var count int64
		require.NoError(t, db.Table(table).Count(&count).Error)
		assert.Zero(t, count)
	}
}

func TestChannelAdminDeleteKeyCompetition(t *testing.T) {
	db := channelAdminDB(t)
	r, k := NewChannelRepo(db, testCache()), NewKeyRepo(db)
	for _, action := range []string{"create", "update", "remove"} {
		for _, deleteFirst := range []bool{false, true} {
			t.Run(action+map[bool]string{false: "-key-first", true: "-delete-first"}[deleteFirst], func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				ch := adminRepoChannel(t, r, action)
				keys, err := k.ListByChannel(ctx, int64(ch.ID))
				require.NoError(t, err)
				locked, release := make(chan struct{}), make(chan struct{})
				var first atomic.Bool
				first.Store(true)
				require.NoError(t, db.Callback().Query().After("gorm:query").Register("141:parent-barrier", func(tx *gorm.DB) {
					if strings.Contains(tx.Statement.SQL.String(), "FOR UPDATE") && tx.Error == nil && first.CompareAndSwap(true, false) {
						close(locked)
						select {
						case <-release:
						case <-ctx.Done():
						}
					}
				}))
				defer db.Callback().Query().Remove("141:parent-barrier")
				keyOp := func() error {
					switch action {
					case "create":
						_, err := k.CreateOwned(ctx, int64(ch.ID), []byte("new-cipher"))
						return err
					case "update":
						_, err := k.UpdateOwned(ctx, int64(ch.ID), int64(keys[0].ID), 10, 0)
						return err
					default:
						return k.RemoveOwned(ctx, int64(ch.ID), int64(keys[0].ID))
					}
				}
				delOp := func() error { return r.Delete(ctx, int64(ch.ID)) }
				one, two := keyOp, delOp
				if deleteFirst {
					one, two = delOp, keyOp
				}
				done1, done2 := make(chan error, 1), make(chan error, 1)
				go func() { done1 <- one() }()
				select {
				case <-locked:
				case <-ctx.Done():
					t.Fatal("first operation never acquired parent lock")
				}
				go func() { done2 <- two() }()
				// Observe an actual InnoDB wait, not just goroutine launch order or
				// final counts. Only this serial fixture uses gaigate_test.
				observed := assert.Eventually(t, func() bool {
					var count int64
					result := db.WithContext(ctx).Raw("SELECT COUNT(*) FROM performance_schema.data_lock_waits").Scan(&count)
					return result.Error == nil && count > 0
				}, 3*time.Second, 10*time.Millisecond, "second SQL operation must wait for parent row")
				close(release)
				err1, err2 := <-done1, <-done2 // join both before inspecting or rebuilding DB
				require.True(t, observed)
				require.NoError(t, err1)
				if deleteFirst {
					assert.ErrorIs(t, err2, ErrNotFound)
				} else {
					require.NoError(t, err2)
				}
				var orphans int64
				require.NoError(t, db.Raw("SELECT COUNT(*) FROM channel_keys k LEFT JOIN channels c ON c.id=k.channel_id WHERE c.id IS NULL").Scan(&orphans).Error)
				assert.Zero(t, orphans)
			})
		}
	}
}

func TestChannelAdminCanceledAndDriverFailures(t *testing.T) {
	db := channelAdminDB(t)
	r, k := NewChannelRepo(db, testCache()), NewKeyRepo(db)
	ctx := context.Background()
	ch := adminRepoChannel(t, r, "failures")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	operations := []func(context.Context) error{
		func(c context.Context) error { _, err := r.List(c); return err },
		func(c context.Context) error { _, err := r.ListModels(c, int64(ch.ID)); return err },
		func(c context.Context) error { return r.UpdateConfig(c, ch, nil) },
		func(c context.Context) error { return r.Delete(c, int64(ch.ID)) },
		func(c context.Context) error { _, err := k.CreateOwned(c, int64(ch.ID), []byte("enc")); return err },
		func(c context.Context) error { _, err := k.UpdateOwned(c, int64(ch.ID), 1, 1, 1); return err },
		func(c context.Context) error { return k.RemoveOwned(c, int64(ch.ID), 1) },
	}
	for _, op := range operations {
		assert.ErrorIs(t, op(canceled), context.Canceled)
	}
	// Cancellation must also interrupt a real, already-waiting parent lock.
	tx := db.Begin()
	require.NoError(t, tx.Error)
	defer tx.Rollback()
	require.NoError(t, lockChannel(tx, int64(ch.ID)))
	waitCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { _, err := k.CreateOwned(waitCtx, int64(ch.ID), []byte("blocked")); done <- err }()
	observed := assert.Eventually(t, func() bool {
		var count int64
		return db.Raw("SELECT COUNT(*) FROM performance_schema.data_lock_waits").Scan(&count).Error == nil && count > 0
	}, 3*time.Second, 10*time.Millisecond)
	stop()
	select {
	case err := <-done:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("canceled SQL lock waiter did not exit")
	}
	require.NoError(t, tx.Rollback().Error)
	require.True(t, observed)
	remaining, err := k.ListByChannel(ctx, int64(ch.ID))
	require.NoError(t, err)
	require.Len(t, remaining, 1)
	assert.Equal(t, []byte("cipher-old"), remaining[0].ProviderKeyEnc)

	require.NoError(t, db.Exec("RENAME TABLE channel_keys TO channel_keys_unavailable").Error)
	for _, op := range operations[4:] {
		err := op(ctx)
		var driverErr *mysqldriver.MySQLError
		require.ErrorAs(t, err, &driverErr)
		assert.Equal(t, uint16(1146), driverErr.Number)
		assert.NotErrorIs(t, err, ErrNotFound)
	}
	require.NoError(t, db.Exec("RENAME TABLE channel_keys_unavailable TO channel_keys").Error)
	require.NoError(t, db.Exec("DROP TABLE channels").Error)
	for _, op := range operations {
		err := op(ctx)
		var driverErr *mysqldriver.MySQLError
		require.ErrorAs(t, err, &driverErr)
		assert.Equal(t, uint16(1146), driverErr.Number)
		assert.NotErrorIs(t, err, ErrNotFound)
	}
}
