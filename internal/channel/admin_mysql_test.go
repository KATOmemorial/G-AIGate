package channel

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/secret"
	"gaigate/internal/store"
)

// All SQL fixtures are serial and force gaigate_test, matching the existing B3
// convention. Unavailable SQL fails this new evidence instead of skipping it.
func adminMySQL(t *testing.T) *gorm.DB {
	t.Helper()
	port := 3307
	if value := os.Getenv("GAIGATE_TEST_MYSQL_PORT"); value != "" {
		var err error
		port, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	host, password := os.Getenv("GAIGATE_TEST_MYSQL_HOST"), os.Getenv("GAIGATE_TEST_MYSQL_PASSWORD")
	if host == "" {
		host = "127.0.0.1"
	}
	if password == "" {
		password = "gaigate_dev"
	}
	opts := store.Options{Host: host, Port: port, User: "root", Password: password, Database: "gaigate_test"}
	adminOpts := opts
	adminOpts.Database = ""
	conn, err := sql.Open("mysql", adminOpts.DSN())
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, conn.PingContext(ctx), "14.1 requires real MySQL")
	_, err = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS gaigate_test")
	require.NoError(t, err)
	_, err = conn.ExecContext(ctx, "CREATE DATABASE gaigate_test")
	require.NoError(t, err)
	require.NoError(t, store.RunMigrations(opts, "../../migrations"))
	db, err := store.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { pool, err := db.DB(); require.NoError(t, err); require.NoError(t, pool.Close()) })
	return db
}

func mysqlService(t *testing.T, db *gorm.DB) (*adminService, store.ChannelRepo, store.KeyRepo, *store.ConfigCache) {
	t.Helper()
	cache := store.NewConfigCache(time.Hour)
	c, k, m := store.NewChannelRepo(db, cache), store.NewKeyRepo(db), store.NewModelRepo(db, cache)
	require.NoError(t, m.Create(context.Background(), &store.ModelAlias{Name: "a"}))
	require.NoError(t, m.Create(context.Background(), &store.ModelAlias{Name: "b"}))
	s, err := NewAdminService(c, k, m, cache, []byte(strings.Repeat("m", 32)))
	require.NoError(t, err)
	return s.(*adminService), c, k, cache
}

func TestAdminMySQLLifecycleAndRealCacheInvalidation(t *testing.T) {
	db := adminMySQL(t)
	s, channels, keys, _ := mysqlService(t, db)
	ctx := context.Background()
	// Empty cache entry must also be evicted after creation.
	active, err := channels.ListActiveByModel(ctx, "a")
	require.NoError(t, err)
	assert.Empty(t, active)
	in := validInput()
	in.Enabled = true
	in.Keys = []string{"same-private", "same-private"}
	created, err := s.Create(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, in.Name, created.Name, "no trimming")
	assert.Equal(t, "unknown", created.HealthStatus)
	assert.Len(t, created.Keys, 2)
	active, err = channels.ListActiveByModel(ctx, "a")
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, created.ID, int64(active[0].ID))
	before, err := keys.ListByChannel(ctx, created.ID)
	require.NoError(t, err)
	require.Len(t, before, 2)
	assert.NotEqual(t, before[0].ProviderKeyEnc, before[1].ProviderKeyEnc, "random nonce for identical raw input")
	for _, key := range before {
		plain, err := secret.Decrypt(key.ProviderKeyEnc, s.masterKey)
		require.NoError(t, err)
		assert.Equal(t, "same-private", string(plain))
	}
	update := UpdateChannelInput{Name: "updated", Provider: "openai", BaseURL: "https://localhost/new",
		Priority: 0, Weight: 10000, Enabled: false, Models: []ModelMappingInput{{Model: "b", UpstreamModel: "mapped-b"}}}
	updated, err := s.Update(ctx, created.ID, update)
	require.NoError(t, err)
	assert.False(t, updated.Enabled)
	assert.Equal(t, created.CreatedAt, updated.CreatedAt)
	assert.Equal(t, update.Models, updated.Models)
	after, err := keys.ListByChannel(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, before, after, "PUT must preserve every Key byte, ID and created_at")
	active, err = channels.ListActiveByModel(ctx, "a")
	require.NoError(t, err)
	assert.Empty(t, active, "management commit invalidates old mapping cache immediately")
	active, err = channels.ListActiveByModel(ctx, "b")
	require.NoError(t, err)
	assert.Empty(t, active, "disabled remains explicit false")
	update.Enabled, update.Priority = true, 123
	_, err = s.Update(ctx, created.ID, update)
	require.NoError(t, err)
	active, err = channels.ListActiveByModel(ctx, "b")
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, int32(123), active[0].Priority)
	assert.Equal(t, "https://localhost/new", active[0].BaseURL)

	key, err := s.AddKey(ctx, created.ID, "rotated-private")
	require.NoError(t, err)
	assert.Equal(t, int32(1), key.Weight)
	updatedKey, err := s.UpdateKey(ctx, created.ID, key.ID, 10, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(10), updatedKey.Weight)
	assert.Zero(t, updatedKey.Status)
	assert.Equal(t, key.CreatedAt, updatedKey.CreatedAt, "new Key response uses the persisted DATETIME(3) precision")
	_, err = s.UpdateKey(ctx, created.ID+1000, key.ID, 1, 1)
	assert.ErrorIs(t, err, store.ErrNotFound)
	require.NoError(t, s.RemoveKey(ctx, created.ID, key.ID))
	require.NoError(t, s.Delete(ctx, created.ID))
	active, err = channels.ListActiveByModel(ctx, "b")
	require.NoError(t, err)
	assert.Empty(t, active)
	_, err = s.Get(ctx, created.ID)
	assert.ErrorIs(t, err, store.ErrNotFound)
	_, err = s.AddKey(ctx, created.ID, "private")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestAdminMySQLFailureBoundaries(t *testing.T) {
	db := adminMySQL(t)
	s, channels, _, cache := mysqlService(t, db)
	ctx := context.Background()
	var inserts atomic.Int32
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("141:count-writes", func(*gorm.DB) { inserts.Add(1) }))
	defer db.Callback().Create().Remove("141:count-writes")
	in := validInput()
	in.Keys = []string{"valid-private", "invalid\x00"}
	_, err := s.Create(ctx, in)
	assert.ErrorIs(t, err, errInvalidInput)
	assert.Zero(t, inserts.Load())
	var encryptions int
	s.encrypt = func(raw, master []byte) ([]byte, error) {
		encryptions++
		if encryptions == 2 {
			return nil, errors.New("controlled entropy failure with raw-private")
		}
		return secret.Encrypt(raw, master)
	}
	in.Keys = []string{"valid-private", "second-private"}
	got, err := s.Create(ctx, in)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Zero(t, inserts.Load(), "all encryption precedes any SQL write")
	s.encrypt = secret.Encrypt

	// First Key INSERT succeeds in the transaction; the second gets a real
	// MySQL column-width failure. No committed channel/model/Key may survive.
	var keyInserts atomic.Int32
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("141:late-key", func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_keys" && keyInserts.Add(1) == 2 {
			_, ok := tx.Statement.ConnPool.(*sql.Tx)
			assert.True(t, ok)
			tx.Statement.Dest.(*store.ChannelKey).ProviderKeyEnc = make([]byte, 600)
		}
	}))
	spy := &invalidationSpy{}
	s.invalidator = spy
	got, err = s.Create(ctx, in)
	var driverErr *mysqldriver.MySQLError
	require.ErrorAs(t, err, &driverErr)
	assert.Equal(t, uint16(1406), driverErr.Number)
	assert.Nil(t, got)
	assert.Equal(t, int32(2), keyInserts.Load())
	assert.GreaterOrEqual(t, inserts.Load(), int32(4))
	assert.Zero(t, spy.calls)
	require.NoError(t, db.Callback().Create().Remove("141:late-key"))
	for _, table := range []string{"channels", "channel_models", "channel_keys"} {
		var count int64
		require.NoError(t, db.Table(table).Count(&count).Error)
		assert.Zero(t, count)
	}

	s.invalidator = cache
	in.Enabled = true
	created, err := s.Create(ctx, in)
	require.NoError(t, err)
	_, err = channels.ListActiveByModel(ctx, "a")
	require.NoError(t, err)
	// A failed mapping replacement must leave both data and the existing cache.
	var cachedQueries atomic.Int32
	require.NoError(t, db.Callback().Row().Before("gorm:row").Register("141:cache-query", func(*gorm.DB) { cachedQueries.Add(1) }))
	defer db.Callback().Row().Remove("141:cache-query")
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("141:late-model", func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_models" {
			tx.Statement.Dest.(*store.ChannelModel).UpstreamModel = strings.Repeat("x", 129)
		}
	}))
	update := UpdateChannelInput{Name: "rollback", Provider: "openai", BaseURL: "http://localhost",
		Weight: 1, Enabled: true, Models: []ModelMappingInput{{Model: "b", UpstreamModel: "up-b"}}}
	_, err = s.Update(ctx, created.ID, update)
	require.ErrorAs(t, err, &driverErr)
	assert.Equal(t, uint16(1406), driverErr.Number)
	require.NoError(t, db.Callback().Create().Remove("141:late-model"))
	cachedQueries.Store(0)
	active, err := channels.ListActiveByModel(ctx, "a")
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, in.Name, active[0].Name)
	assert.Zero(t, cachedQueries.Load(), "rollback does not evict warmed cache: zero Scan SQL")
	unchanged, err := s.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, created, unchanged, "complete view including timestamps matches before transaction")

	// Commit success followed by response-read failure must still evict cache.
	readFailure := errors.New("controlled response SQL error private-cause")
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("141:response-read", func(tx *gorm.DB) {
		if tx.Statement.Table == "channels" {
			if _, locking := tx.Statement.Clauses["FOR"]; !locking {
				tx.AddError(readFailure)
			}
		}
	}))
	got, err = s.Update(ctx, created.ID, update)
	assert.ErrorIs(t, err, readFailure)
	assert.NotContains(t, err.Error(), "private-cause")
	assert.Nil(t, got)
	require.NoError(t, db.Callback().Query().Remove("141:response-read"))
	active, err = channels.ListActiveByModel(ctx, "a")
	require.NoError(t, err)
	assert.Empty(t, active, "committed write invalidated even though response assembly failed")
	committed, err := s.Get(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, "rollback", committed.Name)
	assert.Equal(t, update.Models, committed.Models)

	// The same boundary applies to Create: the response can fail after commit,
	// and a subsequent list is the way to discover the newly created channel.
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("141:create-response", func(tx *gorm.DB) {
		if tx.Statement.Table == "channels" {
			tx.AddError(readFailure)
		}
	}))
	got, err = s.Create(ctx, in)
	assert.ErrorIs(t, err, readFailure)
	assert.Nil(t, got)
	require.NoError(t, db.Callback().Query().Remove("141:create-response"))
	active, err = channels.ListActiveByModel(ctx, "a")
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, in.Name, active[0].Name)
	assert.NotEqual(t, uint64(created.ID), active[0].ID)
	confirmed, err := s.Get(ctx, int64(active[0].ID))
	require.NoError(t, err)
	assert.Len(t, confirmed.Keys, 2)
}
