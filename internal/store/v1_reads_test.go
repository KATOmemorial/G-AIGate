package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestV1RepositoryReadsMySQL(t *testing.T) {
	// New 13.3 SQL evidence must fail if MySQL is unavailable, even though the
	// historical migration fixture permits skipping on developer machines.
	opts := testOptions(t)
	opts.Database = ""
	admin, err := sql.Open("mysql", opts.DSN())
	require.NoError(t, err)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, admin.PingContext(ctx), "13.3 requires real MySQL")
	db := migratedTestDB(t)
	models := NewModelRepo(db, testCache())
	channels := NewChannelRepo(db, testCache())
	ctx = context.Background()

	t.Run("all aliases sorted fresh and isolated", func(t *testing.T) {
		empty, err := models.List(ctx)
		require.NoError(t, err)
		assert.NotNil(t, empty)
		assert.Empty(t, empty)
		fallback := uint64(999)
		for _, alias := range []ModelAlias{
			{Name: "z", DisplayName: "Z", DefaultChannelID: &fallback, FallbackIDs: "[998]"},
			{Name: "a", DisplayName: "A", Strategy: "cost_first"},
			{Name: "m", DisplayName: "M"},
		} {
			require.NoError(t, models.Create(ctx, &alias))
		}
		aliases, err := models.List(ctx)
		require.NoError(t, err)
		require.Len(t, aliases, 3)
		assert.Equal(t, []string{"a", "m", "z"}, []string{aliases[0].Name, aliases[1].Name, aliases[2].Name})
		assert.Equal(t, "cost_first", aliases[0].Strategy)
		assert.Equal(t, "[998]", aliases[2].FallbackIDs)
		require.NotNil(t, aliases[2].DefaultChannelID)
		assert.Equal(t, fallback, *aliases[2].DefaultChannelID)
		aliases[0].Name = "caller-mutated"
		*aliases[2].DefaultChannelID = 1
		alias := ModelAlias{Name: "b"}
		require.NoError(t, models.Create(ctx, &alias))
		fresh, err := models.List(ctx)
		require.NoError(t, err)
		require.Len(t, fresh, 4)
		assert.Equal(t, "a", fresh[0].Name)
		assert.Equal(t, "b", fresh[1].Name)
		assert.EqualValues(t, 999, *fresh[3].DefaultChannelID)
		require.NoError(t, models.Delete(ctx, int64(alias.ID)))
		fresh, err = models.List(ctx)
		require.NoError(t, err)
		assert.Len(t, fresh, 3, "list has no stale Resolve cache")
	})

	t.Run("mapping pair exact fresh and missing", func(t *testing.T) {
		rows := []ChannelModel{{ChannelID: 11, Model: "alias", UpstreamModel: "up-11"},
			{ChannelID: 12, Model: "alias", UpstreamModel: "up-12"},
			{ChannelID: 11, Model: "other", UpstreamModel: "up-other"},
			{ChannelID: 11, Model: "' OR 1=1 --", UpstreamModel: "literal"}}
		require.NoError(t, db.Create(&rows).Error)
		for _, want := range rows {
			got, err := channels.GetModel(ctx, int64(want.ChannelID), want.Model)
			require.NoError(t, err)
			assert.Equal(t, want, *got)
			got.UpstreamModel = "caller-mutated"
		}
		require.NoError(t, db.Model(&ChannelModel{}).Where("channel_id = ? AND model = ?", 11, "alias").Update("upstream_model", "new-upstream").Error)
		got, err := channels.GetModel(ctx, 11, "alias")
		require.NoError(t, err)
		assert.Equal(t, "new-upstream", got.UpstreamModel)
		for _, pair := range []struct {
			id    int64
			model string
		}{{11, "missing"}, {999, "alias"}, {12, "other"}} {
			got, err := channels.GetModel(ctx, pair.id, pair.model)
			assert.Nil(t, got)
			assert.ErrorIs(t, err, ErrNotFound)
		}
	})

	for _, table := range []string{"model_aliases", "channel_models"} {
		t.Run("cancellation/"+table, func(t *testing.T) {
			query := func(ctx context.Context) error {
				if table == "model_aliases" {
					_, err := models.List(ctx)
					return err
				}
				_, err := channels.GetModel(ctx, 11, "alias")
				return err
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			assert.ErrorIs(t, query(canceled), context.Canceled)
			pool, err := db.DB()
			require.NoError(t, err)
			lock, err := pool.Conn(ctx)
			require.NoError(t, err)
			defer lock.Close()
			_, err = lock.ExecContext(ctx, "LOCK TABLES "+table+" WRITE")
			require.NoError(t, err)
			defer func() { _, err := lock.ExecContext(ctx, "UNLOCK TABLES"); require.NoError(t, err) }()
			budget, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			defer cancel()
			started := time.Now()
			err = query(budget)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.False(t, errors.Is(err, ErrNotFound))
			assert.GreaterOrEqual(t, time.Since(started), 100*time.Millisecond)
			assert.Less(t, time.Since(started), 3*time.Second)
		})
	}
	// Lock release must restore both queries, before injecting real SQL faults.
	_, err = models.List(ctx)
	require.NoError(t, err)
	_, err = channels.GetModel(ctx, 11, "alias")
	require.NoError(t, err)
	t.Run("SQL errors retain driver classification", func(t *testing.T) {
		for _, table := range []string{"model_aliases", "channel_models"} {
			require.NoError(t, db.Exec("DROP TABLE "+table).Error)
			var err error
			if table == "model_aliases" {
				_, err = models.List(ctx)
			} else {
				_, err = channels.GetModel(ctx, 11, "alias")
			}
			var driverErr *mysqldriver.MySQLError
			require.ErrorAs(t, err, &driverErr)
			assert.EqualValues(t, 1146, driverErr.Number)
			assert.False(t, errors.Is(err, ErrNotFound))
		}
	})
}
