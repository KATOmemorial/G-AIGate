package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestChannelHealthViewOnlyUpdatesTwoDisplayColumns(t *testing.T) {
	db := channelAdminDB(t)
	r := NewChannelRepo(db, testCache())
	ch := adminRepoChannel(t, r, "health-view-owned")
	ctx := context.Background()
	stamp := time.Unix(100, 0).UTC()
	require.NoError(t, db.Model(&Channel{}).Where("id = ?", ch.ID).UpdateColumns(map[string]any{
		"updated_at": stamp, "error_rate_cache": .25, "latency_p99_cache": 123,
	}).Error)
	before, err := r.Get(ctx, int64(ch.ID))
	require.NoError(t, err)
	keysBefore, err := NewKeyRepo(db).ListByChannel(ctx, int64(ch.ID))
	require.NoError(t, err)
	checked := time.Unix(150, 0).UTC()
	for _, status := range []string{"healthy", "disabled", "cooling", "unknown"} {
		require.NoError(t, r.UpdateHealthView(ctx, int64(ch.ID), status, &checked))
		got, err := r.Get(ctx, int64(ch.ID))
		require.NoError(t, err)
		require.Equal(t, status, got.HealthStatus)
		require.Equal(t, checked, *got.LastCheckedAt)
		got.HealthStatus, got.LastCheckedAt = before.HealthStatus, before.LastCheckedAt
		require.Equal(t, *before, *got, "all other columns, including updated_at, must be byte/value unchanged")
		// Same values may report RowsAffected=0 and must still succeed.
		require.NoError(t, r.UpdateHealthView(ctx, int64(ch.ID), status, &checked))
	}
	require.NoError(t, r.UpdateHealthView(ctx, int64(ch.ID), "unknown", nil))
	got, err := r.Get(ctx, int64(ch.ID))
	require.NoError(t, err)
	require.Nil(t, got.LastCheckedAt)
	keysAfter, err := NewKeyRepo(db).ListByChannel(ctx, int64(ch.ID))
	require.NoError(t, err)
	require.Equal(t, keysBefore, keysAfter)
	require.ErrorIs(t, r.UpdateHealthView(ctx, 99999, "healthy", nil), ErrNotFound)
	for _, id := range []int64{0, -1} {
		require.Error(t, r.UpdateHealthView(ctx, id, "healthy", nil))
	}
	require.Error(t, r.UpdateHealthView(ctx, int64(ch.ID), "invalid", nil))
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, r.UpdateHealthView(canceled, int64(ch.ID), "healthy", nil), context.Canceled)
}
