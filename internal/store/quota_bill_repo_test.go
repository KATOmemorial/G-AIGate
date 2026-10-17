package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// §5 用例⑤：Quota Upsert 幂等（同 target+period 更新不重复）。
func TestQuotaRepo_UpsertIdempotent(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewQuotaRepo(db)
	ctx := context.Background()

	q := &Quota{TargetType: QuotaTargetUser, TargetID: 7, Period: QuotaPeriodMonthly,
		TokenLimit: 1000, CostLimit: 500, SoftRatio: 0.8}
	require.NoError(t, repo.Upsert(ctx, q))

	got, err := repo.Get(ctx, QuotaTargetUser, 7, QuotaPeriodMonthly)
	require.NoError(t, err)
	assert.Equal(t, int64(1000), got.TokenLimit)
	assert.Equal(t, int64(500), got.CostLimit)
	assert.InDelta(t, 0.8, got.SoftRatio, 0.001)
	firstID := got.ID

	// 同一唯一键再次 Upsert（改值）→ 不应新增行、id 不变（幂等，机制性证据）
	require.NoError(t, repo.Upsert(ctx, &Quota{TargetType: QuotaTargetUser, TargetID: 7,
		Period: QuotaPeriodMonthly, TokenLimit: 2000, CostLimit: 900, SoftRatio: 0.5}))

	var cnt int64
	require.NoError(t, db.Model(&Quota{}).
		Where("target_type = ? AND target_id = ? AND period = ?", QuotaTargetUser, 7, QuotaPeriodMonthly).
		Count(&cnt).Error)
	assert.Equal(t, int64(1), cnt, "同唯一键 Upsert 不得产生重复行")

	got2, err := repo.Get(ctx, QuotaTargetUser, 7, QuotaPeriodMonthly)
	require.NoError(t, err)
	assert.Equal(t, firstID, got2.ID, "幂等更新应命中同一行（id 不变）")
	assert.Equal(t, int64(2000), got2.TokenLimit)
	assert.Equal(t, int64(900), got2.CostLimit)
	assert.InDelta(t, 0.5, got2.SoftRatio, 0.001)
}

// Quota Get 未命中 → ErrNotFound；ListByTarget 按 period 升序返回。
func TestQuotaRepo_GetAndListByTarget(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewQuotaRepo(db)
	ctx := context.Background()

	_, err := repo.Get(ctx, QuotaTargetAPIKey, 999, QuotaPeriodDaily)
	assert.ErrorIs(t, err, ErrNotFound)

	require.NoError(t, repo.Upsert(ctx, &Quota{TargetType: QuotaTargetAPIKey, TargetID: 3,
		Period: QuotaPeriodMonthly, TokenLimit: 100, CostLimit: 10, SoftRatio: 0.8}))
	require.NoError(t, repo.Upsert(ctx, &Quota{TargetType: QuotaTargetAPIKey, TargetID: 3,
		Period: QuotaPeriodDaily, TokenLimit: 5, CostLimit: 1, SoftRatio: 0.9}))
	// 另一目标不应混入
	require.NoError(t, repo.Upsert(ctx, &Quota{TargetType: QuotaTargetUser, TargetID: 3,
		Period: QuotaPeriodDaily, TokenLimit: 77, CostLimit: 7, SoftRatio: 0.8}))

	qs, err := repo.ListByTarget(ctx, QuotaTargetAPIKey, 3)
	require.NoError(t, err)
	require.Len(t, qs, 2)
	assert.Equal(t, QuotaPeriodDaily, qs[0].Period, "period 应升序：daily 在前")
	assert.Equal(t, QuotaPeriodMonthly, qs[1].Period)
}

// §5 用例⑥：Bill UpsertMonth 重复调用取最后值（UNIQUE 键冲突转 update）。
func TestBillRepo_UpsertMonthLastWins(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewBillRepo(db)
	ctx := context.Background()

	require.NoError(t, repo.UpsertMonth(ctx, 1, 1, "2026-10", 100, 10))
	require.NoError(t, repo.UpsertMonth(ctx, 1, 1, "2026-10", 200, 20)) // 同唯一键 → 更新

	bills, err := repo.List(ctx, BillFilter{UserID: 1, Period: "2026-10"})
	require.NoError(t, err)
	require.Len(t, bills, 1, "同 user+period 只应一行（冲突转 update）")
	assert.Equal(t, int64(200), bills[0].TotalCost, "重复调用应取最后值")
	assert.Equal(t, int64(20), bills[0].TokenTotal)
	assert.Equal(t, uint64(1), bills[0].GroupID)
}

// Bill List 过滤与排序（user/group/period）。
func TestBillRepo_ListFilters(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewBillRepo(db)
	ctx := context.Background()

	require.NoError(t, repo.UpsertMonth(ctx, 1, 1, "2026-09", 10, 1))
	require.NoError(t, repo.UpsertMonth(ctx, 1, 1, "2026-10", 20, 2))
	require.NoError(t, repo.UpsertMonth(ctx, 2, 1, "2026-10", 30, 3))

	// 按 user 过滤 + period 降序
	u1, err := repo.List(ctx, BillFilter{UserID: 1})
	require.NoError(t, err)
	require.Len(t, u1, 2)
	assert.Equal(t, "2026-10", u1[0].Period, "period 应降序")
	assert.Equal(t, "2026-09", u1[1].Period)

	// 按 group 过滤
	g1, err := repo.List(ctx, BillFilter{GroupID: 1})
	require.NoError(t, err)
	assert.Len(t, g1, 3)

	// 按 period 过滤
	p, err := repo.List(ctx, BillFilter{Period: "2026-10"})
	require.NoError(t, err)
	assert.Len(t, p, 2)

	// 无匹配
	none, err := repo.List(ctx, BillFilter{UserID: 999})
	require.NoError(t, err)
	assert.Empty(t, none)
}
