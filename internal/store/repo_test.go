package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/secret"
)

// seededTestDB 迁移 + seed 后的测试库（复用 11.1 测试基建）。
func seededTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := migratedTestDB(t)
	if err := Seed(db, SeedOptions{AdminPassword: "admin-test", MasterKey: testMasterKey()}); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	return db
}

// channelByID 按 name 查渠道（测试辅助，避免复用带主键的 struct，01 §3.1）。
func channelByName(t *testing.T, db *gorm.DB, name string) Channel {
	t.Helper()
	var c Channel
	require.NoError(t, db.Where("name = ?", name).First(&c).Error)
	return c
}

// §5 用例①：ListActiveByModel("gpt-4o-demo") 返回 2 条且 priority 降序
func TestChannelRepo_ListActiveByModel(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db)

	chs, err := repo.ListActiveByModel(context.Background(), "gpt-4o-demo")
	require.NoError(t, err)
	require.Len(t, chs, 2, "应返回两条启用渠道")
	// priority 降序：demo-primary(10) 在 demo-fallback(5) 之前
	assert.Equal(t, "demo-primary", chs[0].Name)
	assert.Equal(t, "demo-fallback", chs[1].Name)
	assert.Greater(t, chs[0].Priority, chs[1].Priority, "priority 必须降序")

	// 未知模型返回空
	none, err := repo.ListActiveByModel(context.Background(), "no-such-model")
	require.NoError(t, err)
	assert.Empty(t, none)
}

// 禁用渠道不从 ListActiveByModel 返回
func TestChannelRepo_ListActiveByModel_ExcludesDisabled(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db)
	require.NoError(t, db.Model(&Channel{}).Where("name = ?", "demo-fallback").
		Update("enabled", false).Error)

	chs, err := repo.ListActiveByModel(context.Background(), "gpt-4o-demo")
	require.NoError(t, err)
	require.Len(t, chs, 1)
	assert.Equal(t, "demo-primary", chs[0].Name)
}

// Get 命中 / 未命中
func TestChannelRepo_Get(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db)
	primary := channelByName(t, db, "demo-primary")

	got, err := repo.Get(context.Background(), int64(primary.ID))
	require.NoError(t, err)
	assert.Equal(t, primary.ID, got.ID)
	assert.Equal(t, "openai", got.Provider)

	_, err = repo.Get(context.Background(), 999999)
	assert.ErrorIs(t, err, ErrNotFound)
}

// ① 派生：Create 正路径——三表落库、health_status 显式置 unknown
func TestChannelRepo_Create(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewChannelRepo(db)
	ctx := context.Background()

	ch := &Channel{Name: "new-chan", Provider: "openai", BaseURL: "http://127.0.0.1:9999",
		Priority: 7, Weight: 2, Enabled: true}
	models := []ChannelModel{
		{Model: "m-a", UpstreamModel: "up-a"},
		{Model: "m-b", UpstreamModel: "up-b"},
	}
	keys := [][]byte{{1, 2, 3}, {4, 5, 6, 7}}
	require.NoError(t, repo.Create(ctx, ch, models, keys))
	require.NotZero(t, ch.ID)

	// 渠道行
	var got Channel
	require.NoError(t, db.Where("name = ?", "new-chan").First(&got).Error)
	assert.Equal(t, int32(7), got.Priority)
	// 台账 #1：GORM 零值不覆盖 DDL 默认，health_status 应为 unknown
	assert.Equal(t, "unknown", got.HealthStatus)
	assert.Equal(t, "unknown", ch.HealthStatus, "结构体应被显式赋值")

	// 映射行
	var cms []ChannelModel
	require.NoError(t, db.Where("channel_id = ?", ch.ID).Find(&cms).Error)
	assert.Len(t, cms, 2)
	// Key 行（密文原样）
	ks, err := NewKeyRepo(db).ListByChannel(ctx, int64(ch.ID))
	require.NoError(t, err)
	require.Len(t, ks, 2)
	assert.Equal(t, []byte{1, 2, 3}, ks[0].ProviderKeyEnc)
	assert.Equal(t, ChannelKeyStatusActive, ks[0].Status)
}

// §5 用例⑤：Create 事务——注入 channel_keys 插入失败 → 渠道行不存在（回滚）
func TestChannelRepo_CreateRollback(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewChannelRepo(db)
	ctx := context.Background()

	ch := &Channel{Name: "tx-rollback", Provider: "openai", BaseURL: "http://x",
		Priority: 1, Weight: 1, Enabled: true}
	models := []ChannelModel{{Model: "tx-m", UpstreamModel: "tx-m"}}
	// 600 字节 > VARBINARY(512)：channel_keys 插入必然失败，触发整体回滚
	oversize := make([]byte, 600)
	err := repo.Create(ctx, ch, models, [][]byte{oversize})
	require.Error(t, err, "Key 超长应导致 Create 失败")

	var chCnt, cmCnt, ckCnt int64
	require.NoError(t, db.Model(&Channel{}).Where("name = ?", "tx-rollback").Count(&chCnt).Error)
	require.NoError(t, db.Model(&ChannelModel{}).Where("model = ?", "tx-m").Count(&cmCnt).Error)
	require.NoError(t, db.Model(&ChannelKey{}).Where("provider_key_enc = ?", oversize).Count(&ckCnt).Error)
	assert.Zero(t, chCnt, "回滚后渠道行必须不存在")
	assert.Zero(t, cmCnt, "回滚后模型映射必须不存在")
	assert.Zero(t, ckCnt, "回滚后 Key 必须不存在")
}

// Update 正路径 + 未命中
func TestChannelRepo_Update(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db)
	ctx := context.Background()
	primary := channelByName(t, db, "demo-primary")

	primary.Priority = 99
	primary.Enabled = false
	require.NoError(t, repo.Update(ctx, &primary))

	got, err := repo.Get(ctx, int64(primary.ID))
	require.NoError(t, err)
	assert.Equal(t, int32(99), got.Priority)
	assert.False(t, got.Enabled)
	// 健康缓存列不被 Update 触碰
	assert.Equal(t, "unknown", got.HealthStatus)

	err = repo.Update(ctx, &Channel{ID: 999999, Name: "x"})
	assert.ErrorIs(t, err, ErrNotFound)
}

// Update 无实际变更也应成功（MySQL changed-rows 语义不得误判为不存在）
func TestChannelRepo_Update_NoOpIsOK(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db)
	primary := channelByName(t, db, "demo-primary")
	require.NoError(t, repo.Update(context.Background(), &primary))
}

// Delete 级联删除 Key 与映射；未命中返回 ErrNotFound
func TestChannelRepo_Delete(t *testing.T) {
	db := seededTestDB(t)
	repo := NewChannelRepo(db)
	ctx := context.Background()
	primary := channelByName(t, db, "demo-primary")

	require.NoError(t, repo.Delete(ctx, int64(primary.ID)))

	var chCnt, cmCnt, ckCnt int64
	require.NoError(t, db.Model(&Channel{}).Where("id = ?", primary.ID).Count(&chCnt).Error)
	require.NoError(t, db.Model(&ChannelModel{}).Where("channel_id = ?", primary.ID).Count(&cmCnt).Error)
	require.NoError(t, db.Model(&ChannelKey{}).Where("channel_id = ?", primary.ID).Count(&ckCnt).Error)
	assert.Zero(t, chCnt)
	assert.Zero(t, cmCnt, "渠道删除应级联删除模型映射")
	assert.Zero(t, ckCnt, "渠道删除应级联删除 Key")

	assert.ErrorIs(t, repo.Delete(ctx, int64(primary.ID)), ErrNotFound)
}

// §5 用例① 相关：ModelRepo.Resolve 命中 seed 别名
func TestModelRepo_Resolve(t *testing.T) {
	db := seededTestDB(t)
	repo := NewModelRepo(db)
	primary := channelByName(t, db, "demo-primary")

	a, err := repo.Resolve(context.Background(), "gpt-4o-demo")
	require.NoError(t, err)
	require.NotNil(t, a.DefaultChannelID)
	assert.Equal(t, primary.ID, *a.DefaultChannelID)
	assert.Equal(t, "weighted", a.Strategy)
	ids := ParseFallbackIDs(a.FallbackIDs)
	require.Len(t, ids, 1)
	fallback := channelByName(t, db, "demo-fallback")
	assert.Equal(t, fallback.ID, ids[0])

	_, err = repo.Resolve(context.Background(), "unknown")
	assert.ErrorIs(t, err, ErrNotFound)
}

// ModelRepo CRUD
func TestModelRepo_CRUD(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewModelRepo(db)
	ctx := context.Background()

	// Create 省略 strategy → 补齐 DDL 默认 weighted
	a := &ModelAlias{Name: "alias-x", DisplayName: "X"}
	require.NoError(t, repo.Create(ctx, a))
	require.NotZero(t, a.ID)

	got, err := repo.Resolve(ctx, "alias-x")
	require.NoError(t, err)
	assert.Equal(t, "weighted", got.Strategy, "空 strategy 应补齐默认 weighted")

	// 空 fallback_ids 必须落 NULL（JSON 列不接受 ''，否则 MySQL Error 3140）
	var rawFallback *string
	require.NoError(t, db.Raw("SELECT fallback_ids FROM model_aliases WHERE id = ?", a.ID).
		Scan(&rawFallback).Error)
	assert.Nil(t, rawFallback, "空 fallback_ids 应写 NULL")

	// Update
	got.FallbackIDs = "[3,4]"
	got.DefaultChannelID = nil
	require.NoError(t, repo.Update(ctx, got))
	got2, err := repo.Resolve(ctx, "alias-x")
	require.NoError(t, err)
	// MySQL JSON 列会规范化文本（"[3,4]"→"[3, 4]"），故按解析后的 id 断言
	assert.Equal(t, []uint64{3, 4}, ParseFallbackIDs(got2.FallbackIDs))
	assert.Nil(t, got2.DefaultChannelID)

	assert.NoError(t, repo.Update(ctx, &ModelAlias{ID: a.ID, Name: "alias-x", Strategy: "weighted"}))
	assert.ErrorIs(t, repo.Update(ctx, &ModelAlias{ID: 999999, Name: "z"}), ErrNotFound)

	// Delete
	require.NoError(t, repo.Delete(ctx, int64(a.ID)))
	_, err = repo.Resolve(ctx, "alias-x")
	assert.ErrorIs(t, err, ErrNotFound)
	assert.ErrorIs(t, repo.Delete(ctx, int64(a.ID)), ErrNotFound)
}

// ParseFallbackIDs：正常解析 + 坏数据/空值容错（不 panic，按无回退处理）
func TestParseFallbackIDs(t *testing.T) {
	assert.Equal(t, []uint64{2, 3}, ParseFallbackIDs("[2,3]"))
	assert.Nil(t, ParseFallbackIDs(""))
	assert.Nil(t, ParseFallbackIDs("null"))
	assert.Nil(t, ParseFallbackIDs("{not json"))
	assert.Nil(t, ParseFallbackIDs(`[2,"x"]`))
}

// §5 用例⑥：Effective 多条 effective_from 返回最新生效条；无价格 → ErrNoPrice
func TestPriceRepo_Effective(t *testing.T) {
	db := seededTestDB(t)
	repo := NewPriceRepo(db)
	ctx := context.Background()
	now := time.Now().UTC()

	// seed 价格：15M/60M，effective_from 纪元零点
	p, err := repo.Effective(ctx, "gpt-4o-demo", now)
	require.NoError(t, err)
	assert.Equal(t, int64(15_000_000), p.InputPricePerM)

	// 追加一条更晚生效、更贵的价格 → Effective(now) 应返回它。
	// DATETIME(3) 毫秒精度：时间戳截断到 ms 以免往返比较受四舍五入影响
	t1 := now.Add(-time.Hour).Truncate(time.Millisecond)
	t2 := now.Add(-time.Minute).Truncate(time.Millisecond)
	require.NoError(t, db.Create(&ModelPrice{Model: "tiered", InputPricePerM: 100,
		OutputPricePerM: 200, Currency: "CNY", EffectiveFrom: t1, Enabled: true}).Error)
	require.NoError(t, db.Create(&ModelPrice{Model: "tiered", InputPricePerM: 300,
		OutputPricePerM: 400, Currency: "CNY", EffectiveFrom: t2, Enabled: true}).Error)

	got, err := repo.Effective(ctx, "tiered", now)
	require.NoError(t, err)
	assert.Equal(t, int64(300), got.InputPricePerM, "应取最新生效条")
	assert.Equal(t, t2, got.EffectiveFrom.UTC())

	// 历史时刻：at 落在 t1 与 t2 之间 → 只有 t1 生效
	hist, err := repo.Effective(ctx, "tiered", t1.Add(30*time.Second))
	require.NoError(t, err)
	assert.Equal(t, int64(100), hist.InputPricePerM)

	// disabled 的价格不生效
	require.NoError(t, db.Create(&ModelPrice{Model: "disabled-model", InputPricePerM: 1,
		OutputPricePerM: 1, Currency: "CNY", EffectiveFrom: t1, Enabled: false}).Error)
	_, err = repo.Effective(ctx, "disabled-model", now)
	assert.ErrorIs(t, err, ErrNoPrice)

	// 无价格模型
	_, err = repo.Effective(ctx, "no-price", now)
	assert.ErrorIs(t, err, ErrNoPrice)
}

// KeyRepo：密文原样透传 + 增删
func TestKeyRepo_ListAndMutate(t *testing.T) {
	db := seededTestDB(t)
	repo := NewKeyRepo(db)
	ctx := context.Background()
	primary := channelByName(t, db, "demo-primary")

	ks, err := repo.ListByChannel(ctx, int64(primary.ID))
	require.NoError(t, err)
	require.Len(t, ks, 1)
	// 密文透传：能与 master key 解密回 seed 明文，证明未经二次加工
	plain, err := secret.Decrypt(ks[0].ProviderKeyEnc, testMasterKey())
	require.NoError(t, err)
	assert.Equal(t, "sk-mock-a", string(plain))

	// Add
	require.NoError(t, repo.Add(ctx, int64(primary.ID), []byte{9, 9, 9}))
	ks2, err := repo.ListByChannel(ctx, int64(primary.ID))
	require.NoError(t, err)
	require.Len(t, ks2, 2)
	assert.Equal(t, []byte{9, 9, 9}, ks2[1].ProviderKeyEnc)

	// Remove
	require.NoError(t, repo.Remove(ctx, int64(ks2[1].ID)))
	ks3, err := repo.ListByChannel(ctx, int64(primary.ID))
	require.NoError(t, err)
	assert.Len(t, ks3, 1)
	// 其他渠道不受影响
	fallback := channelByName(t, db, "demo-fallback")
	kf, err := repo.ListByChannel(ctx, int64(fallback.ID))
	require.NoError(t, err)
	assert.Len(t, kf, 1)

	assert.ErrorIs(t, repo.Remove(ctx, 999999), ErrNotFound)
}
