package store

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// redisFor 指向同一 miniredis 的独立客户端（模拟另一副本）。
func redisFor(t *testing.T, mr *miniredis.Miniredis) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// §5 用例⑤：HealthState Load 不存在的渠道 → 零值；Save/Load 往返一致。
func TestHealthState_LoadMissingIsZeroAndRoundTrip(t *testing.T) {
	mr, client := newTestRedis(t)
	ctx := context.Background()
	h := NewHealthState(client)

	// 不存在 → 零值快照（DisabledUntil=0 即未禁用），且非错误
	got, err := h.Load(ctx, 404)
	require.NoError(t, err)
	assert.Equal(t, HealthSnapshot{}, got, "不存在的渠道应为零值快照")

	// Save/Load 往返一致（含 bool 与两个时间戳）
	want := HealthSnapshot{
		ConsecutiveFailures: 5,
		DisabledUntil:       1790000000,
		LastProbeAt:         1789999999,
		LastProbeOk:         true,
	}
	require.NoError(t, h.Save(ctx, 1, want))
	got, err = h.Load(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	// 机制性证据：key 命名逐字符对照契约，且为持久 key（无 TTL）
	key := "gaigate:health:1"
	require.True(t, mr.Exists(key))
	assert.Zero(t, mr.TTL(key), "健康状态 key 无 TTL（持久）")

	// 覆盖零值字段亦须写入（整 HASH 覆盖写）
	require.NoError(t, h.Save(ctx, 1, HealthSnapshot{ConsecutiveFailures: 0, LastProbeOk: false}))
	got, err = h.Load(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, HealthSnapshot{}, got)
	assert.Equal(t, "0", mr.HGet(key, healthFieldLastProbeOk), "零值字段也须落 HASH")
}

// HealthState 跨副本共享同一份真相：两个客户端（模拟两副本）读写同一 key 一致。
func TestHealthState_SharedAcrossClients(t *testing.T) {
	mr, client := newTestRedis(t)
	ctx := context.Background()

	replicaA := NewHealthState(client)
	replicaB := NewHealthState(redisFor(t, mr))

	snap := HealthSnapshot{ConsecutiveFailures: 2, DisabledUntil: 12345, LastProbeAt: 999, LastProbeOk: false}
	require.NoError(t, replicaA.Save(ctx, 7, snap))

	got, err := replicaB.Load(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, snap, got, "副本 B 应读到副本 A 写入的健康状态")
}
