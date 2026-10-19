package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRedis 起一台 miniredis 并返回其句柄与 go-redis 客户端（11.4 §2：单测用
// miniredis，不需要真实 Redis）。Miniredis 随 t 自动关闭。
func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// freezeClock 把包内时钟冻结到 tc（11.4 §5 用例④需跨天/跨月控制），用例结束恢复。
func freezeClock(t *testing.T, tc time.Time) {
	t.Helper()
	prev := nowFn
	nowFn = func() time.Time { return tc }
	t.Cleanup(func() { nowFn = prev })
}

// §5 用例①：AddUsed 两次 → tokens/cost 累加正确；key 命名含正确 periodTag（UTC）。
func TestQuotaCounter_AddUsedAccumulatesAndKeyNaming(t *testing.T) {
	mr, client := newTestRedis(t)
	freezeClock(t, time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	c := NewQuotaCounter(client)

	daily := QuotaKey{TargetType: "api_key", TargetID: 7, Period: PeriodDaily}
	require.NoError(t, c.AddUsed(ctx, daily, 100, 500))
	require.NoError(t, c.AddUsed(ctx, daily, 50, 250))

	// key 命名逐字符对照 00 §7.2 / 11.4 §3 契约
	assert.Equal(t, "gaigate:quota:used:api_key:7:2026-09-26", daily.String())
	// 机制性证据：key 确实落在上述命名（非仅 String() 的字符串断言）
	require.True(t, mr.Exists(daily.String()), "计数 key 应真实存在")
	assert.Equal(t, "150", mr.HGet(daily.String(), quotaFieldTokens))
	assert.Equal(t, "750", mr.HGet(daily.String(), quotaFieldCost))

	tokens, cost, err := c.GetUsed(ctx, daily)
	require.NoError(t, err)
	assert.EqualValues(t, 150, tokens)
	assert.EqualValues(t, 750, cost)

	// monthly 的 periodTag 为 "YYYY-MM"
	monthly := QuotaKey{TargetType: "user", TargetID: 3, Period: PeriodMonthly}
	assert.Equal(t, "gaigate:quota:used:user:3:2026-09", monthly.String())
	require.NoError(t, c.AddUsed(ctx, monthly, 10, 20))
	mt, mc, err := c.GetUsed(ctx, monthly)
	require.NoError(t, err)
	assert.EqualValues(t, 10, mt)
	assert.EqualValues(t, 20, mc)
}

// §5 用例① 补充：GetUsed 对不存在的 key/渠道返回 0,0（非错误）。
func TestQuotaCounter_GetUsedMissingKeyIsZero(t *testing.T) {
	freezeClock(t, time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	_, client := newTestRedis(t)
	c := NewQuotaCounter(client)

	tokens, cost, err := c.GetUsed(context.Background(),
		QuotaKey{TargetType: "group", TargetID: 42, Period: PeriodDaily})
	require.NoError(t, err)
	assert.Zero(t, tokens)
	assert.Zero(t, cost)
}

// §5 用例②：并发 100 goroutine 各 AddUsed(+1) → 总和 == 100（须 -race 通过）。
func TestQuotaCounter_ConcurrentAddUsed(t *testing.T) {
	freezeClock(t, time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	_, client := newTestRedis(t)
	ctx := context.Background()
	c := NewQuotaCounter(client)
	k := QuotaKey{TargetType: "user", TargetID: 1, Period: PeriodDaily}

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		// Lua 脚本保证 HINCRBY 原子；goroutine 属测试并发路径（01 §3 裸 go 禁令仅限请求路径）
		go func() {
			defer wg.Done()
			require.NoError(t, c.AddUsed(ctx, k, 1, 1))
		}()
	}
	wg.Wait()

	tokens, cost, err := c.GetUsed(ctx, k)
	require.NoError(t, err)
	assert.EqualValues(t, n, tokens, "并发累加不得丢失更新")
	assert.EqualValues(t, n, cost)
}

// §5 用例③：首次 AddUsed 后 key 有 TTL；重复 AddUsed 不重置 TTL。
func TestQuotaCounter_TTLSetOnceNotReset(t *testing.T) {
	mr, client := newTestRedis(t)
	// 周期起始时刻：daily 的 TTLHint = 24h + 1h 缓冲 = 25h
	freezeClock(t, time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC))
	ctx := context.Background()
	c := NewQuotaCounter(client)
	k := QuotaKey{TargetType: "api_key", TargetID: 1, Period: PeriodDaily}

	require.Equal(t, 25*time.Hour, k.TTLHint())

	require.NoError(t, c.AddUsed(ctx, k, 1, 1))
	assert.Equal(t, 25*time.Hour, mr.TTL(k.String()), "首次写入须设 TTL")

	// 推进 1h 后 TTL 应随之减少（证明 TTL 是一次性设置、随时间流逝）
	mr.FastForward(time.Hour)
	assert.Equal(t, 24*time.Hour, mr.TTL(k.String()))

	// 重复写入不得重置 TTL（Lua: 仅当无 TTL 时才 EXPIRE）
	require.NoError(t, c.AddUsed(ctx, k, 1, 1))
	assert.Equal(t, 24*time.Hour, mr.TTL(k.String()), "重复写入不得重置 TTL")
}

// §5 用例③ 补充：TTLHint 到期的绝对时刻固定为"周期末 + 1h"，与首写时刻无关。
func TestQuotaCounter_TTLHintFixedExpiry(t *testing.T) {
	// 深夜首写：距周期末仅 2h，TTLHint 应为 2h + 1h = 3h（而非整周期）
	freezeClock(t, time.Date(2026, 9, 26, 22, 0, 0, 0, time.UTC))
	daily := QuotaKey{TargetType: "api_key", TargetID: 1, Period: PeriodDaily}
	assert.Equal(t, 3*time.Hour, daily.TTLHint())

	// 同刻月末视角：2026-09-26 22:00 → 次月 1 日 00:00 + 1h
	monthly := QuotaKey{TargetType: "user", TargetID: 1, Period: PeriodMonthly}
	assert.Equal(t, 4*24*time.Hour+2*time.Hour+time.Hour, monthly.TTLHint()) // 9/26 22:00 → 10/1 00:00 = 4d2h, +1h
}

// §5 用例③ 边界（裁定 2026-10-19 / 文档 a1b831a）：到期 = "周期末 + 1h" 的绝对时刻，
// 与首写时刻无关——深夜首写的 TTL 显著小于 25h（周期起点首写的最长情形）。
func TestQuotaCounter_TTLHintLateDayShorterThanFullPeriod(t *testing.T) {
	mr, client := newTestRedis(t)
	// 23:00 首写：距周期末仅 1h，TTLHint = 1h + 1h = 2h，远小于 25h
	freezeClock(t, time.Date(2026, 9, 26, 23, 0, 0, 0, time.UTC))
	ctx := context.Background()
	c := NewQuotaCounter(client)
	k := QuotaKey{TargetType: "api_key", TargetID: 1, Period: PeriodDaily}

	require.Equal(t, 2*time.Hour, k.TTLHint())
	require.Less(t, k.TTLHint(), 25*time.Hour, "深夜首写 TTL 应显著小于周期起点首写的 25h")

	// 机制性证据：真实写入后 miniredis 上的 TTL 确为 2h（而非整周期）
	require.NoError(t, c.AddUsed(ctx, k, 1, 1))
	assert.Equal(t, 2*time.Hour, mr.TTL(k.String()),
		"到期为固定绝对时刻（周期末+1h），与首写时刻无关")
}

// §5 用例④：跨天 periodTag 变化 → 新 key，旧 key 仍在（等 TTL）。
func TestQuotaCounter_PeriodRolloverNewKeyOldKeyRemains(t *testing.T) {
	mr, client := newTestRedis(t)
	ctx := context.Background()
	c := NewQuotaCounter(client)
	k := QuotaKey{TargetType: "api_key", TargetID: 1, Period: PeriodDaily}

	freezeClock(t, time.Date(2026, 9, 26, 23, 0, 0, 0, time.UTC))
	require.NoError(t, c.AddUsed(ctx, k, 5, 5))
	day1 := k.String()
	require.Equal(t, "gaigate:quota:used:api_key:1:2026-09-26", day1)
	require.True(t, mr.Exists(day1))

	// 跨天（UTC 日切）——nowFn 推进到次日
	nowFn = func() time.Time { return time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC) }
	require.NoError(t, c.AddUsed(ctx, k, 7, 7))
	day2 := k.String()
	require.Equal(t, "gaigate:quota:used:api_key:1:2026-09-27", day2)

	assert.NotEqual(t, day1, day2, "跨天应换 key")
	assert.True(t, mr.Exists(day2), "新 key 应存在")
	assert.True(t, mr.Exists(day1), "旧 key 仍在（等 TTL 自然过期，不做清理任务）")
	assert.Greater(t, mr.TTL(day1), time.Duration(0), "旧 key 仍有剩余 TTL")
}
