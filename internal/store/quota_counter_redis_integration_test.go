//go:build redis_integration

package store

import (
	"context"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 显式执行：GAIGATE_TEST_REDIS_ADDR=<隔离 Redis> go test -tags=redis_integration
// ./internal/store -run '^TestQuotaCounter_RealRedis' -count=1 -v。
// 不可达即失败，不以 SKIP 代替真实 Redis 错误路径验证；仅创建/删除本用例的 key。
func quotaIntegrationRedis(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("GAIGATE_TEST_REDIS_ADDR")
	require.NotEmpty(t, addr, "必须显式指定隔离测试 Redis 的 GAIGATE_TEST_REDIS_ADDR")
	client := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, client.Ping(ctx).Err(), "真实 Redis 必须可用")
	return client
}

func TestQuotaCounter_RealRedisOverflowLeavesHashUnchanged(t *testing.T) {
	client := quotaIntegrationRedis(t)
	for _, tc := range []struct {
		name         string
		fields       map[string]string
		tokens, cost int64
	}{
		{"cost_overflow", map[string]string{quotaFieldTokens: "9007199254740993", quotaFieldCost: "9223372036854775807", "other": "keep"}, 1, 1},
		{"cost_overflow_missing_tokens", map[string]string{quotaFieldCost: "9223372036854775807", "other": "keep"}, 1, 1},
		{"cost_underflow", map[string]string{quotaFieldTokens: "-9007199254740993", quotaFieldCost: "-9223372036854775808", "other": "keep"}, -1, -1},
		{"tokens_overflow", map[string]string{quotaFieldTokens: "9223372036854775807", quotaFieldCost: "7"}, 1, 1},
		{"tokens_underflow", map[string]string{quotaFieldTokens: "-9223372036854775808", quotaFieldCost: "7"}, -1, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			checkQuotaCounterFailedAdd(t, client, tc.fields, tc.tokens, tc.cost, "overflow")
		})
	}
}

func TestQuotaCounter_RealRedisExpiryCommandErrorLeavesHashUnchanged(t *testing.T) {
	checkQuotaCounterExpiryError(t, quotaIntegrationRedis(t))
}

// 原报告的两次调用序列：成功写入 MaxInt64 后，失败的第二次调用不能增加 tokens。
func TestQuotaCounter_RealRedisOverflowAfterSuccessfulAdd(t *testing.T) {
	client := quotaIntegrationRedis(t)
	freezeClock(t, time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	k := QuotaKey{TargetType: "user", TargetID: time.Now().UnixNano(), Period: PeriodDaily}
	key := k.String()
	t.Cleanup(func() { _ = client.Del(context.Background(), key).Err() })
	c := NewQuotaCounter(client)
	require.NoError(t, c.AddUsed(ctx, k, 1, math.MaxInt64))
	beforeTTL, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	require.Positive(t, beforeTTL)
	require.ErrorContains(t, c.AddUsed(ctx, k, 1, 1), "overflow")
	tokens, cost, err := c.GetUsed(ctx, k)
	require.NoError(t, err)
	assert.EqualValues(t, 1, tokens)
	assert.EqualValues(t, math.MaxInt64, cost)
	afterTTL, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.InDelta(t, beforeTTL, afterTTL, float64(5*time.Second))
	assert.LessOrEqual(t, afterTTL, beforeTTL)
}

// 同一 key 只剩一次可累加空间：100 个竞争调用中仅一次成功，其余失败不得改变 tokens。
func TestQuotaCounter_RealRedisConcurrentOverflow(t *testing.T) {
	client := quotaIntegrationRedis(t)
	freezeClock(t, time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()
	k := QuotaKey{TargetType: "user", TargetID: time.Now().UnixNano(), Period: PeriodDaily}
	key := k.String()
	t.Cleanup(func() { _ = client.Del(context.Background(), key).Err() })
	require.NoError(t, client.HSet(ctx, key, quotaFieldTokens, 0, quotaFieldCost, int64(math.MaxInt64-1)).Err())
	c := NewQuotaCounter(client)
	const n = 100
	results := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- c.AddUsed(ctx, k, 1, 1)
		}()
	}
	wg.Wait()
	close(results)
	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
		} else {
			require.ErrorContains(t, err, "overflow")
		}
	}
	assert.Equal(t, 1, succeeded)
	tokens, cost, err := c.GetUsed(ctx, k)
	require.NoError(t, err)
	assert.EqualValues(t, 1, tokens)
	assert.EqualValues(t, math.MaxInt64, cost)
	ttl, err := client.PTTL(ctx, key).Result()
	require.NoError(t, err)
	assert.Positive(t, ttl, "唯一成功的调用仍须设置 TTL")
}
