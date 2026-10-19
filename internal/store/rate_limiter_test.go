package store

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// §5 用例⑥：窗口内第 limit+1 次 → false；window 过后 → true。
func TestRateLimiter_AllowsUpToLimitThenResetsAfterWindow(t *testing.T) {
	mr, client := newTestRedis(t)
	ctx := context.Background()
	rl := NewRateLimiter(client)

	const limit = 3
	window := time.Minute

	for i := 1; i <= limit; i++ {
		ok, err := rl.Allow(ctx, "api_key", "42", limit, window)
		require.NoError(t, err)
		assert.Truef(t, ok, "窗口内第 %d 次应放行", i)
	}
	ok, err := rl.Allow(ctx, "api_key", "42", limit, window)
	require.NoError(t, err)
	assert.False(t, ok, "窗口内第 limit+1 次应拒绝")

	// 机制性证据：key 命名逐字符对照契约，且首计数即设了 TTL（防永久 key）
	key := "gaigate:rl:api_key:42:" + window.String()
	require.Equal(t, "gaigate:rl:api_key:42:1m0s", key)
	require.True(t, mr.Exists(key))
	assert.Equal(t, window, mr.TTL(key), "首次计数应设窗口 TTL")

	// 窗口过后（时间推进，key 过期）→ 计数清零，重新放行
	mr.FastForward(window)
	ok, err = rl.Allow(ctx, "api_key", "42", limit, window)
	require.NoError(t, err)
	assert.True(t, ok, "window 过后应放行")
}

// 限流 key 按 {scope}:{id}:{window} 分片：不同 scope/id/window 互不影响。
func TestRateLimiter_KeyIsolation(t *testing.T) {
	_, client := newTestRedis(t)
	ctx := context.Background()
	rl := NewRateLimiter(client)

	// 打满 scope=user/id=1/1m
	require.True(t, mustAllow(t, rl, ctx, "user", "1", 1, time.Minute))
	require.False(t, mustAllow(t, rl, ctx, "user", "1", 1, time.Minute))

	// 同 scope 不同 id：不受影响
	assert.True(t, mustAllow(t, rl, ctx, "user", "2", 1, time.Minute))
	// 同 scope/id 不同 window：另起一计数（不共键）
	assert.True(t, mustAllow(t, rl, ctx, "user", "1", 1, time.Hour))
}

func mustAllow(t *testing.T, rl RateLimiter, ctx context.Context, scope, id string, limit int, window time.Duration) bool {
	t.Helper()
	ok, err := rl.Allow(ctx, scope, id, limit, window)
	require.NoError(t, err)
	return ok
}
