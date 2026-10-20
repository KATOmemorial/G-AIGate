package store

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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

// §5⑥ / 主会话 §9.3 裁定：精确整毫秒窗口保留计数、首设 TTL 和到期恢复语义。
func TestRateLimiter_WholeMillisecondWindows(t *testing.T) {
	for _, tc := range []struct {
		window time.Duration
		tag    string
	}{
		{time.Millisecond, "1ms"},
		{2 * time.Millisecond, "2ms"},
		{500 * time.Millisecond, "500ms"},
		{time.Second, "1s"},
	} {
		t.Run(tc.tag, func(t *testing.T) {
			mr, client := newTestRedis(t)
			rl := NewRateLimiter(client)
			ctx := context.Background()
			key := "gaigate:rl:user:42:" + tc.tag
			require.True(t, mustAllow(t, rl, ctx, "user", "42", 1, tc.window))
			value, err := mr.Get(key)
			require.NoError(t, err)
			assert.Equal(t, "1", value)
			assert.Equal(t, tc.window, mr.TTL(key))
			mr.FastForward(tc.window / 2)
			require.False(t, mustAllow(t, rl, ctx, "user", "42", 1, tc.window))
			value, err = mr.Get(key)
			require.NoError(t, err)
			assert.Equal(t, "2", value)
			assert.Equal(t, tc.window-tc.window/2, mr.TTL(key), "重复计数不得刷新 TTL")
			mr.FastForward(tc.window - tc.window/2)
			assert.False(t, mr.Exists(key))
			require.True(t, mustAllow(t, rl, ctx, "user", "42", 1, tc.window))
			value, err = mr.Get(key)
			require.NoError(t, err)
			assert.Equal(t, "1", value)
			assert.Equal(t, tc.window, mr.TTL(key))
		})
	}
}

// 非法参数校验必须早于任何 Redis 访问；覆盖首连与既有连接，及 limit<=0 的优先级。
func TestRateLimiter_InvalidWindowDoesNotAccessRedis(t *testing.T) {
	for _, window := range []time.Duration{
		0, -time.Millisecond, -500 * time.Microsecond, time.Nanosecond,
		500 * time.Microsecond, 999 * time.Microsecond, 1500 * time.Microsecond,
	} {
		t.Run(window.String(), func(t *testing.T) {
			for _, limit := range []int{1, 0, -1} {
				t.Run("limit="+strconv.Itoa(limit), func(t *testing.T) {
					for _, warm := range []bool{false, true} {
						t.Run("warm="+strconv.FormatBool(warm), func(t *testing.T) {
							mr, client := newTestRedis(t)
							access := &rateLimitAccessCounter{}
							client.AddHook(access)
							key := "gaigate:rl:user:42:" + window.String()
							if warm {
								require.NoError(t, client.Ping(context.Background()).Err())
								mr.Set(key, "9")
								mr.SetTTL(key, 7*time.Second)
							}
							before := access.counts()
							rl := NewRateLimiter(client)
							for i := 0; i < 3; i++ {
								allowed, err := rl.Allow(context.Background(), "user", "42", limit, window)
								assert.False(t, allowed)
								assertInvalidRateWindowError(t, err, window)
							}
							assert.Equal(t, before, access.counts(), "非法窗口不得产生命令、pipeline 或连接尝试")
							if warm {
								value, err := mr.Get(key)
								require.NoError(t, err)
								assert.Equal(t, "9", value)
								assert.Equal(t, 7*time.Second, mr.TTL(key))
							} else {
								assert.False(t, mr.Exists(key))
							}
						})
					}
				})
			}
		})
	}
}

func TestRateLimiter_InvalidWindowWithUnavailableRedis(t *testing.T) {
	// TCP 端口 0 不提供 Redis 服务；参数错误应在进入拨号逻辑前返回。
	client := NewRedis(RedisOptions{Addr: "127.0.0.1:0"})
	t.Cleanup(func() { _ = client.Close() })
	access := &rateLimitAccessCounter{}
	client.AddHook(access)
	rl := NewRateLimiter(client)
	for _, window := range []time.Duration{500 * time.Microsecond, 1500 * time.Microsecond} {
		for _, limit := range []int{1, 0, -1} {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			allowed, err := rl.Allow(ctx, "user", "42", limit, window)
			cancel()
			assert.False(t, allowed)
			assertInvalidRateWindowError(t, err, window)
		}
	}
	assert.Equal(t, [3]int64{}, access.counts(), "不可达地址也不得产生 Redis 访问尝试")
}

func TestRateLimiter_ValidWindowNonPositiveLimitStillCounts(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			mr, client := newTestRedis(t)
			access := &rateLimitAccessCounter{}
			client.AddHook(access)
			allowed, err := NewRateLimiter(client).Allow(context.Background(), "user", "42", limit, time.Second)
			require.NoError(t, err)
			assert.False(t, allowed)
			value, err := mr.Get("gaigate:rl:user:42:1s")
			require.NoError(t, err)
			assert.Equal(t, "1", value, "合法窗口 limit<=0 沿用原有计数行为")
			assert.Equal(t, time.Second, mr.TTL("gaigate:rl:user:42:1s"))
			assert.Positive(t, access.commands.Load())
			assert.Positive(t, access.dials.Load())
		})
	}
}

func assertInvalidRateWindowError(t *testing.T, err error, window time.Duration) {
	t.Helper()
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "window")
		assert.Contains(t, err.Error(), window.String())
		assert.Contains(t, err.Error(), "1ms")
		assert.Contains(t, err.Error(), "整毫秒")
	}
}

// 同时观测连接、普通命令、pipeline；原子计数支持客户端在多个 goroutine 中调用 hook。
type rateLimitAccessCounter struct {
	dials, commands, pipelines atomic.Int64
}

func (h *rateLimitAccessCounter) counts() [3]int64 {
	return [3]int64{h.dials.Load(), h.commands.Load(), h.pipelines.Load()}
}

func (h *rateLimitAccessCounter) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		h.dials.Add(1)
		return next(ctx, network, addr)
	}
}

func (h *rateLimitAccessCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.commands.Add(1)
		return next(ctx, cmd)
	}
}

func (h *rateLimitAccessCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.pipelines.Add(1)
		return next(ctx, cmds)
	}
}
