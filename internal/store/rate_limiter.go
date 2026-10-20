package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// 限流计数 key 契约（00 §7.2 / 11.4 §3）：gaigate:rl:{scope}:{id}:{window}。
// window 取 time.Duration 的规范字符串（如 "1m0s"），不同窗口不共键。
const rateLimitKeyPrefix = "gaigate:rl:"

// RateLimiter 是固定窗口限流计数的原子原语（11.4 §3：预留接口 + 实现）。
// 只做计数与窗口判定，不承载业务判定语义（scope/id/limit 由消费方给定）。
// 启用策略（哪些 scope、阈值、开关）未定、当前无消费方——移交项见 02 台账 #19。
type RateLimiter interface {
	// Allow 计数一次并判定：当前窗口内计数 <= limit → true；超出 → false。
	// 窗口自首次计数起算，到期后计数自动清零（靠 key 过期，无需清理任务）。
	// window 必须至少 1ms 且为整毫秒；非法时先返回 false, 参数错误，不访问 Redis。
	Allow(ctx context.Context, scope string, id string, limit int, window time.Duration) (bool, error)
}

type rateLimiter struct{ client *redis.Client }

// NewRateLimiter 构造限流计数原语（11.4 §3）。
func NewRateLimiter(client *redis.Client) RateLimiter { return &rateLimiter{client: client} }

// rateLimitScript 原子完成 INCR + 仅首次设 PEXPIRE（防"加了数没 TTL"的永久 key，
// 与 AddUsed 同一纪律）。用毫秒精度 PEXPIRE：§3 描述为 INCR/EXPIRE，此处取
// EXPIRE 家族的 PEXPIRE 以支持亚秒窗口（整秒窗口语义等价，见 §9 实现记录）。
var rateLimitScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return n
`)

func rateLimitKey(scope, id string, window time.Duration) string {
	return fmt.Sprintf("%s%s:%s:%s", rateLimitKeyPrefix, scope, id, window.String())
}

// Allow 见 RateLimiter。
func (r *rateLimiter) Allow(ctx context.Context, scope, id string, limit int,
	window time.Duration) (bool, error) {
	if window < time.Millisecond || window%time.Millisecond != 0 {
		return false, fmt.Errorf("store: 限流 window=%s 无效：必须至少 1ms 且为整毫秒", window)
	}
	key := rateLimitKey(scope, id, window)
	n, err := rateLimitScript.Run(ctx, r.client, []string{key}, window.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("store: 限流计数 %s: %w", key, err)
	}
	return n <= int64(limit), nil
}
