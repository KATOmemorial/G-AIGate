package store

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// 配额计数 key 契约（00 §7.2 / 11.4 §3，逐字符固定，一旦使用不得变更）：
//
//	gaigate:quota:used:{targetType}:{targetID}:{periodTag}
//
// HASH 字段：tokens（已用 token 数）、cost_micro（已用成本，微元 int64，00 §7.4）。
const (
	quotaKeyPrefix = "gaigate:quota:used:"

	quotaFieldTokens = "tokens"
	quotaFieldCost   = "cost_micro"
)

// period 取值（17-L1 §2：target_type ∈ {api_key,user,group}、period ∈ {daily,monthly}）。
const (
	PeriodDaily   = "daily"
	PeriodMonthly = "monthly"
)

// quotaTTLBuffer 是周期末尾之后的缓冲（11.4 §3：+1h），保证周期内的读不被过早过期截断。
const quotaTTLBuffer = time.Hour

// QuotaKey 是配额计数 key 的三元组（11.4 §3 冻结）。TargetID 为 key/user/group 的 id。
type QuotaKey struct {
	TargetType string
	TargetID   int64
	Period     string // daily|monthly
}

// String 返回完整 Redis key（含按当天推导的 periodTag）：
//
//	gaigate:quota:used:{targetType}:{targetID}:{periodTag}
//
// periodTag 按 UTC 推导（11.4 §7 决策，避免时区漂移）：daily → "2006-01-02"，
// monthly → "2006-01"。周期切换即换 key，旧 key 靠 TTL 自然过期（11.4 §4 决策①）。
func (k QuotaKey) String() string {
	return k.stringAt(nowUTC())
}

// stringAt 与 ttlHintAt 接收同一 UTC 快照，避免单次操作跨周期时 key 和到期时间错配。
func (k QuotaKey) stringAt(now time.Time) string {
	format := "2006-01-02" // daily（默认；消费方 17.2 只传 daily|monthly）
	if k.Period == PeriodMonthly {
		format = "2006-01"
	}
	return fmt.Sprintf("%s%s:%d:%s", quotaKeyPrefix, k.TargetType, k.TargetID, now.Format(format))
}

// TTLHint 返回计数 key 首次写入时应设的 TTL：周期末尾 + 1h 缓冲（11.4 §3）。
//   - daily：次日 00:00 UTC + 1h（周期起始时为 25h）；
//   - monthly：次月 1 日 00:00 UTC + 1h。
//
// 语义为"到期落在固定绝对时刻（周期末 + 1h）"，与首次写入时刻无关——同一周期内
// 任意时刻首写，过期时刻一致。重复写入不重置（见 AddUsed 的 Lua 契约）。
func (k QuotaKey) TTLHint() time.Duration {
	return k.ttlHintAt(nowUTC())
}

func (k QuotaKey) ttlHintAt(now time.Time) time.Duration {
	var periodEnd time.Time
	if k.Period == PeriodMonthly {
		periodEnd = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC).AddDate(0, 1, 0)
	} else {
		periodEnd = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	}
	return periodEnd.Sub(now) + quotaTTLBuffer
}

// QuotaCounter 是配额已用计数的原子原语（11.4 §3 冻结）。只累加/读取，不做
// "是否超限"判定（判定归 17.2 QuotaGuard，11-L1 §1 / 11.4 §1）。
type QuotaCounter interface {
	// AddUsed 原子累加已用 tokens 与成本（微元）；首次写时设置周期 TTL
	// （HINCRBY 后 EXPIRE，仅当无 TTL 时——一个 Lua 内完成，防"加了数没 TTL"
	// 的永久 key，11.4 §3 原子性契约）。
	AddUsed(ctx context.Context, k QuotaKey, tokens, costMicro int64) error
	// GetUsed 读取已用 tokens 与成本；key/字段不存在时返回 0,0（非错误）。
	GetUsed(ctx context.Context, k QuotaKey) (tokens, costMicro int64, err error)
}

type quotaCounter struct{ client *redis.Client }

// NewQuotaCounter 构造配额计数原语（11.4 §3）。
func NewQuotaCounter(client *redis.Client) QuotaCounter { return &quotaCounter{client: client} }

// quotaAddScript 原子完成"HINCRBY tokens + HINCRBY cost + 仅首次设 EXPIRE"。
// 写入前 TTL 返回 -1（永久 key）或 -2（不存在）时才设 TTL——
// 故重复调用不重置 TTL（§5 用例③）。字段名由 Go 侧常量拼入，保证单一来源。
// Lua 命令错误不会自动回滚：后续命令失败时显式恢复已写字段，原先缺失的字段删除。
// 快照与恢复均用 Redis 原始字符串，不将 int64 计数转换为 Lua number（避免精度丢失）。
// 使用 Lua 内置 pcall 捕获 redis.call：锁定的 miniredis 对 redis.pcall 的错误返回不兼容。
var quotaAddScript = redis.NewScript(fmt.Sprintf(`
local tokensField = '%s'
local costField = '%s'
local before = redis.call('HMGET', KEYS[1], tokensField, costField)
local ttl = redis.call('TTL', KEYS[1])
local function restore(field, value)
  if value then
    redis.call('HSET', KEYS[1], field, value)
  else
    redis.call('HDEL', KEYS[1], field)
  end
end
local function errorReply(err)
  if type(err) == 'table' then
    return err
  end
  return redis.error_reply(tostring(err))
end
redis.call('HINCRBY', KEYS[1], tokensField, ARGV[1])
local costOK, costResult = pcall(redis.call, 'HINCRBY', KEYS[1], costField, ARGV[2])
if not costOK then
  restore(tokensField, before[1])
  return errorReply(costResult)
end
if ttl < 0 then
  local expiryOK, expiryResult = pcall(redis.call, 'EXPIRE', KEYS[1], ARGV[3])
  if not expiryOK then
    restore(tokensField, before[1])
    restore(costField, before[2])
    return errorReply(expiryResult)
  end
end
return 1
`, quotaFieldTokens, quotaFieldCost))

// AddUsed 见 QuotaCounter。
func (c *quotaCounter) AddUsed(ctx context.Context, k QuotaKey, tokens, costMicro int64) error {
	now := nowUTC()
	key := k.stringAt(now)
	ttlSec := int64(k.ttlHintAt(now) / time.Second)
	if err := quotaAddScript.Run(ctx, c.client, []string{key},
		tokens, costMicro, ttlSec).Err(); err != nil {
		return fmt.Errorf("store: 配额计数累加 %s: %w", key, err)
	}
	return nil
}

// GetUsed 见 QuotaCounter。HMGet 对不存在的 key/字段返回 nil，按 0 处理。
func (c *quotaCounter) GetUsed(ctx context.Context, k QuotaKey) (int64, int64, error) {
	key := k.String()
	vals, err := c.client.HMGet(ctx, key, quotaFieldTokens, quotaFieldCost).Result()
	if err != nil {
		return 0, 0, fmt.Errorf("store: 读取配额已用 %s: %w", key, err)
	}
	tokens, err := quotaHashInt(vals, 0, key, quotaFieldTokens)
	if err != nil {
		return 0, 0, err
	}
	costMicro, err := quotaHashInt(vals, 1, key, quotaFieldCost)
	if err != nil {
		return 0, 0, err
	}
	return tokens, costMicro, nil
}

// quotaHashInt 解析 HMGet 结果中的第 i 项：nil/空串（字段不存在）→ 0；否则按十进制解析。
func quotaHashInt(vals []any, i int, key, field string) (int64, error) {
	if i >= len(vals) || vals[i] == nil {
		return 0, nil
	}
	s, ok := vals[i].(string)
	if !ok || s == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("store: 配额字段非整数 %s[%s]=%q: %w", key, field, s, err)
	}
	return n, nil
}
