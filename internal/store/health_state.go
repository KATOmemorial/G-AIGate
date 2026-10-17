package store

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

// 健康状态 key 契约（00 §7.2 / 11.4 §3，逐字符固定）：gaigate:health:{channelID}。
// 整 HASH 覆盖写、无 TTL（持久）——"禁用到什么时候"由 DisabledUntil 时间戳表达，
// 不靠 key 过期（11.4 §4 决策②）。字段名为本实现内部约定（消费方经 Go 接口读写，
// 不直连 Redis）。
const (
	healthKeyPrefix = "gaigate:health:"

	healthFieldConsecutiveFailures = "consecutive_failures"
	healthFieldDisabledUntil       = "disabled_until"
	healthFieldLastProbeAt         = "last_probe_at"
	healthFieldLastProbeOk         = "last_probe_ok"
)

// HealthSnapshot 是渠道健康状态快照（11.4 §3 冻结字段结构）。改字段名会影响
// 消费方 14.3，变更须走 00 §10。
type HealthSnapshot struct {
	ConsecutiveFailures int32
	DisabledUntil       int64 // unix 秒；0 = 未禁用
	LastProbeAt         int64
	LastProbeOk         bool
}

// HealthState 是健康状态的原子存取原语（11.4 §3 冻结）。只存取，不做
// "是否禁用/是否恢复"判定（状态机归 14.3，11-L1 §1 / 11.4 §1）。
type HealthState interface {
	// Load 读取渠道健康快照；渠道不存在 → 零值快照（DisabledUntil=0 即未禁用）。
	Load(ctx context.Context, channelID int64) (HealthSnapshot, error)
	// Save 整 HASH 覆盖写（单命令 HSET 多字段，原子），无 TTL。
	Save(ctx context.Context, channelID int64, s HealthSnapshot) error
}

type healthState struct{ client *redis.Client }

// NewHealthState 构造健康状态原语（11.4 §3）。
func NewHealthState(client *redis.Client) HealthState { return &healthState{client: client} }

func healthKey(channelID int64) string {
	return fmt.Sprintf("%s%d", healthKeyPrefix, channelID)
}

// Load 见 HealthState。HGetAll 对不存在的 key 返回空 map → 零值快照。
func (h *healthState) Load(ctx context.Context, channelID int64) (HealthSnapshot, error) {
	key := healthKey(channelID)
	m, err := h.client.HGetAll(ctx, key).Result()
	if err != nil {
		return HealthSnapshot{}, fmt.Errorf("store: 读取渠道健康 %s: %w", key, err)
	}
	var s HealthSnapshot
	if v, ok := m[healthFieldConsecutiveFailures]; ok {
		n, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return HealthSnapshot{}, fmt.Errorf("store: 健康字段非整数 %s[%s]=%q: %w",
				key, healthFieldConsecutiveFailures, v, err)
		}
		s.ConsecutiveFailures = int32(n)
	}
	if v, ok := m[healthFieldDisabledUntil]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return HealthSnapshot{}, fmt.Errorf("store: 健康字段非整数 %s[%s]=%q: %w",
				key, healthFieldDisabledUntil, v, err)
		}
		s.DisabledUntil = n
	}
	if v, ok := m[healthFieldLastProbeAt]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return HealthSnapshot{}, fmt.Errorf("store: 健康字段非整数 %s[%s]=%q: %w",
				key, healthFieldLastProbeAt, v, err)
		}
		s.LastProbeAt = n
	}
	if v, ok := m[healthFieldLastProbeOk]; ok {
		s.LastProbeOk = v == "1"
	}
	return s, nil
}

// Save 见 HealthState。单命令 HSET 多字段（原子），全字段写入即整 HASH 覆盖；
// 不设 TTL（持久）。
func (h *healthState) Save(ctx context.Context, channelID int64, s HealthSnapshot) error {
	key := healthKey(channelID)
	if err := h.client.HSet(ctx, key,
		healthFieldConsecutiveFailures, strconv.FormatInt(int64(s.ConsecutiveFailures), 10),
		healthFieldDisabledUntil, strconv.FormatInt(s.DisabledUntil, 10),
		healthFieldLastProbeAt, strconv.FormatInt(s.LastProbeAt, 10),
		healthFieldLastProbeOk, boolFlag(s.LastProbeOk),
	).Err(); err != nil {
		return fmt.Errorf("store: 写入渠道健康 %s: %w", key, err)
	}
	return nil
}

// boolFlag 把 bool 编码为 Redis 字段值 "1"/"0"（与 Load 的解析对称）。
func boolFlag(b bool) string {
	if b {
		return "1"
	}
	return "0"
}
