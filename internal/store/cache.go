package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

// defaultConfigCacheTTL 是配置读缓存的默认 TTL（11-L1 §1：60s）。
const defaultConfigCacheTTL = 60 * time.Second

// Invalidator 是配置读缓存的主动失效入口（11.2 §3 冻结）。14.1 的管理 API
// 在渠道/别名变更后调用本副本的失效方法，使下一次读穿透回源。
// 注意：repo 的写方法（Create/Update/Delete）不自动失效——生效责任在调用方
// （与 11.2 §3 契约一致）；漏调则最长 60s 后由 TTL 收敛（11-L1 §1 已知取舍）。
type Invalidator interface {
	// InvalidateChannels 清 model 的 ListActiveByModel 缓存项。
	InvalidateChannels(model string)
	// InvalidateAlias 清 name 的 Resolve 缓存项。
	InvalidateAlias(name string)
	// InvalidateAll 清空全部缓存项（价格变更等无专属入口时使用）。
	InvalidateAll()
}

// ConfigCache 是进程内 cache-aside 缓存（11-L1 §1 / 11.2 §3）：
// TTL 过期 + 管理变更主动失效 + singleflight 防击穿 + 值拷贝返回。
// 只服务"配置读路径"（ListActiveByModel / Resolve / Effective）；CRUD 不缓存。
// 多副本间不做失效广播——最长 60s 的旧值窗口是文档化取舍（11-L1 §1）。
type ConfigCache struct {
	ttl time.Duration
	now func() time.Time // 可注入时钟，便于 TTL 边界测试

	mu      sync.Mutex
	entries map[string]cacheEntry
	group   singleflight.Group
}

type cacheEntry struct {
	val any
	exp time.Time
}

// 缓存 key 命名空间：失效入口按前缀定位条目。
const (
	keyChannelModel = "chan:model:"
	keyAlias        = "alias:"
	keyPriceModel   = "price:model:"
)

func channelCacheKey(model string) string { return keyChannelModel + model }
func aliasCacheKey(name string) string    { return keyAlias + name }
func priceCacheKey(model string) string   { return keyPriceModel + model }

// NewConfigCache 构造配置缓存；ttl<=0 回退默认 60s。
func NewConfigCache(ttl time.Duration) *ConfigCache {
	if ttl <= 0 {
		ttl = defaultConfigCacheTTL
	}
	return &ConfigCache{ttl: ttl, now: time.Now, entries: map[string]cacheEntry{}}
}

// getOrLoad 返回 key 对应值的拷贝：
//   - 命中且未过期：直接返回缓存值的拷贝，不回源；
//   - 未命中/已过期：经 singleflight 合并同一 key 的并发回源（TTL 到期瞬间
//     的并发读只打一次 DB，11.2 §4 决策①），回源结果存入缓存，对每个调用方
//     返回独立拷贝。
//
// clone 由调用方提供（类型化拷贝），保证调用方修改返回值不污染缓存。
// 回源错误不写入缓存（无负缓存），下次读会重试。
func getOrLoad[T any](c *ConfigCache, ctx context.Context, key string,
	load func(context.Context) (T, error), clone func(T) T) (T, error) {

	var zero T
	if v, ok := c.get(key); ok {
		if t, ok := v.(T); ok {
			return clone(t), nil
		}
	}
	res, err, _ := c.group.Do(key, func() (any, error) {
		if v, ok := c.get(key); ok { // double-check：等待合并期间可能已被填入
			return v, nil
		}
		v, err := load(ctx)
		if err != nil {
			return nil, err
		}
		c.put(key, v)
		return v, nil
	})
	if err != nil {
		return zero, err
	}
	t, ok := res.(T)
	if !ok {
		return zero, fmt.Errorf("store: 缓存条目类型不符 key=%s", key)
	}
	return clone(t), nil
}

func (c *ConfigCache) get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if !c.now().Before(e.exp) { // 已过期
		delete(c.entries, key)
		return nil, false
	}
	return e.val, true
}

func (c *ConfigCache) put(key string, val any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cacheEntry{val: val, exp: c.now().Add(c.ttl)}
}

func (c *ConfigCache) delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}

// InvalidateChannels 见 Invalidator。
func (c *ConfigCache) InvalidateChannels(model string) { c.delete(channelCacheKey(model)) }

// InvalidateAlias 见 Invalidator。
func (c *ConfigCache) InvalidateAlias(name string) { c.delete(aliasCacheKey(name)) }

// InvalidateAll 见 Invalidator。
func (c *ConfigCache) InvalidateAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = map[string]cacheEntry{}
}

// —— 值拷贝：缓存内为规范值，每次读出返回深拷贝 ——

func cloneChannel(c Channel) Channel {
	if c.LastCheckedAt != nil {
		t := *c.LastCheckedAt
		c.LastCheckedAt = &t
	}
	return c
}

func cloneChannels(in []Channel) []Channel {
	if in == nil {
		return nil
	}
	out := make([]Channel, len(in))
	for i := range in {
		out[i] = cloneChannel(in[i])
	}
	return out
}

func cloneAlias(a *ModelAlias) *ModelAlias {
	if a == nil {
		return nil
	}
	cp := *a
	if a.DefaultChannelID != nil {
		id := *a.DefaultChannelID
		cp.DefaultChannelID = &id
	}
	return &cp
}

func clonePrice(p *ModelPrice) *ModelPrice {
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}
