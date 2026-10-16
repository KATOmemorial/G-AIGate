package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// PriceRepo 是模型价格（model_prices）的读取层（11.2 §3）。消费方：17.3（计价）。
type PriceRepo interface {
	// Effective 返回模型在 at 时刻生效的价格：enabled 且 effective_from<=at
	// 的最新一条（走缓存）。无生效价格返回 ErrNoPrice——降级策略由 17.3 决定。
	Effective(ctx context.Context, model string, at time.Time) (*ModelPrice, error)
}

// priceRepo 是 PriceRepo 的 MySQL 实现。
type priceRepo struct {
	db    *gorm.DB
	cache *ConfigCache
}

// NewPriceRepo 装配价格访问层。cache 为共享的配置读缓存（11-L1 §1）。
func NewPriceRepo(db *gorm.DB, cache *ConfigCache) PriceRepo {
	return &priceRepo{db: db, cache: cache}
}

// Effective 走缓存。缓存按 model 单键——快照为回源那一刻按传入 at 解析的结果；
// 配置读路径按"当前时刻"消费（17.3 计价），故 60s 内的快照语义成立（11-L1 §1
// 的文档化取舍）。历史 at 查询不属该缓存语义，需绕过缓存或先 InvalidateAll。
// 无生效价格时回源返回 ErrNoPrice，不写入缓存（无负缓存）。
func (r *priceRepo) Effective(ctx context.Context, model string, at time.Time) (*ModelPrice, error) {
	return getOrLoad(r.cache, ctx, priceCacheKey(model),
		func(ctx context.Context) (*ModelPrice, error) { return r.effectiveAt(ctx, model, at) },
		clonePrice)
}

// effectiveAt 以 idx_price_model(model, effective_from) 取生效价格的最新一条，
// 同 effective_from 时按 id 降序（后插入者胜）。
func (r *priceRepo) effectiveAt(ctx context.Context, model string, at time.Time) (*ModelPrice, error) {
	var p ModelPrice
	err := r.db.WithContext(ctx).
		Where("model = ? AND enabled = ? AND effective_from <= ?", model, true, at.UTC()).
		Order("effective_from DESC, id DESC").
		First(&p).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("store: 模型 %q 在 %s: %w", model, at.UTC(), ErrNoPrice)
		}
		return nil, fmt.Errorf("store: 查询模型 %q 生效价格: %w", model, err)
	}
	return &p, nil
}
