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
	db *gorm.DB
}

// NewPriceRepo 装配价格访问层。
func NewPriceRepo(db *gorm.DB) PriceRepo {
	return &priceRepo{db: db}
}

// Effective 以 idx_price_model(model, effective_from) 取生效价格的最新一条，
// 同 effective_from 时按 id 降序（后插入者胜）。
func (r *priceRepo) Effective(ctx context.Context, model string, at time.Time) (*ModelPrice, error) {
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
