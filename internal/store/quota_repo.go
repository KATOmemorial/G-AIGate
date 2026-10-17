package store

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// quotas.target_type / period 取值域（11.1 §3 DDL 注释冻结）。
const (
	QuotaTargetAPIKey = "api_key"
	QuotaTargetUser   = "user"
	QuotaTargetGroup  = "group"

	QuotaPeriodDaily   = "daily"
	QuotaPeriodMonthly = "monthly"
)

// QuotaRepo 是配额配置（quotas 表）的读写层（11.3 §3 冻结签名）。
// 只存配置，不含预检/结算语义（17.2）；Redis 已用计数器是 11.4 的
// QuotaCounter，与本层无关（11.3 §2 非目标）。
type QuotaRepo interface {
	// Get 按 (target_type,target_id,period) 唯一键读取；未命中返回 ErrNotFound。
	Get(ctx context.Context, targetType string, targetID int64, period string) (*Quota, error)
	// Upsert 按唯一键插入或更新（幂等：同 target+period 重复调用不产生重复行）。
	Upsert(ctx context.Context, q *Quota) error
	// ListByTarget 返回某目标下全部 period 的配额，period 升序。
	ListByTarget(ctx context.Context, targetType string, targetID int64) ([]Quota, error)
}

// quotaRepo 是 QuotaRepo 的 MySQL 实现（GORM）。
type quotaRepo struct{ db *gorm.DB }

// NewQuotaRepo 装配配额配置访问层。
func NewQuotaRepo(db *gorm.DB) QuotaRepo { return &quotaRepo{db: db} }

// Get 见 QuotaRepo。
func (r *quotaRepo) Get(ctx context.Context, targetType string, targetID int64, period string) (*Quota, error) {
	var q Quota
	err := r.db.WithContext(ctx).
		Where("target_type = ? AND target_id = ? AND period = ?", targetType, targetID, period).
		First(&q).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("store: 配额 %s/%d/%s: %w", targetType, targetID, period, ErrNotFound)
		}
		return nil, fmt.Errorf("store: 查询配额 %s/%d/%s: %w", targetType, targetID, period, err)
	}
	return &q, nil
}

// Upsert 见 QuotaRepo。依赖 uk_quota_target(target_type,target_id,period)
// 唯一键：冲突时转为更新 token_limit/cost_limit/soft_ratio，故幂等
// （11.3 §5 用例⑤）。
func (r *quotaRepo) Upsert(ctx context.Context, q *Quota) error {
	if q == nil {
		return fmt.Errorf("store: Upsert 配额为 nil")
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "target_type"}, {Name: "target_id"}, {Name: "period"}},
		DoUpdates: clause.AssignmentColumns([]string{"token_limit", "cost_limit", "soft_ratio"}),
	}).Create(q).Error
	if err != nil {
		return fmt.Errorf("store: upsert 配额 %s/%d/%s: %w", q.TargetType, q.TargetID, q.Period, err)
	}
	return nil
}

// ListByTarget 见 QuotaRepo。
func (r *quotaRepo) ListByTarget(ctx context.Context, targetType string, targetID int64) ([]Quota, error) {
	var qs []Quota
	err := r.db.WithContext(ctx).
		Where("target_type = ? AND target_id = ?", targetType, targetID).
		Order("period ASC").
		Find(&qs).Error
	if err != nil {
		return nil, fmt.Errorf("store: 列配额 %s/%d: %w", targetType, targetID, err)
	}
	return qs, nil
}
