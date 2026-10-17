package store

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// BillFilter 是账单列表的过滤条件（11.3 §3：按 user/group/period 过滤）。
// 零值表示该维度不过滤。Period 为 'YYYY-MM'。
type BillFilter struct {
	UserID  int64
	GroupID int64
	Period  string
}

// BillRepo 是月账单（bills 表）的物化写读层（11.3 §3 冻结签名）。
// bills 是 usage_records 的定时物化聚合，对账永远以明细求和为准
// （17-L1 §3 决策③）——本层只做 upsert 与列表，不做计价/聚合语义。
type BillRepo interface {
	// UpsertMonth 按唯一键 (user_id,period) 插入或更新月账单，重复调用取最后值
	// （11.3 §5 用例⑥）。totalCost 单位微元 int64。
	UpsertMonth(ctx context.Context, userID, groupID int64, period string, totalCost, tokenTotal int64) error
	// List 按过滤条件返回账单，period 降序（同期按 user_id 升序）。
	List(ctx context.Context, filter BillFilter) ([]Bill, error)
}

// billRepo 是 BillRepo 的 MySQL 实现（GORM）。
type billRepo struct{ db *gorm.DB }

// NewBillRepo 装配月账单访问层。
func NewBillRepo(db *gorm.DB) BillRepo { return &billRepo{db: db} }

// UpsertMonth 见 BillRepo。依赖 uk_bills_user_period(user_id,period) 唯一键：
// 冲突时转为更新 group_id/total_cost/token_total（created_at 保留首次物化时间）。
func (r *billRepo) UpsertMonth(ctx context.Context, userID, groupID int64, period string, totalCost, tokenTotal int64) error {
	b := Bill{
		UserID:     uint64(userID),
		GroupID:    uint64(groupID),
		Period:     period,
		TotalCost:  totalCost,
		TokenTotal: tokenTotal,
		CreatedAt:  time.Now().UTC(),
	}
	err := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "user_id"}, {Name: "period"}},
		DoUpdates: clause.AssignmentColumns([]string{"group_id", "total_cost", "token_total"}),
	}).Create(&b).Error
	if err != nil {
		return fmt.Errorf("store: upsert 月账单 user=%d period=%s: %w", userID, period, err)
	}
	return nil
}

// List 见 BillRepo。
func (r *billRepo) List(ctx context.Context, filter BillFilter) ([]Bill, error) {
	q := r.db.WithContext(ctx).Model(&Bill{})
	if filter.UserID != 0 {
		q = q.Where("user_id = ?", filter.UserID)
	}
	if filter.GroupID != 0 {
		q = q.Where("group_id = ?", filter.GroupID)
	}
	if filter.Period != "" {
		q = q.Where("period = ?", filter.Period)
	}
	var bills []Bill
	if err := q.Order("period DESC, user_id ASC").Find(&bills).Error; err != nil {
		return nil, fmt.Errorf("store: 列账单: %w", err)
	}
	return bills, nil
}
