package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ChannelRepo 是渠道配置的读写层（11.2 §3 冻结签名；跨模块契约见 00 §6.3）。
// 消费方：16.1/16.2（路由读渠道）、14.1（管理 CRUD）。
type ChannelRepo interface {
	// 14.1 management reads bypass the configuration cache.
	List(ctx context.Context) ([]Channel, error)
	ListModels(ctx context.Context, channelID int64) ([]ChannelModel, error)
	UpdateConfig(ctx context.Context, ch *Channel, models []ChannelModel) error
	// UpdateHealthView mirrors Redis state without changing management columns.
	UpdateHealthView(ctx context.Context, channelID int64, status string, lastCheckedAt *time.Time) error
	// GetModel reads one channel/model mapping without caching (13.3).
	// A missing pair wraps ErrNotFound; other SQL errors retain their cause.
	GetModel(ctx context.Context, channelID int64, model string) (*ChannelModel, error)
	// ListActiveByModel 返回 enabled 且服务该模型的渠道，priority 降序（走缓存）。
	ListActiveByModel(ctx context.Context, model string) ([]Channel, error)
	// Get 按 id 读取渠道（不缓存）；未命中返回 ErrNotFound。
	Get(ctx context.Context, id int64) (*Channel, error)
	// Create 在一个事务内写入 渠道 + 渠道模型映射 + 渠道 Key 密文；
	// 任意一步失败整体回滚（11.2 §4 决策②）。keys 为密文，store 不解密。
	Create(ctx context.Context, ch *Channel, models []ChannelModel, keys [][]byte) error
	// Update 更新渠道的可管理字段；未命中返回 ErrNotFound。
	Update(ctx context.Context, ch *Channel) error
	// Delete 事务内删除渠道及其模型映射与 Key（无外键，需显式级联）。
	Delete(ctx context.Context, id int64) error
}

// channelRepo 是 ChannelRepo 的 MySQL 实现（GORM）。
type channelRepo struct {
	db    *gorm.DB
	cache *ConfigCache
}

// NewChannelRepo 装配渠道访问层。cache 为共享的配置读缓存（11-L1 §1）；
// 读方法走缓存，写方法不回源缓存（失效由调用方经 Invalidator 触发）。
func NewChannelRepo(db *gorm.DB, cache *ConfigCache) ChannelRepo {
	return &channelRepo{db: db, cache: cache}
}

func (r *channelRepo) GetModel(ctx context.Context, channelID int64, model string) (*ChannelModel, error) {
	var mapping ChannelModel
	if err := r.db.WithContext(ctx).Where("channel_id = ? AND model = ?", channelID, model).
		First(&mapping).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("store: channel model mapping: %w", ErrNotFound)
		}
		return nil, fmt.Errorf("store: read channel model mapping: %w", err)
	}
	return &mapping, nil
}

// ListActiveByModel 走缓存（TTL 60s + singleflight）；未命中回源 listActiveByModel。
func (r *channelRepo) ListActiveByModel(ctx context.Context, model string) ([]Channel, error) {
	return getOrLoad(r.cache, ctx, channelCacheKey(model),
		func(ctx context.Context) ([]Channel, error) { return r.listActiveByModel(ctx, model) },
		cloneChannels)
}

// listActiveByModel 联表 channel_models 取 enabled 渠道，priority 降序
// （同优先级按 id 升序，保证选择结果确定）。enabled 为 TINYINT，布尔绑定。
func (r *channelRepo) listActiveByModel(ctx context.Context, model string) ([]Channel, error) {
	var chs []Channel
	err := r.db.WithContext(ctx).
		Table("channels AS c").
		Select("c.*").
		Joins("JOIN channel_models AS cm ON cm.channel_id = c.id").
		Where("cm.model = ? AND c.enabled = ?", model, true).
		Order("c.priority DESC, c.id ASC").
		Scan(&chs).Error
	if err != nil {
		return nil, fmt.Errorf("store: 按模型 %q 查询启用渠道: %w", model, err)
	}
	return chs, nil
}

// Get 按主键读取渠道。
func (r *channelRepo) Get(ctx context.Context, id int64) (*Channel, error) {
	var ch Channel
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&ch).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("store: 渠道 %d: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("store: 查询渠道 %d: %w", id, err)
	}
	return &ch, nil
}

// Create 在事务内写入渠道、模型映射与 Key 密文。channel_keys 与
// channel_models 无外键约束（11.1 DDL 冻结），三表同事务保证不产生
// "渠道在而映射/Key 缺失"的脏数据（部分写入对路由即脏数据）。
func (r *channelRepo) Create(ctx context.Context, ch *Channel, models []ChannelModel, keys [][]byte) error {
	if ch == nil {
		return fmt.Errorf("store: Create 渠道为 nil")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()
		if ch.CreatedAt.IsZero() {
			ch.CreatedAt = now
		}
		ch.UpdatedAt = now
		// GORM Create 会写入零值 ""，覆盖 DDL 默认 'unknown'（台账 #1 / 01 §3.1）
		if ch.HealthStatus == "" {
			ch.HealthStatus = "unknown"
		}
		if err := tx.Create(ch).Error; err != nil {
			return fmt.Errorf("store: 插入渠道 %q: %w", ch.Name, err)
		}
		for i := range models {
			models[i].ChannelID = ch.ID
			if err := tx.Create(&models[i]).Error; err != nil {
				return fmt.Errorf("store: 插入渠道 %d 模型映射: %w", ch.ID, err)
			}
		}
		for _, enc := range keys {
			k := ChannelKey{
				ChannelID:      ch.ID,
				ProviderKeyEnc: enc,
				Weight:         1, // DDL 默认 1，显式置值（01 §3.1）
				Status:         ChannelKeyStatusActive,
				CreatedAt:      now,
			}
			if err := tx.Create(&k).Error; err != nil {
				return fmt.Errorf("store: 插入渠道 %d Key: %w", ch.ID, err)
			}
		}
		return nil
	})
}

// Update 更新渠道的可管理字段。健康缓存列（health_status /
// error_rate_cache / latency_p99_cache）归 14.3 异步维护，本方法不触碰。
func (r *channelRepo) Update(ctx context.Context, ch *Channel) error {
	if ch == nil || ch.ID == 0 {
		return fmt.Errorf("store: Update 渠道需要有效 id")
	}
	res := r.db.WithContext(ctx).Model(&Channel{}).Where("id = ?", ch.ID).Updates(map[string]any{
		"name":       ch.Name,
		"provider":   ch.Provider,
		"base_url":   ch.BaseURL,
		"priority":   ch.Priority,
		"weight":     ch.Weight,
		"enabled":    ch.Enabled,
		"updated_at": time.Now().UTC(),
	})
	if res.Error != nil {
		return fmt.Errorf("store: 更新渠道 %d: %w", ch.ID, res.Error)
	}
	// MySQL 默认返回 changed rows（值未变时 RowsAffected=0），不能据此判不存在
	if res.RowsAffected == 0 {
		var cnt int64
		if err := r.db.WithContext(ctx).Model(&Channel{}).Where("id = ?", ch.ID).
			Count(&cnt).Error; err != nil {
			return fmt.Errorf("store: 校验渠道 %d 存在性: %w", ch.ID, err)
		}
		if cnt == 0 {
			return fmt.Errorf("store: 更新渠道 %d: %w", ch.ID, ErrNotFound)
		}
	}
	return nil
}

// Delete 事务内级联删除渠道的 Key 与模型映射后删除渠道本身。
// （model_aliases.default_channel_id / fallback_ids 的引用清理属管理操作，
// 不在此处隐式处理，60s TTL 兜底见 11-L1 §1。）
func (r *channelRepo) Delete(ctx context.Context, id int64) error {
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockChannel(tx, id); err != nil {
			return err
		}
		if err := tx.Where("channel_id = ?", id).Delete(&ChannelKey{}).Error; err != nil {
			return fmt.Errorf("store: 删除渠道 %d 的 Key: %w", id, err)
		}
		if err := tx.Where("channel_id = ?", id).Delete(&ChannelModel{}).Error; err != nil {
			return fmt.Errorf("store: 删除渠道 %d 的模型映射: %w", id, err)
		}
		res := tx.Where("id = ?", id).Delete(&Channel{})
		if res.Error != nil {
			return fmt.Errorf("store: 删除渠道 %d: %w", id, res.Error)
		}
		if res.RowsAffected == 0 {
			return fmt.Errorf("store: 删除渠道 %d: %w", id, ErrNotFound)
		}
		return nil
	})
}

// lockChannel is shared by configuration replacement, deletion and Owned Key
// writes. Always acquire the parent before touching children, including inserts.
func lockChannel(tx *gorm.DB, id int64) error {
	var ch Channel
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("id").Where("id = ?", id).First(&ch).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("store: lock channel: %w", ErrNotFound)
		}
		return fmt.Errorf("store: lock channel: %w", err)
	}
	return nil
}

func (r *channelRepo) List(ctx context.Context) ([]Channel, error) {
	channels := make([]Channel, 0)
	if err := r.db.WithContext(ctx).Order("id ASC").Find(&channels).Error; err != nil {
		return nil, fmt.Errorf("store: list channels: %w", err)
	}
	return channels, nil
}

func (r *channelRepo) ListModels(ctx context.Context, channelID int64) ([]ChannelModel, error) {
	if _, err := r.Get(ctx, channelID); err != nil {
		return nil, err
	}
	models := make([]ChannelModel, 0)
	if err := r.db.WithContext(ctx).Where("channel_id = ?", channelID).Order("model ASC").Find(&models).Error; err != nil {
		return nil, fmt.Errorf("store: list channel models: %w", err)
	}
	return models, nil
}

func (r *channelRepo) UpdateConfig(ctx context.Context, ch *Channel, models []ChannelModel) error {
	if ch == nil || ch.ID == 0 || ch.ID > uint64(1<<63-1) {
		return fmt.Errorf("store: UpdateConfig requires a valid channel ID")
	}
	return r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockChannel(tx, int64(ch.ID)); err != nil {
			return err
		}
		if err := tx.Model(&Channel{}).Where("id = ?", ch.ID).Updates(map[string]any{
			"name": ch.Name, "provider": ch.Provider, "base_url": ch.BaseURL,
			"priority": ch.Priority, "weight": ch.Weight, "enabled": ch.Enabled,
			"updated_at": time.Now().UTC(),
		}).Error; err != nil {
			return fmt.Errorf("store: update channel config: %w", err)
		}
		if err := tx.Where("channel_id = ?", ch.ID).Delete(&ChannelModel{}).Error; err != nil {
			return fmt.Errorf("store: replace channel models: %w", err)
		}
		for _, model := range models {
			row := ChannelModel{ChannelID: ch.ID, Model: model.Model, UpstreamModel: model.UpstreamModel}
			if err := tx.Create(&row).Error; err != nil {
				return fmt.Errorf("store: insert channel model: %w", err)
			}
		}
		return nil
	})
}

func (r *channelRepo) UpdateHealthView(ctx context.Context, channelID int64, status string, lastCheckedAt *time.Time) error {
	if channelID <= 0 || (status != "unknown" && status != "healthy" && status != "disabled" && status != "cooling") {
		return errors.New("store: invalid health view")
	}
	// UpdateColumns deliberately bypasses GORM's updated_at callback. No stale
	// Channel model is saved, so concurrent management edits cannot be reverted.
	result := r.db.WithContext(ctx).Model(&Channel{}).Where("id = ?", channelID).
		UpdateColumns(map[string]any{"health_status": status, "last_checked_at": lastCheckedAt})
	if result.Error != nil {
		return fmt.Errorf("store: update channel health view: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		if _, err := r.Get(ctx, channelID); err != nil {
			return err
		}
	}
	return nil
}
