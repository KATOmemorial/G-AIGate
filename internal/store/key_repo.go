package store

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// KeyRepo 是渠道 Key（channel_keys）的读写层（11.2 §3）。密文透传：
// store 不解密 provider_key_enc，加解密边界在 14.2（11-L1 §1 / 11.2 §4 决策③）。
// 消费方：14.1（管理）、14.2（EndpointPicker 取值解密）。
type KeyRepo interface {
	// ListByChannel 返回渠道的全部 Key（密文原样，按 id 升序）。
	// status 过滤与解密由调用方负责（本层只搬运字节）。
	ListByChannel(ctx context.Context, channelID int64) ([]ChannelKey, error)
	// Add 为渠道追加一把 Key（密文）；weight 默认 1、status 默认启用。
	Add(ctx context.Context, channelID int64, enc []byte) error
	// Remove 按 id 删除 Key；未命中返回 ErrNotFound。
	Remove(ctx context.Context, id int64) error
}

// keyRepo 是 KeyRepo 的 MySQL 实现。
type keyRepo struct {
	db *gorm.DB
}

// NewKeyRepo 装配渠道 Key 访问层。
func NewKeyRepo(db *gorm.DB) KeyRepo {
	return &keyRepo{db: db}
}

// ListByChannel 取渠道全部 Key（密文原样）。
func (r *keyRepo) ListByChannel(ctx context.Context, channelID int64) ([]ChannelKey, error) {
	var ks []ChannelKey
	if err := r.db.WithContext(ctx).Where("channel_id = ?", channelID).
		Order("id ASC").Find(&ks).Error; err != nil {
		return nil, fmt.Errorf("store: 查询渠道 %d 的 Key: %w", channelID, err)
	}
	return ks, nil
}

// Add 追加一把渠道 Key。weight/status 依赖 DDL 默认，显式置值（01 §3.1）。
func (r *keyRepo) Add(ctx context.Context, channelID int64, enc []byte) error {
	k := ChannelKey{
		ChannelID:      uint64(channelID),
		ProviderKeyEnc: enc,
		Weight:         1,
		Status:         ChannelKeyStatusActive,
		CreatedAt:      time.Now().UTC(),
	}
	if err := r.db.WithContext(ctx).Create(&k).Error; err != nil {
		return fmt.Errorf("store: 插入渠道 %d Key: %w", channelID, err)
	}
	return nil
}

// Remove 按 id 删除 Key。
func (r *keyRepo) Remove(ctx context.Context, id int64) error {
	res := r.db.WithContext(ctx).Where("id = ?", id).Delete(&ChannelKey{})
	if res.Error != nil {
		return fmt.Errorf("store: 删除 Key %d: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("store: 删除 Key %d: %w", id, ErrNotFound)
	}
	return nil
}
