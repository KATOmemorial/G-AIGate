package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"gaigate/internal/secret"
)

// defaultAdminPassword 是 GAIGATE_ADMIN_PASSWORD 未设置时的 admin 密码
// （11.1 §3：admin123，仅 dev）。
const defaultAdminPassword = "admin123"

// SeedOptions 是幂等 seed 的输入。
type SeedOptions struct {
	// AdminPassword 来自环境变量 GAIGATE_ADMIN_PASSWORD（11.1 §3）；
	// 空串回退 defaultAdminPassword。已存在的 admin 用户密码与环境
	// 不一致时更新（环境是权威），一致时不产生写。
	AdminPassword string
	// MasterKey 为渠道 Key AES-256-GCM 主密钥（32 字节，来自
	// GAIGATE_SECRET_MASTER_KEY，config 已校验）。
	MasterKey []byte
}

// seed 内容常量（11.1 §3 seed 表格逐项冻结）。
const (
	seedGroupName          = "default"
	seedAdminUsername      = "admin"
	seedPrimaryChannel     = "demo-primary"
	seedFallbackChannel    = "demo-fallback"
	seedChannelProvider    = "openai" // openai 兼容协议，指向 mock（12）地址
	seedChannelBaseURL     = "http://127.0.0.1:18080"
	seedModel              = "gpt-4o-demo"
	seedPrimaryKeyPlaintext  = "sk-mock-a"
	seedFallbackKeyPlaintext = "sk-mock-b"
	seedInputPricePerM     = int64(15_000_000)  // 微元/M ≈ ¥15
	seedOutputPricePerM    = int64(60_000_000) // 微元/M ≈ ¥60
)

// seedPriceEffectiveFrom 是示例价格的生效起点（Unix 纪元零点：自始生效，
// 且保证重启不产生新行）。
var seedPriceEffectiveFrom = time.Unix(0, 0).UTC()

// Seed 幂等写入 11.1 §3 的 seed 数据（Go 实现：bcrypt 与 AES-GCM 密文
// 须运行时计算，见 §4 决策①）。幂等语义：按业务唯一键 ensure——
// 缺失则插入，存在且与 seed 规格不一致则更新，完全一致时不产生写
// （重启零抖动；密码与规格变更随重启收敛）。
//
// 多副本说明：GET_LOCK 只串行化迁移（11.1 §3）；seed 的幂等性依赖
// 业务唯一键与存在性检查，channel_keys / model_prices 无唯一键的场景
// 采用"已存在即跳过"，多副本同时首次 seed 存在竞态窗口——开发环境
// 单副本，属文档化取舍。
func Seed(db *gorm.DB, opts SeedOptions) error {
	if len(opts.MasterKey) != 32 {
		return fmt.Errorf("store: seed 需要 32 字节 master key（AES-256），实际 %d 字节", len(opts.MasterKey))
	}
	password := opts.AdminPassword
	if password == "" {
		password = defaultAdminPassword
	}

	return db.Transaction(func(tx *gorm.DB) error {
		now := time.Now().UTC()

		groupID, err := ensureGroup(tx, now)
		if err != nil {
			return err
		}
		if err := ensureAdmin(tx, groupID, password, now); err != nil {
			return err
		}
		primaryID, err := ensureChannel(tx, seedPrimaryChannel, 10, 70, now)
		if err != nil {
			return err
		}
		fallbackID, err := ensureChannel(tx, seedFallbackChannel, 5, 30, now)
		if err != nil {
			return err
		}
		if err := ensureChannelModel(tx, primaryID); err != nil {
			return err
		}
		if err := ensureChannelModel(tx, fallbackID); err != nil {
			return err
		}
		if err := ensureChannelKey(tx, primaryID, seedPrimaryKeyPlaintext, opts.MasterKey, now); err != nil {
			return err
		}
		if err := ensureChannelKey(tx, fallbackID, seedFallbackKeyPlaintext, opts.MasterKey, now); err != nil {
			return err
		}
		if err := ensureModelAlias(tx, primaryID, fallbackID, now); err != nil {
			return err
		}
		return ensureModelPrice(tx)
	})
}

// ensureGroup 确保 groups 存在 default 组，返回其 id。
func ensureGroup(tx *gorm.DB, now time.Time) (uint64, error) {
	var g Group
	err := tx.Where("name = ?", seedGroupName).First(&g).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		g = Group{Name: seedGroupName, CreatedAt: now}
		if err := tx.Create(&g).Error; err != nil {
			return 0, fmt.Errorf("store: seed 写入 groups: %w", err)
		}
		return g.ID, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: seed 查询 groups: %w", err)
	}
	return g.ID, nil
}

// ensureAdmin 确保 users 存在 admin（role=admin，default 组，启用）。
// 密码以环境为权威：与 bcrypt 校验不符时更新，一致时不产生写。
func ensureAdmin(tx *gorm.DB, groupID uint64, password string, now time.Time) error {
	var u User
	err := tx.Where("username = ?", seedAdminUsername).First(&u).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		hash, err := bcrypt.GenerateFromPassword([]byte(password), 10) // cost 10（01 §8）
		if err != nil {
			return fmt.Errorf("store: seed 计算 bcrypt: %w", err)
		}
		u = User{
			Username:     seedAdminUsername,
			PasswordHash: string(hash),
			Role:         RoleAdmin,
			GroupID:      groupID,
			Status:       UserStatusEnabled,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := tx.Create(&u).Error; err != nil {
			return fmt.Errorf("store: seed 写入 users: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: seed 查询 users: %w", err)
	}

	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) == nil &&
		u.Role == RoleAdmin && u.GroupID == groupID && u.Status == UserStatusEnabled {
		return nil // 与 seed 规格完全一致，零写
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 10)
	if err != nil {
		return fmt.Errorf("store: seed 计算 bcrypt: %w", err)
	}
	if err := tx.Model(&User{}).Where("id = ?", u.ID).Updates(map[string]any{
		"password_hash": string(hash),
		"role":          RoleAdmin,
		"group_id":      groupID,
		"status":        UserStatusEnabled,
		"updated_at":    now,
	}).Error; err != nil {
		return fmt.Errorf("store: seed 更新 users: %w", err)
	}
	return nil
}

// ensureChannel 确保示例渠道存在且规格一致，返回其 id。健康缓存列
// （health_status 等）归 14.3 异步维护，seed 不触碰。
func ensureChannel(tx *gorm.DB, name string, priority, weight int32, now time.Time) (uint64, error) {
	var c Channel
	err := tx.Where("name = ?", name).First(&c).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		c = Channel{
			Name: name, Provider: seedChannelProvider, BaseURL: seedChannelBaseURL,
			Priority: priority, Weight: weight, Enabled: true,
			// 显式置 unknown：GORM Create 会写入零值 ""、覆盖 DDL 默认（评审遗留项，
			// 见 docs/spec/02-遗留事项台账.md #1）。14.3 落地后该列由其异步维护。
			HealthStatus: "unknown",
			CreatedAt:    now, UpdatedAt: now,
		}
		if err := tx.Create(&c).Error; err != nil {
			return 0, fmt.Errorf("store: seed 写入 channels: %w", err)
		}
		return c.ID, nil
	}
	if err != nil {
		return 0, fmt.Errorf("store: seed 查询 channels: %w", err)
	}
	// 规格一致零写（幂等短路）：health_status 为空串视为待自愈，不走短路。
	if c.Provider == seedChannelProvider && c.BaseURL == seedChannelBaseURL &&
		c.Priority == priority && c.Weight == weight && c.Enabled && c.HealthStatus != "" {
		return c.ID, nil
	}
	updates := map[string]any{
		"provider":   seedChannelProvider,
		"base_url":   seedChannelBaseURL,
		"priority":   priority,
		"weight":     weight,
		"enabled":    true,
		"updated_at": now,
	}
	if c.HealthStatus == "" {
		// 旧库自愈：11.1 首版 seed 写入过 ""，仅归一为 unknown；
		// 不覆盖 14.3 将来写入的真实状态（disabled/degraded 等）。
		updates["health_status"] = "unknown"
	}
	if err := tx.Model(&Channel{}).Where("id = ?", c.ID).Updates(updates).Error; err != nil {
		return 0, fmt.Errorf("store: seed 更新 channels: %w", err)
	}
	return c.ID, nil
}

// ensureChannelModel 确保渠道服务 gpt-4o-demo（上游同名映射）。
func ensureChannelModel(tx *gorm.DB, channelID uint64) error {
	var cm ChannelModel
	err := tx.Where("channel_id = ? AND model = ?", channelID, seedModel).First(&cm).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		cm = ChannelModel{ChannelID: channelID, Model: seedModel, UpstreamModel: seedModel}
		if err := tx.Create(&cm).Error; err != nil {
			return fmt.Errorf("store: seed 写入 channel_models: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: seed 查询 channel_models: %w", err)
	}
	if cm.UpstreamModel == seedModel {
		return nil
	}
	if err := tx.Model(&ChannelModel{}).
		Where("channel_id = ? AND model = ?", channelID, seedModel).
		Update("upstream_model", seedModel).Error; err != nil {
		return fmt.Errorf("store: seed 更新 channel_models: %w", err)
	}
	return nil
}

// ensureChannelKey 确保渠道至少有一把 Key（AES-256-GCM 密文）。
// channel_keys 无业务唯一键（11.1 §3 DDL 冻结），幂等以存在性保证：
// 渠道已有任意 Key 即跳过（Key 轮换属管理操作，非 seed 职责）。
func ensureChannelKey(tx *gorm.DB, channelID uint64, plaintext string, masterKey []byte, now time.Time) error {
	var count int64
	if err := tx.Model(&ChannelKey{}).Where("channel_id = ?", channelID).
		Count(&count).Error; err != nil {
		return fmt.Errorf("store: seed 统计 channel_keys: %w", err)
	}
	if count > 0 {
		return nil
	}
	enc, err := secret.Encrypt([]byte(plaintext), masterKey)
	if err != nil {
		return fmt.Errorf("store: seed 加密渠道 Key: %w", err)
	}
	k := ChannelKey{
		ChannelID: channelID, ProviderKeyEnc: enc,
		Weight: 1, Status: ChannelKeyStatusActive, CreatedAt: now,
	}
	if err := tx.Create(&k).Error; err != nil {
		return fmt.Errorf("store: seed 写入 channel_keys: %w", err)
	}
	return nil
}

// ensureModelAlias 确保 gpt-4o-demo 别名：default 渠道 1、fallback [渠道 2]、
// strategy weighted（11.1 §3 seed 表格）。
func ensureModelAlias(tx *gorm.DB, primaryID, fallbackID uint64, now time.Time) error {
	fallbackJSON, err := json.Marshal([]uint64{fallbackID})
	if err != nil {
		return fmt.Errorf("store: seed 序列化 fallback_ids: %w", err)
	}
	var a ModelAlias
	err = tx.Where("name = ?", seedModel).First(&a).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		a = ModelAlias{
			Name: seedModel, DefaultChannelID: &primaryID,
			FallbackIDs: string(fallbackJSON), Strategy: "weighted",
			CreatedAt: now, UpdatedAt: now,
		}
		if err := tx.Create(&a).Error; err != nil {
			return fmt.Errorf("store: seed 写入 model_aliases: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: seed 查询 model_aliases: %w", err)
	}
	if a.DefaultChannelID != nil && *a.DefaultChannelID == primaryID &&
		a.FallbackIDs == string(fallbackJSON) && a.Strategy == "weighted" {
		return nil
	}
	if err := tx.Model(&ModelAlias{}).Where("id = ?", a.ID).Updates(map[string]any{
		"default_channel_id": primaryID,
		"fallback_ids":       string(fallbackJSON),
		"strategy":            "weighted",
		"updated_at":         now,
	}).Error; err != nil {
		return fmt.Errorf("store: seed 更新 model_aliases: %w", err)
	}
	return nil
}

// ensureModelPrice 确保 gpt-4o-demo 示例价格（15M/60M 微元/M，CNY）。
// model_prices 无业务唯一键（11.1 §3 DDL 冻结），幂等以存在性保证：
// 该模型已有价格行即跳过。
func ensureModelPrice(tx *gorm.DB) error {
	var count int64
	if err := tx.Model(&ModelPrice{}).Where("model = ?", seedModel).
		Count(&count).Error; err != nil {
		return fmt.Errorf("store: seed 统计 model_prices: %w", err)
	}
	if count > 0 {
		return nil
	}
	p := ModelPrice{
		Model: seedModel, InputPricePerM: seedInputPricePerM,
		OutputPricePerM: seedOutputPricePerM, Currency: "CNY",
		EffectiveFrom: seedPriceEffectiveFrom, Enabled: true,
	}
	if err := tx.Create(&p).Error; err != nil {
		return fmt.Errorf("store: seed 写入 model_prices: %w", err)
	}
	return nil
}
