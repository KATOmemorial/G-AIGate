package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

// ModelRepo 是模型别名（model_aliases）的读写层（11.2 §3 冻结签名；
// 跨模块契约见 00 §6.3）。消费方：16.1/16.2（路由解析别名）、14.1（管理 CRUD）。
type ModelRepo interface {
	// Resolve 按别名解析出 ModelAlias（default_channel_id + fallback_ids + strategy），走缓存。
	// 未命中返回 ErrNotFound。
	Resolve(ctx context.Context, name string) (*ModelAlias, error)
	// Create 写入别名；未显式赋值的默认列按 DDL 默认补齐（01 §3.1）。
	Create(ctx context.Context, a *ModelAlias) error
	// Update 更新别名可管理字段；未命中返回 ErrNotFound。
	Update(ctx context.Context, a *ModelAlias) error
	// Delete 按 id 删除别名；未命中返回 ErrNotFound。
	Delete(ctx context.Context, id int64) error
}

// modelRepo 是 ModelRepo 的 MySQL 实现。
type modelRepo struct {
	db *gorm.DB
}

// NewModelRepo 装配别名访问层。
func NewModelRepo(db *gorm.DB) ModelRepo {
	return &modelRepo{db: db}
}

// Resolve 按 uk_alias_name 唯一键点查别名。
func (r *modelRepo) Resolve(ctx context.Context, name string) (*ModelAlias, error) {
	var a ModelAlias
	if err := r.db.WithContext(ctx).Where("name = ?", name).First(&a).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("store: 别名 %q: %w", name, ErrNotFound)
		}
		return nil, fmt.Errorf("store: 查询别名 %q: %w", name, err)
	}
	return &a, nil
}

// aliasRow 是 model_aliases 的写入投影。fallback_ids 是 JSON 列：
// 空字符串 '' 不是合法 JSON 文档（MySQL 8 报 Error 3140），故提为 *string，
// 空值写 NULL 而非零值 ""（GORM 会写入零值覆盖列语义，01 §3.1）。
type aliasRow struct {
	ID               uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name             string    `gorm:"column:name"`
	DisplayName      string    `gorm:"column:display_name"`
	DefaultChannelID *uint64   `gorm:"column:default_channel_id"`
	FallbackIDs      *string   `gorm:"column:fallback_ids"`
	Strategy         string    `gorm:"column:strategy"`
	CreatedAt        time.Time `gorm:"column:created_at"`
	UpdatedAt        time.Time `gorm:"column:updated_at"`
}

func (aliasRow) TableName() string { return "model_aliases" }

// nullableJSON 把空串映射为 NULL，非空串取地址（供 JSON 列写入）。
func nullableJSON(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Create 写入别名。display_name 默认 ''、strategy 默认 'weighted'（DDL）——
// GORM 零值会覆盖默认，故空 strategy 显式补齐（01 §3.1）。
func (r *modelRepo) Create(ctx context.Context, a *ModelAlias) error {
	if a == nil {
		return fmt.Errorf("store: Create 别名为 nil")
	}
	now := time.Now().UTC()
	if a.CreatedAt.IsZero() {
		a.CreatedAt = now
	}
	a.UpdatedAt = now
	if a.Strategy == "" {
		a.Strategy = "weighted"
	}
	row := aliasRow{
		Name:             a.Name,
		DisplayName:      a.DisplayName,
		DefaultChannelID: a.DefaultChannelID,
		FallbackIDs:      nullableJSON(a.FallbackIDs),
		Strategy:         a.Strategy,
		CreatedAt:        a.CreatedAt,
		UpdatedAt:        a.UpdatedAt,
	}
	if err := r.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("store: 插入别名 %q: %w", a.Name, err)
	}
	a.ID = row.ID
	return nil
}

// Update 更新别名。fallback_ids 空串写 NULL（JSON 列不接受 ''）。
func (r *modelRepo) Update(ctx context.Context, a *ModelAlias) error {
	if a == nil || a.ID == 0 {
		return fmt.Errorf("store: Update 别名需要有效 id")
	}
	var fallback any // nil → SQL NULL；JSON 列不接受空串
	if a.FallbackIDs != "" {
		fallback = a.FallbackIDs
	}
	res := r.db.WithContext(ctx).Model(&ModelAlias{}).Where("id = ?", a.ID).Updates(map[string]any{
		"name":               a.Name,
		"display_name":       a.DisplayName,
		"default_channel_id": a.DefaultChannelID,
		"fallback_ids":       fallback,
		"strategy":           a.Strategy,
		"updated_at":         time.Now().UTC(),
	})
	if res.Error != nil {
		return fmt.Errorf("store: 更新别名 %d: %w", a.ID, res.Error)
	}
	if res.RowsAffected == 0 {
		var cnt int64
		if err := r.db.WithContext(ctx).Model(&ModelAlias{}).Where("id = ?", a.ID).
			Count(&cnt).Error; err != nil {
			return fmt.Errorf("store: 校验别名 %d 存在性: %w", a.ID, err)
		}
		if cnt == 0 {
			return fmt.Errorf("store: 更新别名 %d: %w", a.ID, ErrNotFound)
		}
	}
	return nil
}

// Delete 按 id 删除别名。
func (r *modelRepo) Delete(ctx context.Context, id int64) error {
	res := r.db.WithContext(ctx).Where("id = ?", id).Delete(&ModelAlias{})
	if res.Error != nil {
		return fmt.Errorf("store: 删除别名 %d: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return fmt.Errorf("store: 删除别名 %d: %w", id, ErrNotFound)
	}
	return nil
}

// ParseFallbackIDs 容错解析 model_aliases.fallback_ids（JSON 数组文本）。
// 空串（SQL NULL）或坏数据一律返回 nil（按无回退处理），绝不 panic——
// 坏数据记一条 Warn 日志（11.2 §7）。解析结果供 16 路由组装回退链。
func ParseFallbackIDs(raw string) []uint64 {
	if raw == "" {
		return nil
	}
	var ids []uint64
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		slog.Warn("别名 fallback_ids 解析失败，按无回退处理",
			"fallback_ids", raw, "err", err.Error())
		return nil
	}
	return ids
}
