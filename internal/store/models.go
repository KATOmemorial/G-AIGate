// Package store 是 11-store 的 MySQL 侧：schema 与版本化迁移（11.1）、
// 幂等 seed 与 GORM 模型。查询方法在 11.2/11.3 交付。
//
// schema 一律由 migrations/ 下的 SQL 文件管理（golang-migrate），
// 禁用 GORM AutoMigrate（01-工程规范 §1）；本包模型仅作表映射，
// 字段类型与列一一对应（BIGINT UNSIGNED→uint64、INT→int32、
// INT UNSIGNED→uint32、BIGINT→int64、TINYINT→int8/bool、FLOAT→float32）。
package store

import "time"

// users.status（11.1 §3：1 启用 0 禁用）
const (
	UserStatusEnabled  int8 = 1
	UserStatusDisabled int8 = 0
)

// users.role（11.1 §3：user|admin）
const (
	RoleUser  = "user"
	RoleAdmin = "admin"
)

// api_keys.status（11.1 §3：1 启用 0 吊销）
const (
	APIKeyStatusActive  int8 = 1
	APIKeyStatusRevoked int8 = 0
)

// channel_keys.status（11.1 §3：1 启用 0 吊销，与 api_keys 同义）
const (
	ChannelKeyStatusActive  int8 = 1
	ChannelKeyStatusRevoked int8 = 0
)

// usage_records.status（11.1 §3：success|failed|canceled）
const (
	UsageStatusSuccess = "success"
	UsageStatusFailed  = "failed"
	UsageStatusCanceled = "canceled"
)

// Group 对应 groups 表。
type Group struct {
	ID        uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name      string    `gorm:"column:name;not null"`
	CreatedAt time.Time `gorm:"column:created_at;not null"`
}

func (Group) TableName() string { return "groups" }

// User 对应 users 表。
type User struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Username     string    `gorm:"column:username;not null"`
	PasswordHash string    `gorm:"column:password_hash;not null"`
	Role         string    `gorm:"column:role;not null"`
	GroupID      uint64    `gorm:"column:group_id;not null"`
	Status       int8      `gorm:"column:status;not null"`
	CreatedAt    time.Time `gorm:"column:created_at;not null"`
	UpdatedAt    time.Time `gorm:"column:updated_at;not null"`
}

func (User) TableName() string { return "users" }

// APIKey 对应 api_keys 表。KeyHash 为 SHA-256 hex；明文不落库（01 §8）。
type APIKey struct {
	ID        uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	UserID    uint64     `gorm:"column:user_id;not null"`
	Name      string     `gorm:"column:name;not null"`
	KeyPrefix string     `gorm:"column:key_prefix;not null"`
	KeyHash   string     `gorm:"column:key_hash;not null"`
	Status    int8       `gorm:"column:status;not null"`
	CreatedAt time.Time  `gorm:"column:created_at;not null"`
	ExpiredAt *time.Time `gorm:"column:expired_at"`
}

func (APIKey) TableName() string { return "api_keys" }

// Channel 对应 channels 表。HealthStatus/ErrorRateCache/LatencyP99Cache
// 是健康模块（14.3）异步维护的展示用缓存列，路由判定不读（00 §7.1）。
type Channel struct {
	ID             uint64     `gorm:"column:id;primaryKey;autoIncrement"`
	Name           string     `gorm:"column:name;not null"`
	Provider       string     `gorm:"column:provider;not null"`
	BaseURL        string     `gorm:"column:base_url;not null"`
	Priority       int32      `gorm:"column:priority;not null"`
	Weight         int32      `gorm:"column:weight;not null"`
	Enabled        bool       `gorm:"column:enabled;not null"`
	HealthStatus   string     `gorm:"column:health_status;not null"`
	ErrorRateCache float32    `gorm:"column:error_rate_cache;not null"`
	LatencyP99     int32      `gorm:"column:latency_p99_cache;not null"`
	LastCheckedAt  *time.Time `gorm:"column:last_checked_at"`
	CreatedAt      time.Time  `gorm:"column:created_at;not null"`
	UpdatedAt      time.Time  `gorm:"column:updated_at;not null"`
}

func (Channel) TableName() string { return "channels" }

// ChannelModel 对应 channel_models 表（复合主键，无自增 id）。
// Model 为对外模型名（别名），UpstreamModel 为映射后的上游模型名。
type ChannelModel struct {
	ChannelID     uint64 `gorm:"column:channel_id;primaryKey"`
	Model         string `gorm:"column:model;primaryKey"`
	UpstreamModel string `gorm:"column:upstream_model;not null"`
}

func (ChannelModel) TableName() string { return "channel_models" }

// ChannelKey 对应 channel_keys 表。ProviderKeyEnc 为 AES-256-GCM 密文
// （nonce 前置拼接，internal/secret），解密归 14.2，store 不解密（11-L1 §1）。
type ChannelKey struct {
	ID            uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	ChannelID     uint64    `gorm:"column:channel_id;not null"`
	ProviderKeyEnc []byte   `gorm:"column:provider_key_enc;not null"`
	Weight        int32     `gorm:"column:weight;not null"`
	Status        int8      `gorm:"column:status;not null"`
	CreatedAt     time.Time `gorm:"column:created_at;not null"`
}

func (ChannelKey) TableName() string { return "channel_keys" }

// ModelAlias 对应 model_aliases 表。FallbackIDs 为 JSON 数组文本
// （如 "[2,3]"），解析归 11.2 访问层。
type ModelAlias struct {
	ID               uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Name             string    `gorm:"column:name;not null"`
	DisplayName      string    `gorm:"column:display_name;not null"`
	DefaultChannelID *uint64   `gorm:"column:default_channel_id"`
	FallbackIDs      string    `gorm:"column:fallback_ids"`
	Strategy         string    `gorm:"column:strategy;not null"`
	CreatedAt        time.Time `gorm:"column:created_at;not null"`
	UpdatedAt        time.Time `gorm:"column:updated_at;not null"`
}

func (ModelAlias) TableName() string { return "model_aliases" }

// ModelPrice 对应 model_prices 表。价格为微元/百万 token（00 §7.4）。
type ModelPrice struct {
	ID              uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	Model           string    `gorm:"column:model;not null"`
	InputPricePerM  int64     `gorm:"column:input_price_per_m;not null"`
	OutputPricePerM int64     `gorm:"column:output_price_per_m;not null"`
	Currency        string    `gorm:"column:currency;not null"`
	EffectiveFrom   time.Time `gorm:"column:effective_from;not null"`
	Enabled         bool      `gorm:"column:enabled;not null"`
}

func (ModelPrice) TableName() string { return "model_prices" }

// UsageRecord 对应 usage_records 表。只追加、不更新（11-L1 §3 决策②）。
// Cost 单位微元 int64（00 §7.4）。
type UsageRecord struct {
	ID           uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	RequestID    string    `gorm:"column:request_id;not null"`
	UserID       uint64    `gorm:"column:user_id;not null"`
	APIKeyID     uint64    `gorm:"column:api_key_id;not null"`
	ChannelID    uint64    `gorm:"column:channel_id;not null"`
	Model        string    `gorm:"column:model;not null"`
	InputTokens  uint32    `gorm:"column:input_tokens;not null"`
	OutputTokens uint32    `gorm:"column:output_tokens;not null"`
	Cost         int64     `gorm:"column:cost;not null"`
	LatencyMS    uint32    `gorm:"column:latency_ms;not null"`
	Status       string    `gorm:"column:status;not null"`
	Estimated    int8      `gorm:"column:estimated;not null"`
	CreatedAt    time.Time `gorm:"column:created_at;not null"`
}

func (UsageRecord) TableName() string { return "usage_records" }

// Quota 对应 quotas 表。限额为 0 表示不限；CostLimit 单位微元。
type Quota struct {
	ID         uint64  `gorm:"column:id;primaryKey;autoIncrement"`
	TargetType string  `gorm:"column:target_type;not null"`
	TargetID   uint64  `gorm:"column:target_id;not null"`
	Period     string  `gorm:"column:period;not null"`
	TokenLimit int64   `gorm:"column:token_limit;not null"`
	CostLimit  int64   `gorm:"column:cost_limit;not null"`
	SoftRatio  float64 `gorm:"column:soft_ratio;not null"`
}

func (Quota) TableName() string { return "quotas" }

// Bill 对应 bills 表。TotalCost 单位微元；Period 为 'YYYY-MM'。
type Bill struct {
	ID         uint64    `gorm:"column:id;primaryKey;autoIncrement"`
	UserID     uint64    `gorm:"column:user_id;not null"`
	GroupID    uint64    `gorm:"column:group_id;not null"`
	Period     string    `gorm:"column:period;not null"`
	TotalCost  int64     `gorm:"column:total_cost;not null"`
	TokenTotal int64     `gorm:"column:token_total;not null"`
	CreatedAt  time.Time `gorm:"column:created_at;not null"`
}

func (Bill) TableName() string { return "bills" }
