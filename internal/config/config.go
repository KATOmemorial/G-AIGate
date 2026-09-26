// Package config 提供 G-AIGate 的配置结构、加载与 fail-fast 校验。
// 结构体与字段按 10.1（docs/spec/10-骨架与装配/10.1-entry-config.md §3）冻结，
// 后续模块按 section 增量扩展时须走 00-总纲 §10 变更流程。
package config

import "time"

// Config 是配置根结构，按模块 ownership 分区（01-工程规范 §7）。
type Config struct {
	Server  ServerConfig  `yaml:"server" mapstructure:"server"`
	Log     LogConfig     `yaml:"log" mapstructure:"log"`
	MySQL   MySQLConfig   `yaml:"mysql" mapstructure:"mysql"`
	Redis   RedisConfig   `yaml:"redis" mapstructure:"redis"`
	Proxy   ProxyConfig   `yaml:"proxy" mapstructure:"proxy"`
	Channel ChannelConfig `yaml:"channel" mapstructure:"channel"`
	Router  RouterConfig  `yaml:"router" mapstructure:"router"`
	Meter   MeterConfig   `yaml:"meter" mapstructure:"meter"`
	Secret  SecretConfig  `yaml:"secret" mapstructure:"secret"`
}

// ServerConfig 网关 HTTP 服务（10 骨架）。
type ServerConfig struct {
	// 监听地址，默认 ":8080"
	Addr string `yaml:"addr" mapstructure:"addr"`
	// Prometheus 抓取地址，默认 ":9101"（compose 已按此配置）
	MetricsAddr string `yaml:"metrics-addr" mapstructure:"metrics-addr"`
	// 优雅关闭等待上限，默认 60s
	ShutdownTimeout time.Duration `yaml:"shutdown-timeout" mapstructure:"shutdown-timeout"`
}

// LogConfig 日志（01-工程规范 §4，slog JSONHandler）。
type LogConfig struct {
	// debug|info|warn|error，默认 info
	Level string `yaml:"level" mapstructure:"level"`
}

// MySQLConfig 数据库连接（11 store）。
type MySQLConfig struct {
	Host     string `yaml:"host" mapstructure:"host"`           // 默认 127.0.0.1
	Port     int    `yaml:"port" mapstructure:"port"`           // 默认 3306
	User     string `yaml:"user" mapstructure:"user"`           // 默认 root
	Database string `yaml:"database" mapstructure:"database"`   // 默认 gaigate
	// 仅环境变量 GAIGATE_MYSQL_PASSWORD；yaml 中出现即报错（防密钥入库）
	Password string `yaml:"password" mapstructure:"password"`
}

// RedisConfig Redis 连接（11 store）。
type RedisConfig struct {
	Addr string `yaml:"addr" mapstructure:"addr"` // 默认 127.0.0.1:6379
	DB   int    `yaml:"db" mapstructure:"db"`     // 默认 0
}

// ProxyConfig 流式转发（15 proxy）。
type ProxyConfig struct {
	// 首字节超时，默认 10s
	TTFBTimeout time.Duration `yaml:"ttfb-timeout" mapstructure:"ttfb-timeout"`
	// 整体超时，默认 300s
	OverallTimeout time.Duration `yaml:"overall-timeout" mapstructure:"overall-timeout"`
	// 有界缓冲 chunk 数，默认 64
	StreamBuffer int `yaml:"stream-buffer" mapstructure:"stream-buffer"`
}

// ChannelConfig 渠道健康（14 channel）。
type ChannelConfig struct {
	// 探活间隔，默认 30s
	ProbeInterval time.Duration `yaml:"probe-interval" mapstructure:"probe-interval"`
	// 连续失败→禁用阈值，默认 5
	FailureThreshold int `yaml:"failure-threshold" mapstructure:"failure-threshold"`
	// 禁用冷却时长，默认 60s
	DisableDuration time.Duration `yaml:"disable-duration" mapstructure:"disable-duration"`
}

// RouterConfig 路由与熔断（16 router）。
type RouterConfig struct {
	// 熔断滑动窗口，默认 60s
	BreakerWindow time.Duration `yaml:"breaker-window" mapstructure:"breaker-window"`
	// 熔断最小样本数，默认 20
	BreakerMinSamples int `yaml:"breaker-min-samples" mapstructure:"breaker-min-samples"`
	// 熔断错误率阈值，默认 0.5，合法范围 (0,1]
	BreakerErrThreshold float64 `yaml:"breaker-err-threshold" mapstructure:"breaker-err-threshold"`
	// open→half-open 时长，默认 30s
	BreakerOpenDuration time.Duration `yaml:"breaker-open-duration" mapstructure:"breaker-open-duration"`
	// weighted|cost_first|latency_first|fallback_chain
	DefaultStrategy string `yaml:"default-strategy" mapstructure:"default-strategy"`
}

// MeterConfig 计量与结算（17 meter）。
type MeterConfig struct {
	// 估算回退：每 token 字符数，默认 4（17.1）
	EstimateCharsPerToken float64 `yaml:"estimate-chars-per-token" mapstructure:"estimate-chars-per-token"`
	// 落账队列长度，默认 1024（11.3）
	SettleQueueSize int `yaml:"settle-queue-size" mapstructure:"settle-queue-size"`
}

// SecretConfig 敏感项（01-工程规范 §8 安全基线）。
type SecretConfig struct {
	// 仅环境变量 GAIGATE_SECRET_MASTER_KEY（渠道 Key AES-256-GCM，14.2 用）；
	// base64 编码，解码后须为 32 字节；yaml 中出现即报错
	MasterKey string `yaml:"master-key" mapstructure:"master-key"`
}
