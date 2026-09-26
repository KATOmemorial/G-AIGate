package config

import (
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// 校验报错中的字段名统一使用「环境变量风格」的小写下划线路径
// （如 server.addr、proxy.ttfb_timeout、secret.master_key），与
// GAIGATE_<SECTION>_<FIELD> 覆盖规则一一对应（10.1 §3）。

// log.level 严格小写枚举（10.1 §3），不归一化
var validLogLevels = map[string]bool{
	"debug": true, "info": true, "warn": true, "error": true,
}

// router.default-strategy 枚举（10.1 §3）
var validStrategies = map[string]bool{
	"weighted":       true,
	"cost_first":     true,
	"latency_first":  true,
	"fallback_chain": true,
}

// Load 一步完成：配置文件加载（可选）→ 环境变量覆盖 → fail-fast 校验（10.1 §3）。
//
// path 指向的文件不存在时，仅以默认值 + 环境变量启动（10.1 §5 验收：
// 仓库不携带 config.yaml，`go run ./cmd/g-aigate` 须能直接启动）。
// 敏感项 mysql.password / secret.master-key 只认环境变量，配置文件中出现即报错
// （01-工程规范 §7：配置文件与仓库中不出现任何密钥）。
func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetEnvPrefix("GAIGATE")
	// viper 内部 key 为 kebab-case（server.metrics-addr），
	// 环境变量为下划线（GAIGATE_SERVER_METRICS_ADDR），两者都用 "_" 展平
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))

	if fileExists(path) {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("读取配置文件 %s: %w", path, err)
		}
		// 防密钥入库：在注册默认值与 AutomaticEnv 之前检查，此时 IsSet
		// 只可能来自配置文件本身（10.1 §3：yaml 中出现即报错）
		for _, key := range []string{"mysql.password", "secret.master-key"} {
			if v.IsSet(key) {
				return nil, fmt.Errorf(
					"config: %s 为敏感项，只允许从环境变量读取，不得写入配置文件（01-工程规范 §7）",
					strings.ReplaceAll(key, "-", "_"))
			}
		}
	}

	setDefaults(v)
	// AutomaticEnv + 全量默认值：所有 key 均在 AllKeys 中，
	// 保证 Unmarshal 能取到 GAIGATE_* 覆盖值
	v.AutomaticEnv()

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// setDefaults 注册全量默认值（10.1 §3 各字段注释），与 config.example.yaml 一一对应。
// 敏感项默认值为空串占位：仅为把 key 纳入 AllKeys 以支持环境变量覆盖。
func setDefaults(v *viper.Viper) {
	v.SetDefault("server.addr", ":8080")
	v.SetDefault("server.metrics-addr", ":9101")
	v.SetDefault("server.shutdown-timeout", 60*time.Second)
	v.SetDefault("log.level", "info")
	v.SetDefault("mysql.host", "127.0.0.1")
	v.SetDefault("mysql.port", 3306)
	v.SetDefault("mysql.user", "root")
	v.SetDefault("mysql.database", "gaigate")
	v.SetDefault("mysql.password", "")
	v.SetDefault("redis.addr", "127.0.0.1:6379")
	v.SetDefault("redis.db", 0)
	v.SetDefault("proxy.ttfb-timeout", 10*time.Second)
	v.SetDefault("proxy.overall-timeout", 300*time.Second)
	v.SetDefault("proxy.stream-buffer", 64)
	v.SetDefault("channel.probe-interval", 30*time.Second)
	v.SetDefault("channel.failure-threshold", 5)
	v.SetDefault("channel.disable-duration", 60*time.Second)
	v.SetDefault("router.breaker-window", 60*time.Second)
	v.SetDefault("router.breaker-min-samples", 20)
	v.SetDefault("router.breaker-err-threshold", 0.5)
	v.SetDefault("router.breaker-open-duration", 30*time.Second)
	v.SetDefault("router.default-strategy", "weighted")
	v.SetDefault("meter.estimate-chars-per-token", 4.0)
	v.SetDefault("meter.settle-queue-size", 1024)
	v.SetDefault("secret.master-key", "")
}

// validate 执行 fail-fast 校验（10.1 §3 校验规则），收集全部违规后一次性报出。
func (c *Config) validate() error {
	var errs []string

	if err := checkListenAddr("server.addr", c.Server.Addr); err != nil {
		errs = append(errs, err.Error())
	}
	if err := checkListenAddr("server.metrics_addr", c.Server.MetricsAddr); err != nil {
		errs = append(errs, err.Error())
	}
	if c.Server.ShutdownTimeout <= 0 {
		errs = append(errs, fmt.Sprintf("server.shutdown_timeout: 须大于 0，实际 %s", c.Server.ShutdownTimeout))
	}

	if !validLogLevels[c.Log.Level] {
		errs = append(errs, fmt.Sprintf("log.level: 须为 debug|info|warn|error 之一（严格小写），实际 %q", c.Log.Level))
	}

	if c.MySQL.Port < 1 || c.MySQL.Port > 65535 {
		errs = append(errs, fmt.Sprintf("mysql.port: 须在 1~65535，实际 %d", c.MySQL.Port))
	}

	if c.Proxy.TTFBTimeout <= 0 {
		errs = append(errs, fmt.Sprintf("proxy.ttfb_timeout: 须大于 0，实际 %s", c.Proxy.TTFBTimeout))
	}
	if c.Proxy.OverallTimeout <= 0 {
		errs = append(errs, fmt.Sprintf("proxy.overall_timeout: 须大于 0，实际 %s", c.Proxy.OverallTimeout))
	}
	if c.Proxy.StreamBuffer <= 0 {
		errs = append(errs, fmt.Sprintf("proxy.stream_buffer: 须大于 0，实际 %d", c.Proxy.StreamBuffer))
	}

	if c.Channel.ProbeInterval <= 0 {
		errs = append(errs, fmt.Sprintf("channel.probe_interval: 须大于 0，实际 %s", c.Channel.ProbeInterval))
	}
	if c.Channel.FailureThreshold <= 0 {
		errs = append(errs, fmt.Sprintf("channel.failure_threshold: 须大于 0，实际 %d", c.Channel.FailureThreshold))
	}
	if c.Channel.DisableDuration <= 0 {
		errs = append(errs, fmt.Sprintf("channel.disable_duration: 须大于 0，实际 %s", c.Channel.DisableDuration))
	}

	if c.Router.BreakerWindow <= 0 {
		errs = append(errs, fmt.Sprintf("router.breaker_window: 须大于 0，实际 %s", c.Router.BreakerWindow))
	}
	if c.Router.BreakerMinSamples <= 0 {
		errs = append(errs, fmt.Sprintf("router.breaker_min_samples: 须大于 0，实际 %d", c.Router.BreakerMinSamples))
	}
	if c.Router.BreakerErrThreshold <= 0 || c.Router.BreakerErrThreshold > 1 {
		errs = append(errs, fmt.Sprintf("router.breaker_err_threshold: 须在 (0,1]，实际 %g", c.Router.BreakerErrThreshold))
	}
	if c.Router.BreakerOpenDuration <= 0 {
		errs = append(errs, fmt.Sprintf("router.breaker_open_duration: 须大于 0，实际 %s", c.Router.BreakerOpenDuration))
	}
	if !validStrategies[c.Router.DefaultStrategy] {
		errs = append(errs, fmt.Sprintf(
			"router.default_strategy: 须为 weighted|cost_first|latency_first|fallback_chain 之一，实际 %q",
			c.Router.DefaultStrategy))
	}

	if c.Meter.SettleQueueSize <= 0 {
		errs = append(errs, fmt.Sprintf("meter.settle_queue_size: 须大于 0，实际 %d", c.Meter.SettleQueueSize))
	}

	// MasterKey 必填且 base64 解码后恰 32 字节（AES-256；10.1 §3 校验规则/§4 决策④）
	if c.Secret.MasterKey == "" {
		errs = append(errs, "secret.master_key: 必填（仅环境变量 GAIGATE_SECRET_MASTER_KEY）")
	} else {
		raw, err := base64.StdEncoding.DecodeString(c.Secret.MasterKey)
		if err != nil {
			errs = append(errs, fmt.Sprintf("secret.master_key: base64 解码失败: %v", err))
		} else if len(raw) != 32 {
			errs = append(errs, fmt.Sprintf("secret.master_key: 解码后须为 32 字节（AES-256），实际 %d 字节", len(raw)))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("config 校验失败: %s", strings.Join(errs, "; "))
	}
	return nil
}

// checkListenAddr 校验监听地址为 host:port 且端口在 1~65535（10.1 §3 端口规则）。
func checkListenAddr(field, addr string) error {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s: 须为 host:port 形式（如 \":8080\"），实际 %q: %v", field, addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("%s: 端口须为数字，实际 %q", field, portStr)
	}
	if port < 1 || port > 65535 {
		return fmt.Errorf("%s: 端口须在 1~65535，实际 %d", field, port)
	}
	return nil
}
