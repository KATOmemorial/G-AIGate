package config

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 开发密钥（10.1 §5）：base64("12345678901234567890123456789012")，解码后恰 32 字节
const devMasterKey = "MTIzNDU2Nzg5MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTI="

// missingPath 返回一个不存在的配置文件路径：Load 走「默认值 + 环境变量」分支
func missingPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "config.yaml")
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

// TestLoad_Defaults 合法配置：无配置文件，全量默认值（10.1 §3 默认值逐项核对）。
func TestLoad_Defaults(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load(missingPath(t))
	require.NoError(t, err)

	assert.Equal(t, ":8080", cfg.Server.Addr)
	assert.Equal(t, ":9101", cfg.Server.MetricsAddr)
	assert.Equal(t, 60*time.Second, cfg.Server.ShutdownTimeout)
	assert.Equal(t, "info", cfg.Log.Level)
	assert.Equal(t, "127.0.0.1", cfg.MySQL.Host)
	assert.Equal(t, 3306, cfg.MySQL.Port)
	assert.Equal(t, "root", cfg.MySQL.User)
	assert.Equal(t, "gaigate", cfg.MySQL.Database)
	assert.Equal(t, "", cfg.MySQL.Password)
	assert.Equal(t, "127.0.0.1:6379", cfg.Redis.Addr)
	assert.Equal(t, 0, cfg.Redis.DB)
	assert.Equal(t, 10*time.Second, cfg.Proxy.TTFBTimeout)
	assert.Equal(t, 300*time.Second, cfg.Proxy.OverallTimeout)
	assert.Equal(t, 64, cfg.Proxy.StreamBuffer)
	assert.Equal(t, 30*time.Second, cfg.Channel.ProbeInterval)
	assert.Equal(t, 5, cfg.Channel.FailureThreshold)
	assert.Equal(t, 60*time.Second, cfg.Channel.DisableDuration)
	assert.Equal(t, 60*time.Second, cfg.Router.BreakerWindow)
	assert.Equal(t, 20, cfg.Router.BreakerMinSamples)
	assert.Equal(t, 0.5, cfg.Router.BreakerErrThreshold)
	assert.Equal(t, 30*time.Second, cfg.Router.BreakerOpenDuration)
	assert.Equal(t, "weighted", cfg.Router.DefaultStrategy)
	assert.Equal(t, 4.0, cfg.Meter.EstimateCharsPerToken)
	assert.Equal(t, 1024, cfg.Meter.SettleQueueSize)
	assert.Equal(t, devMasterKey, cfg.Secret.MasterKey)
}

// TestLoad_ExampleYAML config.example.yaml 可加载且与默认值一一对应（10.1 §4 决策③）。
func TestLoad_ExampleYAML(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	cfg, err := Load("../../config.example.yaml")
	require.NoError(t, err)
	assert.Equal(t, ":8080", cfg.Server.Addr)
	assert.Equal(t, 3306, cfg.MySQL.Port)
	assert.Equal(t, 300*time.Second, cfg.Proxy.OverallTimeout)
	assert.Equal(t, "weighted", cfg.Router.DefaultStrategy)
	assert.Equal(t, 1024, cfg.Meter.SettleQueueSize)
}

// TestLoad_EnvOverride 环境变量覆盖 GAIGATE_<SECTION>_<FIELD>（含 kebab-case 展平），
// 未覆盖项保持默认（10.1 §3 覆盖规则）。
func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	t.Setenv("GAIGATE_SERVER_ADDR", "127.0.0.1:9090")
	t.Setenv("GAIGATE_MYSQL_PORT", "3307")
	t.Setenv("GAIGATE_MYSQL_PASSWORD", "env-only-pw")
	t.Setenv("GAIGATE_REDIS_DB", "3")
	t.Setenv("GAIGATE_LOG_LEVEL", "debug")
	t.Setenv("GAIGATE_PROXY_TTFB_TIMEOUT", "15s")
	t.Setenv("GAIGATE_ROUTER_DEFAULT_STRATEGY", "cost_first")
	t.Setenv("GAIGATE_METER_SETTLE_QUEUE_SIZE", "2048")

	cfg, err := Load(missingPath(t))
	require.NoError(t, err)

	assert.Equal(t, "127.0.0.1:9090", cfg.Server.Addr)
	assert.Equal(t, 3307, cfg.MySQL.Port)
	assert.Equal(t, "env-only-pw", cfg.MySQL.Password)
	assert.Equal(t, 3, cfg.Redis.DB)
	assert.Equal(t, "debug", cfg.Log.Level)
	assert.Equal(t, 15*time.Second, cfg.Proxy.TTFBTimeout)
	assert.Equal(t, "cost_first", cfg.Router.DefaultStrategy)
	assert.Equal(t, 2048, cfg.Meter.SettleQueueSize)
	// 未覆盖项保持默认
	assert.Equal(t, ":9101", cfg.Server.MetricsAddr)
	assert.Equal(t, 300*time.Second, cfg.Proxy.OverallTimeout)
}

// TestLoad_EnvOverridesYAML 环境变量优先级高于配置文件（覆盖语义，10.1 §3）。
func TestLoad_EnvOverridesYAML(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	path := writeConfig(t, "server:\n  addr: \":7070\"\n")
	t.Setenv("GAIGATE_SERVER_ADDR", "127.0.0.1:9091")
	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:9091", cfg.Server.Addr)
}

// TestLoad_SecretsInYAML 敏感项写入 yaml 即报错——防密钥入库（10.1 §3，01 §7）。
func TestLoad_SecretsInYAML(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "mysql.password 出现即报错（含空值）",
			yaml:    "mysql:\n  password: \"\"\n",
			wantErr: "mysql.password",
		},
		{
			name:    "secret.master-key 出现即报错",
			yaml:    "secret:\n  master-key: \"" + devMasterKey + "\"\n",
			wantErr: "secret.master_key",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
			_, err := Load(writeConfig(t, tt.yaml))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestLoad_MasterKey 必填 + base64 解码后恰 32 字节（10.1 §3 校验规则/§4 决策④）。
func TestLoad_MasterKey(t *testing.T) {
	key16 := base64.StdEncoding.EncodeToString(make([]byte, 16)) // 合法 base64，但解码 16 字节
	tests := []struct {
		name    string
		key     string
		wantErr string
	}{
		{"缺失必填报错", "", "secret.master_key: 必填"},
		{"非 base64 报错", "short", "secret.master_key"},
		{"解码非 32 字节报错", key16, "32 字节"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("GAIGATE_SECRET_MASTER_KEY", tt.key)
			_, err := Load(missingPath(t))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestLoad_InvalidFields 非法字段逐条 fail-fast，报错含字段名（10.1 §3 校验规则）。
func TestLoad_InvalidFields(t *testing.T) {
	tests := []struct {
		name    string
		env     string
		value   string
		wantErr string
	}{
		{"server.addr 端口越界", "GAIGATE_SERVER_ADDR", ":99999", "server.addr"},
		{"server.addr 缺少端口", "GAIGATE_SERVER_ADDR", "8080", "server.addr"},
		{"server.addr 端口非数字", "GAIGATE_SERVER_ADDR", ":abc", "server.addr"},
		{"server.metrics_addr 端口越界", "GAIGATE_SERVER_METRICS_ADDR", ":70000", "server.metrics_addr"},
		{"server.shutdown_timeout 零值", "GAIGATE_SERVER_SHUTDOWN_TIMEOUT", "0s", "server.shutdown_timeout"},
		{"log.level 非法枚举", "GAIGATE_LOG_LEVEL", "verbose", "log.level"},
		{"log.level 大写不归一化", "GAIGATE_LOG_LEVEL", "INFO", "log.level"},
		{"mysql.port 零值", "GAIGATE_MYSQL_PORT", "0", "mysql.port"},
		{"mysql.port 越界", "GAIGATE_MYSQL_PORT", "70000", "mysql.port"},
		{"proxy.ttfb_timeout 零值", "GAIGATE_PROXY_TTFB_TIMEOUT", "0s", "proxy.ttfb_timeout"},
		{"proxy.overall_timeout 负值", "GAIGATE_PROXY_OVERALL_TIMEOUT", "-1s", "proxy.overall_timeout"},
		{"proxy.stream_buffer 零值", "GAIGATE_PROXY_STREAM_BUFFER", "0", "proxy.stream_buffer"},
		{"proxy.stream_buffer 负值", "GAIGATE_PROXY_STREAM_BUFFER", "-8", "proxy.stream_buffer"},
		{"channel.probe_interval 零值", "GAIGATE_CHANNEL_PROBE_INTERVAL", "0s", "channel.probe_interval"},
		{"channel.failure_threshold 零值", "GAIGATE_CHANNEL_FAILURE_THRESHOLD", "0", "channel.failure_threshold"},
		{"channel.disable_duration 零值", "GAIGATE_CHANNEL_DISABLE_DURATION", "0s", "channel.disable_duration"},
		{"router.breaker_window 零值", "GAIGATE_ROUTER_BREAKER_WINDOW", "0s", "router.breaker_window"},
		{"router.breaker_min_samples 零值", "GAIGATE_ROUTER_BREAKER_MIN_SAMPLES", "0", "router.breaker_min_samples"},
		{"router.breaker_err_threshold 零值", "GAIGATE_ROUTER_BREAKER_ERR_THRESHOLD", "0", "router.breaker_err_threshold"},
		{"router.breaker_err_threshold 超上界", "GAIGATE_ROUTER_BREAKER_ERR_THRESHOLD", "1.5", "router.breaker_err_threshold"},
		{"router.breaker_open_duration 零值", "GAIGATE_ROUTER_BREAKER_OPEN_DURATION", "0s", "router.breaker_open_duration"},
		{"router.default_strategy 非法枚举", "GAIGATE_ROUTER_DEFAULT_STRATEGY", "random", "router.default_strategy"},
		{"meter.settle_queue_size 零值", "GAIGATE_METER_SETTLE_QUEUE_SIZE", "0", "meter.settle_queue_size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tt.env, tt.value)
			t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
			_, err := Load(missingPath(t))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

// TestLoad_BoundaryValid 边界合法值：端口 1/65535、错误率阈值上界 1。
func TestLoad_BoundaryValid(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	t.Setenv("GAIGATE_SERVER_ADDR", ":1")
	t.Setenv("GAIGATE_MYSQL_PORT", "65535")
	t.Setenv("GAIGATE_ROUTER_BREAKER_ERR_THRESHOLD", "1")
	cfg, err := Load(missingPath(t))
	require.NoError(t, err)
	assert.Equal(t, ":1", cfg.Server.Addr)
	assert.Equal(t, 65535, cfg.MySQL.Port)
	assert.Equal(t, 1.0, cfg.Router.BreakerErrThreshold)
}

// TestLoad_InvalidYAMLFile 配置文件语法错误须报错，不允许带病启动。
func TestLoad_InvalidYAMLFile(t *testing.T) {
	t.Setenv("GAIGATE_SECRET_MASTER_KEY", devMasterKey)
	_, err := Load(writeConfig(t, "server: [unclosed"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "读取配置文件")
}
