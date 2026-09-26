// g-aigate 是 G-AIGate 网关入口（10-骨架与装配）。
// 10.1 阶段仅完成：配置加载（fail-fast）+ Gin 骨架 + /healthz 占位。
// 中间件管道（RequestID/recover/访问日志）与优雅关闭属 10.2，本阶段不做。
package main

import (
	"flag"
	"log/slog"
	"os"

	"github.com/gin-gonic/gin"

	"gaigate/internal/config"
)

func main() {
	configPath := flag.String("config", "config.yaml", "配置文件路径（文件不存在时使用默认值+环境变量）")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		// fail-fast：非法配置启动即退，exit code 1 + 结构化错误信息（10.1 §4 决策②）
		slog.New(slog.NewJSONHandler(os.Stderr, nil)).Error(
			"配置加载失败", "err", err.Error())
		os.Exit(1)
	}

	logger := newLogger(cfg.Log.Level)
	slog.SetDefault(logger)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New() // 中间件管道（RequestID/recover/访问日志）由 10.2 挂载
	r.GET("/healthz", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	logger.Info("g-aigate 启动", "addr", cfg.Server.Addr)
	if err := r.Run(cfg.Server.Addr); err != nil {
		logger.Error("HTTP 服务退出", "err", err.Error())
		os.Exit(1)
	}
}

// newLogger 按 config.log.level 构造结构化 JSON 日志器（00 §8.3 / 01 §4）。
func newLogger(level string) *slog.Logger {
	var lv slog.Level
	switch level {
	case "debug":
		lv = slog.LevelDebug
	case "warn":
		lv = slog.LevelWarn
	case "error":
		lv = slog.LevelError
	default:
		lv = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lv}))
}
