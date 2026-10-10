// g-aigate 是 G-AIGate 网关入口（10-骨架与装配）。
// 10.1：配置加载（fail-fast）+ 最小入口；10.2：中间件管道、
// 探针与优雅关闭。后续业务路由（13.3）挂在本管道之下。
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gin-gonic/gin"

	"gaigate/internal/config"
	"gaigate/internal/middleware"
)

// dependency 优雅关闭的依赖项（10.2 §3）：按装配顺序登记，关闭时
// 逆序执行。后续按序追加：11.3 落账 drain → redis → mysql——
// 落账队列必须先 drain 再关 DB，否则丢账（10.2 §4 决策②）。
type dependency struct {
	name  string
	close func(ctx context.Context) error
}

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
	r := gin.New()
	// 中间件管道（10.2 §3 契约，2026-10-10 修正）：
	// RequestID → AccessLog → Recover → 路由
	r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())

	// 探针（10-L1 §3 决策②）：/healthz liveness 恒 200；/readyz 由
	// Probes 动态注册检查项。B2 末 store（11.x）就绪后在此注册
	// MySQL/Redis ping（10.2 §4 决策①）；当前 0 检查项 → 200。
	probes := &middleware.Probes{}
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", probes.ReadyHandler())

	// 优雅关闭依赖列表：按装配顺序登记，逆序关闭；当前为空
	// （store 未建，11.x 交付后按序追加）
	deps := []dependency{}

	srv := &http.Server{Addr: cfg.Server.Addr, Handler: r}

	serveErr := make(chan error, 1)
	// 服务主循环 goroutine：非请求路径，01 §3 的请求路径裸 go 禁令不适用
	go func() { serveErr <- srv.ListenAndServe() }()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err := <-serveErr:
		// 未收到信号而退出：启动失败（如端口占用）
		logger.Error("HTTP 服务异常退出", "err", err.Error())
		os.Exit(1)
	case sig := <-quit:
		// 10.2 §3：SIGTERM/SIGINT → 停止接受新请求 → 等待在途请求完成
		logger.Info("收到退出信号，开始优雅关闭", "signal", sig.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancel()
	shutdown(ctx, srv, deps, logger)
}

// shutdown 执行优雅关闭序列（10.2 §3）：先 Shutdown 停止接受新请求
// 并等待在途请求（上限为 ctx 超时，流式长连接在此被截断——10-L1 §5
// 已知取舍），再逆序关闭依赖。抽出为独立函数以便单测逆序与超时语义。
func shutdown(ctx context.Context, srv *http.Server, deps []dependency, logger *slog.Logger) {
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("优雅关闭超时/出错，在途请求被截断", "err", err.Error())
	}
	// 逆序关闭：后装配的先关（落账 drain → redis → mysql）
	for i := len(deps) - 1; i >= 0; i-- {
		if err := deps[i].close(ctx); err != nil {
			logger.Error("依赖关闭失败", "name", deps[i].name, "err", err.Error())
		}
	}
	logger.Info("g-aigate 已退出")
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
