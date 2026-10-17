// g-aigate 是 G-AIGate 网关入口（10-骨架与装配）。
// 10.1：配置加载（fail-fast）+ 最小入口；10.2：中间件管道、
// 探针与优雅关闭。后续业务路由（13.3）挂在本管道之下。
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"gaigate/internal/config"
	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

// dependency 优雅关闭的依赖项（10.2 §3）：按装配顺序登记，关闭时
// 逆序执行（装配顺序见 buildDeps）。落账队列（11.3）必须先 drain
// 再关 DB，否则丢账（10.2 §4 决策②）。
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

	logger := newLogger(os.Stdout, cfg.Log.Level)
	slog.SetDefault(logger)

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	// 中间件管道（10.2 §3 契约，2026-10-10 修正）：
	// RequestID → AccessLog → Recover → 路由
	r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())

	// 探针（10-L1 §3 决策②）：/healthz liveness 恒 200；/readyz 由
	// Probes 动态注册检查项（10.2 §4 决策①）。
	probes := &middleware.Probes{}
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", probes.ReadyHandler())

	// store 装配（11.1 §2）：启动迁移（GET_LOCK 防多副本并发）→
	// GORM 客户端 → 幂等 seed。任何一步失败均 fail-fast 退出（exit 1）。
	storeOpts := store.Options{
		Host:     cfg.MySQL.Host,
		Port:     cfg.MySQL.Port,
		User:     cfg.MySQL.User,
		Password: cfg.MySQL.Password,
		Database: cfg.MySQL.Database,
	}
	if err := store.RunMigrations(storeOpts, "migrations"); err != nil {
		logger.Error("数据库迁移失败", "err", err.Error())
		os.Exit(1)
	}
	db, err := store.Open(storeOpts)
	if err != nil {
		logger.Error("MySQL 连接失败", "err", err.Error())
		os.Exit(1)
	}
	sqlDB, err := db.DB()
	if err != nil {
		logger.Error("获取 MySQL 底层连接失败", "err", err.Error())
		os.Exit(1)
	}
	// config 已校验 MasterKey 为 base64 且解码后 32 字节；此处解码失败
	// 理论不可达，防御性退出
	masterKey, err := base64.StdEncoding.DecodeString(cfg.Secret.MasterKey)
	if err != nil {
		logger.Error("master key 解码失败", "err", err.Error())
		os.Exit(1)
	}
	if err := store.Seed(db, store.SeedOptions{
		// GAIGATE_ADMIN_PASSWORD 未设置时 seed 内回退 admin123（仅 dev，11.1 §3）
		AdminPassword: os.Getenv("GAIGATE_ADMIN_PASSWORD"),
		MasterKey:     masterKey,
	}); err != nil {
		logger.Error("seed 失败", "err", err.Error())
		os.Exit(1)
	}

	// readyz 注册 MySQL ping（10.2 注释要求 check 自带超时，防挂起拖住 /readyz）
	probes.Register("mysql", func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return sqlDB.PingContext(ctx)
	})

	// 落账队列装配（11.3 §3）：Record 供 meter（17）经内存队列异步落库，
	// 队列容量取 MeterConfig.SettleQueueSize（默认 1024）。worker 随装配启动；
	// Close 由关闭序列第一步 drain（见 buildDeps）。
	usageRepo := store.NewUsageRepo(db, cfg.Meter.SettleQueueSize)

	// 优雅关闭依赖列表：按装配顺序登记，逆序关闭（10.2 §3）。
	deps := buildDeps(
		func(ctx context.Context) error { return sqlDB.Close() },
		usageRepo.Close,
	)

	srv := &http.Server{Addr: cfg.Server.Addr, Handler: r}

	logger.Info("g-aigate 启动", "addr", cfg.Server.Addr)

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

	shutdown(cfg.Server.ShutdownTimeout, srv, deps, logger)
}

// buildDeps 按装配顺序登记优雅关闭依赖（10.2 §3：登记后逆序关闭）。
// 顺序契约：mysql 先登记、落账 drain（usage-repo）后登记 → 关闭时
// usage-repo 先于 mysql 执行，保证先 drain 落账队列再关 DB 连接，否则
// 队列中未落库记录丢失（10.2 §4 决策② / 11.3 §3）。
// 11.4 redis 落地时须注册在 mysql 与 usage-repo 之间（非追加到末尾），
// 以维持 10.2 §3.2 的关闭顺序 drain → redis → mysql。
// 抽出为独立函数以便单测关闭顺序（同 10.2 §9 对 shutdown 的处置）。
func buildDeps(mysqlClose, usageDrainClose func(context.Context) error) []dependency {
	return []dependency{
		{name: "mysql", close: mysqlClose},
		{name: "usage-repo", close: usageDrainClose},
	}
}

// shutdown 执行优雅关闭序列（10.2 §3）：先 Shutdown 停止接受新请求
// 并等待在途请求（上限 timeout，流式长连接在此被截断——10-L1 §5
// 已知取舍），再逆序关闭依赖。依赖关闭阶段使用独立的全额超时预算：
// Shutdown 超时耗尽后若复用同一 ctx，落账 drain（11.3）会拿到已取消
// context 即刻失败，与"先 drain 再关 DB"（10.2 §4 决策②）冲突。
// 抽出为独立函数以便单测逆序与超时语义。
func shutdown(timeout time.Duration, srv *http.Server, deps []dependency, logger *slog.Logger) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("优雅关闭超时/出错，在途请求被截断", "err", err.Error())
	}
	// 逆序关闭：后装配的先关（落账 drain → redis → mysql）；
	// 独立预算，不受 Shutdown 阶段是否超时影响
	depCtx, depCancel := context.WithTimeout(context.Background(), timeout)
	defer depCancel()
	for i := len(deps) - 1; i >= 0; i-- {
		if err := deps[i].close(depCtx); err != nil {
			logger.Error("依赖关闭失败", "name", deps[i].name, "err", err.Error())
		}
	}
	logger.Info("g-aigate 已退出")
}

// newLogger 按 config.log.level 构造结构化 JSON 日志器（00 §8.3 / 01 §4）。
// 00 §8.3 冻结的时间键为 ts（slog 默认为 time），经 ReplaceAttr 改写。
func newLogger(w io.Writer, level string) *slog.Logger {
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
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: lv,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				a.Key = "ts"
			}
			return a
		},
	}))
}
