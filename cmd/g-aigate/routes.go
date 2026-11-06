package main

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"

	"gaigate/internal/access"
	"gaigate/internal/bootstrap"
	"gaigate/internal/channel"
	"gaigate/internal/config"
	"gaigate/internal/middleware"
	"gaigate/internal/provider/openai"
	"gaigate/internal/proxy"
	"gaigate/internal/router"
	"gaigate/internal/store"
)

func newGatewayRouter(logger *slog.Logger, probes *middleware.Probes) *gin.Engine {
	r := gin.New()
	r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())
	r.Use(func(c *gin.Context) {
		c.Writer = middleware.WrapWriter(c.Writer)
		c.Next()
	})
	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", probes.ReadyHandler())
	return r
}

func registerGatewayRoutes(r gin.IRouter, db *gorm.DB, masterKey []byte, cfg config.ProxyConfig,
	healthCfg config.ChannelConfig, routerCfg config.RouterConfig, client *redis.Client, logger *slog.Logger) (channel.HealthService, error) {
	cache := store.NewConfigCache(0) // Existing store default TTL and invalidation semantics.
	models := store.NewModelRepo(db, cache)
	channels := store.NewChannelRepo(db, cache)
	keys := store.NewKeyRepo(db)
	upstream, err := openai.New(&http.Client{}, cfg.StreamBuffer)
	if err != nil {
		return nil, err
	}
	businessPicker, err := channel.NewEndpointPicker(channels, keys, masterKey)
	if err != nil {
		return nil, err
	}
	picker, err := channel.NewProbePicker(channels, keys, masterKey)
	if err != nil {
		return nil, err
	}
	tester, err := channel.NewConnectivityTester(picker, upstream, healthCfg.ProbeTimeout)
	if err != nil {
		return nil, err
	}
	health, err := channel.NewHealthService(channels, keys, store.NewVersionedHealthState(client), tester, channel.HealthOptions{
		ProbeInterval: healthCfg.ProbeInterval, DisableDuration: healthCfg.DisableDuration,
		ProbeTimeout: healthCfg.ProbeTimeout, StateTimeout: healthCfg.StateTimeout,
		FailureThreshold: healthCfg.FailureThreshold, ProbeConcurrency: healthCfg.ProbeConcurrency,
	}, logger)
	if err != nil {
		return nil, err
	}
	strategies, err := router.NewStrategyEngine(store.NewPriceRepo(db, cache), logger)
	if err != nil {
		return nil, err
	}
	breaker, err := router.NewBreaker(client, router.BreakerOptions{Window: routerCfg.BreakerWindow, OpenDuration: routerCfg.BreakerOpenDuration,
		StateTimeout: routerCfg.StateTimeout, MinSamples: routerCfg.BreakerMinSamples, ErrorThreshold: routerCfg.BreakerErrThreshold}, logger)
	if err != nil {
		return nil, err
	}
	planner, err := router.NewRouter(models, channels, health, breaker, strategies, router.RouterOptions{DefaultStrategy: router.RouteStrategy(routerCfg.DefaultStrategy), RouteTimeout: routerCfg.RouteTimeout}, logger)
	if err != nil {
		return nil, err
	}
	execution, err := router.NewExecutor(businessPicker, upstream, proxy.NewForwarder(), breaker, health, strategies, router.ExecutorOptions{
		Forward: proxy.ForwardOptions{TTFBTimeout: cfg.TTFBTimeout, OverallTimeout: cfg.OverallTimeout}, StateTimeout: min(routerCfg.StateTimeout, healthCfg.StateTimeout),
		PreparationTimeout: routerCfg.RouteTimeout, MaxAttempts: routerCfg.MaxAttempts}, logger)
	if err != nil {
		return nil, err
	}
	executor, err := bootstrap.NewExecutor(planner, execution, cfg.OverallTimeout)
	if err != nil {
		return nil, err
	}
	userKeys, users := store.NewAPIKeyRepo(db), store.NewUserRepo(db)
	auth := access.AuthMiddleware(access.NewAuthenticator(userKeys, users))
	access.RegisterV1Routes(r, auth, bootstrap.AllowAll{}, executor, models)
	// TODO(19.1): replace Bearer with session/CSRF identity at assembly (台账#23).
	access.RegisterUserKeyRoutes(r, auth, access.NewKeyService(userKeys, users))
	admin, err := channel.NewAdminService(channels, keys, models, cache, masterKey)
	if err != nil {
		return nil, err
	}
	// TODO(19.1): replace all management Bearer routes with session/CSRF identity.
	isAdmin := func(ctx context.Context) bool {
		p, ok := access.PrincipalFromContext(ctx)
		return ok && p.Role == store.RoleAdmin
	}
	channel.RegisterAdminRoutes(r, auth, isAdmin, admin, tester)
	channel.RegisterHealthRoutes(r, auth, isAdmin, channels, health)
	return health, nil
}
