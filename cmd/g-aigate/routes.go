package main

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"gaigate/internal/access"
	"gaigate/internal/bootstrap"
	"gaigate/internal/channel"
	"gaigate/internal/config"
	"gaigate/internal/middleware"
	"gaigate/internal/provider/openai"
	"gaigate/internal/proxy"
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

func registerGatewayRoutes(r gin.IRouter, db *gorm.DB, masterKey []byte, cfg config.ProxyConfig, probeTimeout time.Duration, logger *slog.Logger) error {
	cache := store.NewConfigCache(0) // Existing store default TTL and invalidation semantics.
	models := store.NewModelRepo(db, cache)
	channels := store.NewChannelRepo(db, cache)
	keys := store.NewKeyRepo(db)
	upstream, err := openai.New(&http.Client{}, cfg.StreamBuffer)
	if err != nil {
		return err
	}
	businessPicker, err := channel.NewEndpointPicker(channels, keys, masterKey)
	if err != nil {
		return err
	}
	executor, err := bootstrap.NewExecutor(models, channels, businessPicker, upstream, proxy.NewForwarder(),
		proxy.ForwardOptions{TTFBTimeout: cfg.TTFBTimeout, OverallTimeout: cfg.OverallTimeout}, logger)
	if err != nil {
		return err
	}
	userKeys, users := store.NewAPIKeyRepo(db), store.NewUserRepo(db)
	auth := access.AuthMiddleware(access.NewAuthenticator(userKeys, users))
	access.RegisterV1Routes(r, auth, bootstrap.AllowAll{}, executor, models)
	// TODO(19.1): replace Bearer with session/CSRF identity at assembly (台账#23).
	access.RegisterUserKeyRoutes(r, auth, access.NewKeyService(userKeys, users))
	admin, err := channel.NewAdminService(channels, keys, models, cache, masterKey)
	if err != nil {
		return err
	}
	picker, err := channel.NewProbePicker(channels, keys, masterKey)
	if err != nil {
		return err
	}
	tester, err := channel.NewConnectivityTester(picker, upstream, probeTimeout)
	if err != nil {
		return err
	}
	// TODO(19.1): replace all management Bearer routes with session/CSRF identity.
	channel.RegisterAdminRoutes(r, auth, func(ctx context.Context) bool {
		p, ok := access.PrincipalFromContext(ctx)
		return ok && p.Role == store.RoleAdmin
	}, admin, tester)
	return nil
}
