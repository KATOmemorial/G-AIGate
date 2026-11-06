package bootstrap

import (
	"context"
	"gaigate/internal/access"
	"gaigate/internal/channel"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"
	"gaigate/internal/router"
	"gaigate/internal/store"
	"github.com/alicebob/miniredis/v2"
	"log/slog"
	"net/http"
	"testing"
	"time"
)

// Historical bootstrap assertions now exercise the real router/strategy/
// breaker/executor composition. One actual attempt is explicit for these
// regression cases; production configuration defaults to three.
func newComposedExecutor(t *testing.T, models store.ModelRepo, channels store.ChannelRepo, picker channel.EndpointPicker, upstream provider.Provider, forwarder proxy.Forwarder, opts proxy.ForwardOptions, health channel.HealthRegistry, state time.Duration, logger *slog.Logger) (access.ChatExecutor, error) {
	t.Helper()
	strategies, err := router.NewStrategyEngine(store.NewPriceRepo(nil, store.NewConfigCache(0)), logger)
	if err != nil {
		return nil, err
	}
	mr := miniredis.RunT(t)
	client := store.NewRedis(store.RedisOptions{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	breaker, err := router.NewBreaker(client, router.BreakerOptions{Window: time.Minute, OpenDuration: 30 * time.Second, StateTimeout: state, MinSamples: 20, ErrorThreshold: 0.5}, logger)
	if err != nil {
		return nil, err
	}
	planner, err := router.NewRouter(models, channels, health, breaker, strategies, router.RouterOptions{DefaultStrategy: router.LatencyFirst, RouteTimeout: time.Second}, logger)
	if err != nil {
		return nil, err
	}
	execution, err := router.NewExecutor(picker, upstream, forwarder, breaker, health, strategies, router.ExecutorOptions{Forward: opts, StateTimeout: state, PreparationTimeout: time.Second, MaxAttempts: 1}, logger)
	if err != nil {
		return nil, err
	}
	return NewExecutor(planner, execution, opts.OverallTimeout)
}

// These methods are used only for configuring fake stores in historical cases.
func validFixtureChannel(id uint64, priority int32) store.Channel {
	return store.Channel{ID: id, Name: "fixture", Priority: priority, Provider: "openai", BaseURL: "http://fixture.invalid/v1", Enabled: true, Weight: 1}
}
func fixtureEndpoint(c *store.Channel) *provider.Endpoint {
	return &provider.Endpoint{ChannelID: int64(c.ID), Provider: c.Provider, BaseURL: c.BaseURL, APIKey: "fixture-upstream-credential", Model: "mapped"}
}

type plannerFunc func(context.Context, string, router.RouteStrategy) (*router.Plan, error)

func (f plannerFunc) Route(ctx context.Context, m string, s router.RouteStrategy) (*router.Plan, error) {
	return f(ctx, m, s)
}

type executionFunc func(context.Context, http.ResponseWriter, *access.Principal, *router.Plan, *provider.ChatRequest) error

func (f executionFunc) Execute(ctx context.Context, w http.ResponseWriter, p *access.Principal, plan *router.Plan, r *provider.ChatRequest) error {
	return f(ctx, w, p, plan, r)
}
func testSafeKind(k proxy.ResultKind) string {
	switch k {
	case proxy.ResultSuccess, proxy.ResultTTFBTimeout, proxy.ResultOverallTimeout, proxy.ResultStreamInterrupted, proxy.ResultUpstreamError, proxy.ResultClientCanceled, proxy.ResultWriteError:
		return string(k)
	}
	return "unknown"
}
