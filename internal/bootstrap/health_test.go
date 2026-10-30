package bootstrap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"
	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

func TestExecutorHealthFilteringIsOrderedAndNeverFallback(t *testing.T) {
	for _, mode := range []string{"second_healthy", "all_unavailable", "picker_error", "upstream_error", "disabled_config"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t)
			var checked []int64
			h.health.available = func(_ context.Context, id int64) bool {
				checked = append(checked, id)
				return mode != "all_unavailable" && id != 1
			}
			var picked []int64
			h.picker = &endpointStub{pick: func(_ context.Context, ch *store.Channel, _ string) (*provider.Endpoint, error) {
				picked = append(picked, int64(ch.ID))
				if mode == "picker_error" {
					return nil, errors.New("owned picker failure")
				}
				return &provider.Endpoint{ChannelID: int64(ch.ID)}, nil
			}}
			if mode == "disabled_config" {
				h.channels.list = func(context.Context, string) ([]store.Channel, error) {
					return []store.Channel{{ID: 1, Enabled: false, Priority: 100}, {ID: 2, Enabled: true, Provider: "openai"}}, nil
				}
			}
			attempts, reports := 0, 0
			h.upstream.chat = func(context.Context, *provider.Endpoint, *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
				attempts++
				return nil, nil, nil
			}
			h.forwarder.chat = func(ctx context.Context, _ http.ResponseWriter, open proxy.ResponseOpener, _ proxy.ForwardOptions) (proxy.ForwardResult, error) {
				_, _, _ = open(ctx)
				if mode == "upstream_error" {
					return proxy.ForwardResult{Kind: proxy.ResultUpstreamError}, errors.New("owned upstream failure")
				}
				return proxy.ForwardResult{Kind: proxy.ResultSuccess}, nil
			}
			h.health.report = func(context.Context, int64, bool) { reports++ }
			err := h.build(t).Execute(context.Background(), httptest.NewRecorder(), nil, testRequest(false))
			switch mode {
			case "all_unavailable":
				require.ErrorIs(t, err, provider.ErrNoCandidate)
				require.Empty(t, picked)
				require.Zero(t, attempts)
				require.Zero(t, reports)
				require.Equal(t, []int64{1, 2, 9}, checked)
			case "picker_error":
				require.Error(t, err)
				require.Equal(t, []int64{2}, picked)
				require.Zero(t, attempts)
				require.Zero(t, reports)
			case "upstream_error":
				require.Error(t, err)
				require.Equal(t, []int64{2}, picked)
				require.Equal(t, 1, attempts)
				require.Equal(t, 1, reports)
			default:
				require.NoError(t, err)
				require.Equal(t, []int64{2}, picked)
				require.Equal(t, 1, attempts)
				require.Equal(t, 1, reports)
			}
			if mode == "disabled_config" {
				require.Equal(t, []int64{2}, checked)
			} else if mode != "all_unavailable" {
				require.Equal(t, []int64{1, 2}, checked)
			}
		})
	}
}

func TestExecutorReportsEachIssuedAttemptByKindBeforeCause(t *testing.T) {
	for _, kind := range []proxy.ResultKind{proxy.ResultSuccess, proxy.ResultStreamInterrupted, proxy.ResultTTFBTimeout, proxy.ResultOverallTimeout, proxy.ResultUpstreamError, proxy.ResultClientCanceled, proxy.ResultWriteError} {
		for _, status := range []int{0, 400, 404, 422, 401, 403, 429, 500, 503, 302} {
			t.Run(string(kind)+"/"+http.StatusText(status), func(t *testing.T) {
				for _, stream := range []bool{false, true} {
					h := newHarness(t)
					h.upstream.chat = func(context.Context, *provider.Endpoint, *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
						return nil, nil, nil
					}
					h.upstream.stream = func(context.Context, *provider.Endpoint, *provider.ChatRequest) (provider.Stream, error) {
						return nil, nil
					}
					cause := errors.Join(&provider.UpstreamError{StatusCode: status}, context.Canceled)
					parent, cancel := context.WithCancel(middleware.WithRequestID(context.Background(), "req-health-kind"))
					defer cancel()
					h.forwarder.chat = func(ctx context.Context, _ http.ResponseWriter, open proxy.ResponseOpener, _ proxy.ForwardOptions) (proxy.ForwardResult, error) {
						_, _, _ = open(ctx)
						cancel()
						return proxy.ForwardResult{Kind: kind}, cause
					}
					h.forwarder.stream = func(ctx context.Context, _ http.ResponseWriter, open proxy.StreamOpener, _ proxy.ForwardOptions, _ func(provider.StreamEvent)) (proxy.ForwardResult, error) {
						_, _ = open(ctx)
						cancel()
						return proxy.ForwardResult{Kind: kind}, cause
					}
					reports := 0
					h.health.report = func(ctx context.Context, id int64, ok bool) {
						reports++
						require.NoError(t, ctx.Err())
						require.Equal(t, int64(1), id)
						requestID, _ := middleware.RequestIDFromContext(ctx)
						require.Equal(t, "req-health-kind", requestID)
						deadline, exists := ctx.Deadline()
						require.True(t, exists)
						require.InDelta(t, 500*time.Millisecond, time.Until(deadline), float64(100*time.Millisecond))
						require.Equal(t, kind == proxy.ResultSuccess, ok)
					}
					_ = h.build(t).Execute(parent, httptest.NewRecorder(), nil, testRequest(stream))
					want := kind == proxy.ResultSuccess || kind == proxy.ResultStreamInterrupted || kind == proxy.ResultTTFBTimeout || kind == proxy.ResultOverallTimeout || kind == proxy.ResultUpstreamError && (status != 400 && status != 404 && status != 422)
					if want {
						require.Equal(t, 1, reports)
					} else {
						require.Zero(t, reports)
					}
				}
			})
		}
	}
}

func TestExecutorNoSignalBeforeOpenerAndReportHasIndependentBound(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t)
		h.picker = &endpointStub{pick: func(context.Context, *store.Channel, string) (*provider.Endpoint, error) {
			return &provider.Endpoint{ChannelID: 1}, nil
		}}
		reports := 0
		h.health.report = func(ctx context.Context, _ int64, _ bool) { reports++; <-ctx.Done() }
		// A forwarder can reject before issuing its opener (for example canceled
		// input or unsupported writer); even a timeout kind is not an attempt.
		h.forwarder.chat = func(context.Context, http.ResponseWriter, proxy.ResponseOpener, proxy.ForwardOptions) (proxy.ForwardResult, error) {
			return proxy.ForwardResult{Kind: proxy.ResultOverallTimeout}, context.DeadlineExceeded
		}
		_ = h.build(t).Execute(context.Background(), httptest.NewRecorder(), nil, testRequest(false))
		require.Zero(t, reports)
		h.upstream.chat = func(context.Context, *provider.Endpoint, *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
			return nil, nil, nil
		}
		h.forwarder.chat = func(ctx context.Context, _ http.ResponseWriter, open proxy.ResponseOpener, _ proxy.ForwardOptions) (proxy.ForwardResult, error) {
			_, _, _ = open(ctx)
			return proxy.ForwardResult{Kind: proxy.ResultOverallTimeout}, context.DeadlineExceeded
		}
		executor, err := NewExecutor(h.models, h.channels, h.picker, h.upstream, h.forwarder, h.opts, h.health, 20*time.Millisecond, slog.New(slog.NewJSONHandler(io.Discard, nil)))
		require.NoError(t, err)
		start := time.Now()
		_ = executor.Execute(context.Background(), httptest.NewRecorder(), nil, testRequest(false))
		require.Equal(t, 20*time.Millisecond, time.Since(start))
		require.Equal(t, 1, reports)
	})
}
