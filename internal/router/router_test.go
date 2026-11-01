package router

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/access"
	"gaigate/internal/channel"
	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

type orderFixture func(context.Context, string, RouteStrategy, []Candidate) ([]Candidate, error)

func (f orderFixture) Order(ctx context.Context, model string, strategy RouteStrategy, candidates []Candidate) ([]Candidate, error) {
	return f(ctx, model, strategy, candidates)
}

func identityOrder(_ context.Context, _ string, _ RouteStrategy, candidates []Candidate) ([]Candidate, error) {
	return candidates, nil
}
func testLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }
func fixtureRouter(t *testing.T, f *fixture, order Orderer, timeout time.Duration) Router {
	t.Helper()
	r, err := NewRouter(f.models, f.channels, f.health, f.breaker, order, RouterOptions{DefaultStrategy: Weighted, RouteTimeout: timeout}, testLogger())
	require.NoError(t, err)
	return r
}

func TestRouterConstructorValidatesDependenciesAndOptionsWithoutIO(t *testing.T) {
	f := newFixture(t, Weighted, 1)
	var nilModel *modelFixture
	var nilChannels *channelFixture
	var nilHealth *healthFixture
	var nilInspector *inspectorFixture
	var nilOrder orderFixture
	for _, tc := range []struct {
		name     string
		models   store.ModelRepo
		channels store.ChannelRepo
		health   channel.HealthRegistry
		breaker  BreakerInspector
		order    Orderer
		logger   *slog.Logger
	}{
		{"models nil", nil, f.channels, f.health, f.breaker, orderFixture(identityOrder), testLogger()},
		{"models typed nil", nilModel, f.channels, f.health, f.breaker, orderFixture(identityOrder), testLogger()},
		{"channels nil", f.models, nil, f.health, f.breaker, orderFixture(identityOrder), testLogger()},
		{"channels typed nil", f.models, nilChannels, f.health, f.breaker, orderFixture(identityOrder), testLogger()},
		{"health nil", f.models, f.channels, nil, f.breaker, orderFixture(identityOrder), testLogger()},
		{"health typed nil", f.models, f.channels, nilHealth, f.breaker, orderFixture(identityOrder), testLogger()},
		{"inspector nil", f.models, f.channels, f.health, nil, orderFixture(identityOrder), testLogger()},
		{"inspector typed nil", f.models, f.channels, f.health, nilInspector, orderFixture(identityOrder), testLogger()},
		{"orderer nil", f.models, f.channels, f.health, f.breaker, nil, testLogger()},
		{"orderer typed nil func", f.models, f.channels, f.health, f.breaker, nilOrder, testLogger()},
		{"logger nil", f.models, f.channels, f.health, f.breaker, orderFixture(identityOrder), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := NewRouter(tc.models, tc.channels, tc.health, tc.breaker, tc.order, RouterOptions{DefaultStrategy: Weighted}, tc.logger)
			require.Nil(t, r)
			require.Error(t, err)
		})
	}
	for _, timeout := range []time.Duration{-time.Millisecond, time.Nanosecond, 999 * time.Microsecond, 1001 * time.Microsecond, time.Minute + time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			r, err := NewRouter(f.models, f.channels, f.health, f.breaker, orderFixture(identityOrder), RouterOptions{DefaultStrategy: Weighted, RouteTimeout: timeout}, testLogger())
			require.Nil(t, r)
			require.Error(t, err)
		})
	}
	for _, strategy := range []RouteStrategy{Auto, "unsupported"} {
		r, err := NewRouter(f.models, f.channels, f.health, f.breaker, orderFixture(identityOrder), RouterOptions{DefaultStrategy: strategy}, testLogger())
		require.Nil(t, r)
		require.Error(t, err)
	}
	for _, timeout := range []time.Duration{0, time.Millisecond, time.Minute} {
		r := fixtureRouter(t, f, orderFixture(identityOrder), timeout)
		want := timeout
		if want == 0 {
			want = 2 * time.Second
		}
		require.Equal(t, want, r.(*modelRouter).timeout)
	}
	require.Zero(t, f.resolveCalls.Load())
	require.Zero(t, f.listCalls.Load())
	require.Zero(t, f.getCalls.Load())
	require.Zero(t, f.mappingCalls.Load())
	require.Zero(t, f.healthCalls.Load())
	require.Zero(t, f.inspectCalls.Load())
}

func TestRouterOrderingOnceAfterFilteringAndNoOrderOnEmptyOrError(t *testing.T) {
	for _, tc := range []string{"success", "empty", "alias error", "inspection error"} {
		t.Run(tc, func(t *testing.T) {
			f := newFixture(t, Weighted, 3)
			f.health.available = func(_ context.Context, id int64) bool { return tc != "empty" && id != 2 }
			original := errors.New("private infrastructure details")
			if tc == "alias error" {
				f.models.resolve = func(context.Context, string) (*store.ModelAlias, error) { return nil, original }
			}
			if tc == "inspection error" {
				f.breaker.inspect = func(context.Context, int64) (BreakerView, error) { return BreakerView{}, original }
			}
			calls := 0
			r := fixtureRouter(t, f, orderFixture(func(_ context.Context, model string, strategy RouteStrategy, c []Candidate) ([]Candidate, error) {
				calls++
				require.Equal(t, "public", model)
				require.Equal(t, Weighted, strategy)
				require.Equal(t, []uint64{1, 3}, candidateIDs(&Plan{Candidates: c}))
				slices.Reverse(c)
				return c, nil
			}), 0)
			p, err := r.Route(t.Context(), "public", Auto)
			if tc == "success" {
				require.NoError(t, err)
				require.Equal(t, []uint64{3, 1}, candidateIDs(p))
				require.Equal(t, 1, calls)
			} else {
				require.Nil(t, p)
				require.Zero(t, calls)
				if tc == "empty" {
					require.ErrorIs(t, err, provider.ErrNoCandidate)
				} else {
					require.ErrorIs(t, err, original)
				}
			}
		})
	}
	t.Run("order error is preserved and safe", func(t *testing.T) {
		f := newFixture(t, Weighted, 1)
		original := errors.New("https://credential@private")
		r := fixtureRouter(t, f, orderFixture(func(context.Context, string, RouteStrategy, []Candidate) ([]Candidate, error) { return nil, original }), 0)
		p, err := r.Route(t.Context(), "public", Auto)
		requireConfigError(t, p, err)
		require.ErrorIs(t, err, original)
		require.NotContains(t, err.Error(), original.Error())
	})
}

func TestRouterRejectsIncompleteOrModifiedPermutations(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]Candidate) []Candidate
	}{
		{"nil", func([]Candidate) []Candidate { return nil }}, {"missing", func(c []Candidate) []Candidate { return c[:1] }},
		{"extra", func(c []Candidate) []Candidate { return append(c, c[0]) }}, {"duplicate", func(c []Candidate) []Candidate { c[1] = c[0]; return c }},
		{"unknown ID", func(c []Candidate) []Candidate { c[0].Channel.ID = 99; return c }},
		{"mapping", func(c []Candidate) []Candidate { c[0].UpstreamModel = "changed"; return c }},
		{"name", func(c []Candidate) []Candidate { c[0].Channel.Name = "changed"; return c }},
		{"provider", func(c []Candidate) []Candidate { c[0].Channel.Provider = "mock"; return c }},
		{"URL", func(c []Candidate) []Candidate { c[0].Channel.BaseURL = "https://changed.invalid"; return c }},
		{"priority", func(c []Candidate) []Candidate { c[0].Channel.Priority++; return c }},
		{"weight", func(c []Candidate) []Candidate { c[0].Channel.Weight++; return c }},
		{"enabled", func(c []Candidate) []Candidate { c[0].Channel.Enabled = false; return c }},
		{"health display", func(c []Candidate) []Candidate { c[0].Channel.HealthStatus = "changed"; return c }},
		{"error display", func(c []Candidate) []Candidate { c[0].Channel.ErrorRateCache++; return c }},
		{"latency display", func(c []Candidate) []Candidate { c[0].Channel.LatencyP99++; return c }},
		{"checked pointer", func(c []Candidate) []Candidate {
			*c[0].Channel.LastCheckedAt = c[0].Channel.LastCheckedAt.Add(time.Hour)
			return c
		}},
		{"checked nil", func(c []Candidate) []Candidate { c[0].Channel.LastCheckedAt = nil; return c }},
		{"created", func(c []Candidate) []Candidate {
			c[0].Channel.CreatedAt = c[0].Channel.CreatedAt.Add(time.Hour)
			return c
		}},
		{"updated", func(c []Candidate) []Candidate {
			c[0].Channel.UpdatedAt = c[0].Channel.UpdatedAt.Add(time.Hour)
			return c
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, Weighted, 2)
			original := cloneChannel(f.rows[0])
			r := fixtureRouter(t, f, orderFixture(func(_ context.Context, _ string, _ RouteStrategy, c []Candidate) ([]Candidate, error) {
				return tc.mutate(c), nil
			}), 0)
			p, err := r.Route(t.Context(), "public", Auto)
			requireConfigError(t, p, err)
			require.Equal(t, original, f.rows[0])
		})
	}
}

func TestRouterOrdererInputOutputAndOptionsAreIsolated(t *testing.T) {
	f := newFixture(t, Auto, 2)
	var retainedInput, retainedOutput []Candidate
	opts := RouterOptions{DefaultStrategy: CostFirst, RouteTimeout: 41 * time.Millisecond}
	r, err := NewRouter(f.models, f.channels, f.health, f.breaker, orderFixture(func(_ context.Context, _ string, strategy RouteStrategy, c []Candidate) ([]Candidate, error) {
		require.Equal(t, CostFirst, strategy)
		retainedInput = c
		slices.Reverse(c)
		retainedOutput = cloneCandidates(c)
		return retainedOutput, nil
	}), opts, testLogger())
	require.NoError(t, err)
	opts.DefaultStrategy = FallbackChain
	opts.RouteTimeout = time.Minute
	p, err := r.Route(t.Context(), "public", Auto)
	require.NoError(t, err)
	require.Equal(t, CostFirst, p.Strategy)
	require.Equal(t, 41*time.Millisecond, r.(*modelRouter).timeout)
	require.Equal(t, []uint64{2, 1}, candidateIDs(p))
	original := *p.Candidates[0].Channel.LastCheckedAt
	retainedInput[0].Channel.Name = "input changed"
	*retainedInput[0].Channel.LastCheckedAt = original.Add(time.Hour)
	retainedOutput[0].UpstreamModel = "output changed"
	*retainedOutput[0].Channel.LastCheckedAt = original.Add(2 * time.Hour)
	require.Equal(t, "channel-2", p.Candidates[0].Channel.Name)
	require.Equal(t, "upstream-2", p.Candidates[0].UpstreamModel)
	require.Equal(t, original, *p.Candidates[0].Channel.LastCheckedAt)
	*p.Candidates[0].Channel.LastCheckedAt = original.Add(3 * time.Hour)
	p.Candidates[0].Channel.Name = "plan changed"
	require.Equal(t, "channel-2", f.rows[1].Name)
	require.Equal(t, original, *f.rows[1].LastCheckedAt)
	second, err := r.Route(t.Context(), "public", Auto)
	require.NoError(t, err)
	require.Equal(t, "channel-2", second.Candidates[0].Channel.Name)
	require.Equal(t, original, *second.Candidates[0].Channel.LastCheckedAt)
}

func TestRouterConcurrentOrdererSnapshots(t *testing.T) {
	f := newFixture(t, Weighted, 2)
	var calls atomic.Int64
	r := fixtureRouter(t, f, orderFixture(func(_ context.Context, _ string, _ RouteStrategy, c []Candidate) ([]Candidate, error) {
		calls.Add(1)
		slices.Reverse(c)
		return c, nil
	}), 0)
	const count = 32
	start := make(chan struct{})
	results := make(chan *Plan, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, err := r.Route(t.Context(), "public", Auto)
			if err != nil {
				errs <- err
				return
			}
			results <- p
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, results, count)
	require.EqualValues(t, count, calls.Load())
	for p := range results {
		require.Equal(t, []uint64{2, 1}, candidateIDs(p))
		require.Equal(t, "channel-2", p.Candidates[0].Channel.Name)
		p.Candidates[0].Channel.Name = "private"
		*p.Candidates[0].Channel.LastCheckedAt = time.Time{}
	}
	require.Equal(t, "channel-2", f.rows[1].Name)
	require.False(t, f.rows[1].LastCheckedAt.IsZero())
}

func TestRouterBudgetCoversEverySynchronousStage(t *testing.T) {
	for _, stage := range []string{"resolve", "list", "get", "mapping", "health", "inspect", "order"} {
		for _, earlier := range []bool{false, true} {
			t.Run(stage+map[bool]string{false: "/local", true: "/parent"}[earlier], func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					f := newFixture(t, Weighted, 2)
					entered := make(chan struct{})
					exited := make(chan struct{})
					cause := errors.New("earlier parent deadline")
					parent := t.Context()
					var cancel context.CancelFunc
					if earlier {
						parent, cancel = context.WithTimeoutCause(parent, 7*time.Millisecond, cause)
						defer cancel()
					}
					stall := func(ctx context.Context) { close(entered); <-ctx.Done(); close(exited) }
					switch stage {
					case "resolve":
						f.models.resolve = func(ctx context.Context, _ string) (*store.ModelAlias, error) { stall(ctx); return nil, ctx.Err() }
					case "list":
						f.channels.list = func(ctx context.Context, _ string) ([]store.Channel, error) { stall(ctx); return nil, ctx.Err() }
					case "get":
						f.alias.Strategy = string(FallbackChain)
						f.alias.FallbackIDs = "[1,2]"
						f.channels.get = func(ctx context.Context, _ int64) (*store.Channel, error) {
							f.getCalls.Add(1)
							stall(ctx)
							return nil, ctx.Err()
						}
					case "mapping":
						f.channels.mapping = func(ctx context.Context, _ int64, _ string) (*store.ChannelModel, error) {
							f.mappingCalls.Add(1)
							stall(ctx)
							return nil, ctx.Err()
						}
					case "health":
						f.health.available = func(ctx context.Context, _ int64) bool { f.healthCalls.Add(1); stall(ctx); return false }
					case "inspect":
						f.breaker.inspect = func(ctx context.Context, _ int64) (BreakerView, error) {
							f.inspectCalls.Add(1)
							stall(ctx)
							return BreakerView{}, ctx.Err()
						}
					}
					var orderCalls atomic.Int64
					r := fixtureRouter(t, f, orderFixture(func(ctx context.Context, _ string, _ RouteStrategy, c []Candidate) ([]Candidate, error) {
						orderCalls.Add(1)
						if stage == "order" {
							stall(ctx)
							return nil, ctx.Err()
						}
						return c, nil
					}), 25*time.Millisecond)
					start := time.Now()
					done := make(chan error, 1)
					go func() {
						p, err := r.Route(parent, "public", Auto)
						if p != nil {
							done <- errors.New("unexpected plan")
							return
						}
						done <- err
					}()
					<-entered
					err := <-done
					synctest.Wait()
					select {
					case <-exited:
					default:
						t.Fatal("Route returned before synchronous dependency exited")
					}
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.NotErrorIs(t, err, access.ErrModelNotFound)
					require.NotErrorIs(t, err, provider.ErrNoCandidate)
					want := 25 * time.Millisecond
					if earlier {
						want = 7 * time.Millisecond
						require.ErrorIs(t, err, cause)
					}
					require.Equal(t, want, time.Since(start))
					if stage != "order" {
						require.Zero(t, orderCalls.Load())
					}
					if stage == "get" {
						require.EqualValues(t, 1, f.getCalls.Load())
					}
					if stage == "mapping" || stage == "health" || stage == "inspect" {
						require.EqualValues(t, 1, f.mappingCalls.Load())
					}
				})
			})
		}
	}
}

func TestRouterOneDeadlineAndDecreasingRemainingBudget(t *testing.T) {
	for _, strategy := range []RouteStrategy{Weighted, FallbackChain} {
		t.Run(string(strategy), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newFixture(t, strategy, 1)
				f.alias.FallbackIDs = "[1]"
				type event struct {
					stage string
					ctx   context.Context
				}
				events := make(chan event)
				resume := make(chan struct{})
				done := make(chan error, 1)
				checkpoint := func(ctx context.Context, stage string) { events <- event{stage, ctx}; <-resume }
				resolve := f.models.resolve
				f.models.resolve = func(ctx context.Context, name string) (*store.ModelAlias, error) {
					checkpoint(ctx, "resolve")
					return resolve(ctx, name)
				}
				list := f.channels.list
				f.channels.list = func(ctx context.Context, name string) ([]store.Channel, error) {
					checkpoint(ctx, "list")
					return list(ctx, name)
				}
				get := f.channels.get
				f.channels.get = func(ctx context.Context, id int64) (*store.Channel, error) {
					checkpoint(ctx, "get")
					return get(ctx, id)
				}
				mapping := f.channels.mapping
				f.channels.mapping = func(ctx context.Context, id int64, name string) (*store.ChannelModel, error) {
					checkpoint(ctx, "mapping")
					return mapping(ctx, id, name)
				}
				f.health.available = func(ctx context.Context, _ int64) bool { checkpoint(ctx, "health"); return true }
				f.breaker.inspect = func(ctx context.Context, _ int64) (BreakerView, error) {
					checkpoint(ctx, "inspect")
					return BreakerView{BreakerClosed, true}, nil
				}
				r := fixtureRouter(t, f, orderFixture(func(ctx context.Context, _ string, _ RouteStrategy, c []Candidate) ([]Candidate, error) {
					checkpoint(ctx, "order")
					return c, nil
				}), 0)
				start := time.Now()
				go func() { _, err := r.Route(t.Context(), "public", Auto); done <- err }()
				var first context.Context
				var deadline time.Time
				wantStages := []string{"resolve", "list", "mapping", "health", "inspect", "order"}
				if strategy == FallbackChain {
					wantStages[1] = "get"
				}
				for i, want := range wantStages {
					e := <-events
					require.Equal(t, want, e.stage)
					d, ok := e.ctx.Deadline()
					require.True(t, ok)
					if i == 0 {
						first = e.ctx
						deadline = d
						require.Equal(t, 2*time.Second, d.Sub(start))
					} else {
						require.Equal(t, first, e.ctx)
						require.Equal(t, deadline, d)
					}
					require.Equal(t, 2*time.Second-time.Duration(i)*10*time.Millisecond, time.Until(d))
					timer := time.NewTimer(10 * time.Millisecond)
					<-timer.C
					resume <- struct{}{}
				}
				require.NoError(t, <-done)
				synctest.Wait()
				require.Equal(t, 60*time.Millisecond, time.Since(start))
			})
		})
	}
}

func TestRouterCancellationCauseStopsOrderingAndLogsSafeStage(t *testing.T) {
	f := newFixture(t, Weighted, 1)
	ctx, cancel := context.WithCancelCause(t.Context())
	cause := errors.New("secret private URL")
	cancel(cause)
	calls := 0
	r := fixtureRouter(t, f, orderFixture(func(context.Context, string, RouteStrategy, []Candidate) ([]Candidate, error) {
		calls++
		return nil, nil
	}), 0)
	p, err := r.Route(ctx, "public", Auto)
	require.Nil(t, p)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, err, cause)
	require.Zero(t, calls)
	require.Zero(t, f.resolveCalls.Load())
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			a.Key = "ts"
		}
		return a
	}}))
	f.models.resolve = func(context.Context, string) (*store.ModelAlias, error) { return nil, cause }
	r, err = NewRouter(f.models, f.channels, f.health, f.breaker, orderFixture(identityOrder), RouterOptions{DefaultStrategy: Weighted}, logger)
	require.NoError(t, err)
	_, err = r.Route(middleware.WithRequestID(t.Context(), "request-router-test"), "public", Auto)
	require.ErrorIs(t, err, cause)
	require.Contains(t, logs.String(), `"request_id":"request-router-test"`)
	require.Contains(t, logs.String(), `"stage":"alias lookup failed"`)
	require.Contains(t, logs.String(), `"ts":`)
	require.NotContains(t, logs.String(), cause.Error())
	require.NotContains(t, logs.String(), f.rows[0].BaseURL)
}
