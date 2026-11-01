package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/access"
	"gaigate/internal/provider"
	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

// These are behavior fixtures only, not production ordering or breaker policies.
// Embedded nil interfaces make unexpected repository methods fail immediately.
type modelFixture struct {
	store.ModelRepo
	resolve func(context.Context, string) (*store.ModelAlias, error)
}

func (f modelFixture) Resolve(ctx context.Context, name string) (*store.ModelAlias, error) {
	return f.resolve(ctx, name)
}

type channelFixture struct {
	store.ChannelRepo
	get     func(context.Context, int64) (*store.Channel, error)
	list    func(context.Context, string) ([]store.Channel, error)
	mapping func(context.Context, int64, string) (*store.ChannelModel, error)
}

func (f channelFixture) Get(ctx context.Context, id int64) (*store.Channel, error) {
	return f.get(ctx, id)
}
func (f channelFixture) ListActiveByModel(ctx context.Context, name string) ([]store.Channel, error) {
	return f.list(ctx, name)
}
func (f channelFixture) GetModel(ctx context.Context, id int64, name string) (*store.ChannelModel, error) {
	return f.mapping(ctx, id, name)
}

type healthFixture struct {
	available func(context.Context, int64) bool
	reports   atomic.Int64
}

func (f *healthFixture) IsAvailable(ctx context.Context, id int64) bool   { return f.available(ctx, id) }
func (f *healthFixture) ReportRequestResult(context.Context, int64, bool) { f.reports.Add(1) }

type inspectorFixture struct {
	inspect             func(context.Context, int64) (BreakerView, error)
	acquires, completes atomic.Int64
}

func (f *inspectorFixture) Inspect(ctx context.Context, id int64) (BreakerView, error) {
	return f.inspect(ctx, id)
}

// Tripwires intentionally do not declare 16.3's future Permit/Outcome types.
func (f *inspectorFixture) Acquire()  { f.acquires.Add(1) }
func (f *inspectorFixture) Complete() { f.completes.Add(1) }

type fixture struct {
	r                                                                          *resolver
	alias                                                                      *store.ModelAlias
	rows                                                                       []store.Channel
	models                                                                     *modelFixture
	channels                                                                   *channelFixture
	health                                                                     *healthFixture
	breaker                                                                    *inspectorFixture
	resolveCalls, listCalls, getCalls, mappingCalls, healthCalls, inspectCalls atomic.Int64
	mu                                                                         sync.Mutex
	gets                                                                       []int64
}

func newFixture(t *testing.T, strategy RouteStrategy, count int) *fixture {
	t.Helper()
	f := &fixture{alias: &store.ModelAlias{Name: "public", Strategy: string(strategy)}}
	for i := 1; i <= count; i++ {
		checked := time.Date(2026, 10, 30, 0, 0, i, 0, time.UTC)
		f.rows = append(f.rows, store.Channel{ID: uint64(i), Name: fmt.Sprintf("channel-%d", i), Provider: "openai", BaseURL: "https://upstream.invalid/v1", Weight: 1, Enabled: true, LastCheckedAt: &checked})
	}
	f.models = &modelFixture{resolve: func(_ context.Context, name string) (*store.ModelAlias, error) {
		f.resolveCalls.Add(1)
		require.Equal(t, f.alias.Name, name)
		return f.alias, nil
	}}
	f.channels = &channelFixture{
		get: func(_ context.Context, id int64) (*store.Channel, error) {
			f.getCalls.Add(1)
			f.mu.Lock()
			f.gets = append(f.gets, id)
			f.mu.Unlock()
			for i := range f.rows {
				if f.rows[i].ID == uint64(id) {
					return &f.rows[i], nil
				}
			}
			return nil, fmt.Errorf("missing reference: %w", store.ErrNotFound)
		},
		list: func(_ context.Context, name string) ([]store.Channel, error) {
			f.listCalls.Add(1)
			require.Equal(t, f.alias.Name, name)
			return f.rows, nil
		},
		mapping: func(_ context.Context, id int64, name string) (*store.ChannelModel, error) {
			f.mappingCalls.Add(1)
			require.Equal(t, f.alias.Name, name)
			return &store.ChannelModel{ChannelID: uint64(id), Model: name, UpstreamModel: fmt.Sprintf("upstream-%d", id)}, nil
		},
	}
	f.health = &healthFixture{available: func(context.Context, int64) bool { f.healthCalls.Add(1); return true }}
	f.breaker = &inspectorFixture{inspect: func(context.Context, int64) (BreakerView, error) {
		f.inspectCalls.Add(1)
		return BreakerView{State: BreakerClosed, Eligible: true}, nil
	}}
	f.r = &resolver{models: f.models, channels: f.channels, health: f.health, breaker: f.breaker, defaultStrategy: Weighted}
	t.Cleanup(func() {
		require.Zero(t, f.health.reports.Load(), "planning must never report a request result")
		require.Zero(t, f.breaker.acquires.Load(), "planning must never acquire")
		require.Zero(t, f.breaker.completes.Load(), "planning must never complete")
	})
	return f
}

func candidateIDs(p *Plan) []uint64 {
	ids := make([]uint64, len(p.Candidates))
	for i, c := range p.Candidates {
		ids[i] = c.Channel.ID
	}
	return ids
}

func requireConfigError(t *testing.T, plan *Plan, err error) {
	t.Helper()
	require.Nil(t, plan)
	require.Error(t, err)
	require.NotErrorIs(t, err, access.ErrModelNotFound)
	require.NotErrorIs(t, err, provider.ErrNoCandidate)
}

func TestResolverAliasAndStrategies(t *testing.T) {
	for _, tc := range []struct {
		name                           string
		alias, requested, global, want RouteStrategy
		bad                            bool
	}{
		{"alias weighted survives global change", Weighted, Auto, CostFirst, Weighted, false},
		{"explicit override", Weighted, LatencyFirst, CostFirst, LatencyFirst, false},
		{"legacy empty", Auto, Auto, CostFirst, CostFirst, false},
		{"explicit chain", Weighted, FallbackChain, Weighted, FallbackChain, false},
		{"alias chain", FallbackChain, Auto, Weighted, FallbackChain, false},
		{"alias cost", CostFirst, Auto, Weighted, CostFirst, false},
		{"alias latency", LatencyFirst, Auto, Weighted, LatencyFirst, false},
		{"bad alias", "invalid", Auto, Weighted, Auto, true},
		{"bad alias with override", "invalid", Weighted, Weighted, Auto, true},
		{"bad explicit", Weighted, "invalid", Weighted, Auto, true},
		{"bad default", Auto, Auto, Auto, Auto, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.alias, 1)
			f.alias.FallbackIDs = "[1]"
			f.r.defaultStrategy = tc.global
			p, err := f.r.resolve(t.Context(), "public", tc.requested)
			if tc.bad {
				requireConfigError(t, p, err)
				require.Zero(t, f.mappingCalls.Load())
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, p.Strategy)
			require.Equal(t, "public", p.Model)
			require.Equal(t, []uint64{1}, candidateIDs(p))
			require.Equal(t, "upstream-1", p.Candidates[0].UpstreamModel)
			require.EqualValues(t, 1, f.resolveCalls.Load(), "upstream mapping must not recursively resolve")
		})
	}
	for _, tc := range []struct {
		name, model string
		unknown     bool
	}{
		{"empty", "", false}, {"bad utf8", string([]byte{0xff}), false}, {"129 ascii", strings.Repeat("a", 129), true}, {"129 unicode", strings.Repeat("模", 129), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, Weighted, 1)
			p, err := f.r.resolve(t.Context(), tc.model, Auto)
			require.Nil(t, p)
			require.Error(t, err)
			require.Equal(t, tc.unknown, errors.Is(err, access.ErrModelNotFound))
			require.Zero(t, f.resolveCalls.Load())
		})
	}
	for _, model := range []string{" Public ", strings.Repeat("模", 128)} {
		t.Run("exact "+model, func(t *testing.T) {
			f := newFixture(t, Weighted, 1)
			f.alias.Name = model
			p, err := f.r.resolve(t.Context(), model, Auto)
			require.NoError(t, err)
			require.Equal(t, model, p.Model)
		})
	}
	for _, tc := range []string{"missing", "sql", "nil", "wrong name"} {
		t.Run(tc, func(t *testing.T) {
			f := newFixture(t, Weighted, 1)
			original := errors.New("database address and credentials")
			f.models.resolve = func(context.Context, string) (*store.ModelAlias, error) {
				switch tc {
				case "missing":
					return nil, fmt.Errorf("lookup: %w", store.ErrNotFound)
				case "sql":
					return nil, original
				case "nil":
					return nil, nil
				default:
					return &store.ModelAlias{Name: "other"}, nil
				}
			}
			p, err := f.r.resolve(t.Context(), "public", Auto)
			require.Nil(t, p)
			require.Error(t, err)
			require.Equal(t, tc == "missing", errors.Is(err, access.ErrModelNotFound))
			require.Equal(t, tc == "missing", errors.Is(err, store.ErrNotFound))
			if tc == "sql" {
				require.ErrorIs(t, err, original)
				require.NotContains(t, err.Error(), original.Error())
			}
			require.Zero(t, f.listCalls.Load())
		})
	}
}

func TestResolverFallbackOrderAndFiltering(t *testing.T) {
	f := newFixture(t, FallbackChain, 8)
	first := uint64(3)
	f.alias.DefaultChannelID = &first
	f.alias.FallbackIDs = "[2,3,2,99,4,5,6,7]"
	f.rows[3].Enabled = false
	baseMapping := f.channels.mapping
	f.channels.mapping = func(ctx context.Context, id int64, name string) (*store.ChannelModel, error) {
		if id == 5 {
			return nil, store.ErrNotFound
		}
		return baseMapping(ctx, id, name)
	}
	f.health.available = func(_ context.Context, id int64) bool { f.healthCalls.Add(1); return id != 6 }
	f.breaker.inspect = func(_ context.Context, id int64) (BreakerView, error) {
		f.inspectCalls.Add(1)
		return BreakerView{State: BreakerOpen, Eligible: id != 7}, nil
	}
	p, err := f.r.resolve(t.Context(), "public", Auto)
	require.NoError(t, err)
	require.Equal(t, []uint64{3, 2}, candidateIDs(p))
	require.Equal(t, []int64{3, 2, 99, 4, 5, 6, 7}, f.gets)
	require.Zero(t, f.listCalls.Load(), "chain must not add outside candidates")
	require.EqualValues(t, 4, f.healthCalls.Load())
	require.EqualValues(t, 3, f.inspectCalls.Load())
}

func TestResolverFallbackDecoderAndLimits(t *testing.T) {
	for _, raw := range []string{"", "null", "[]", "{", "{}", "[2,-1]", "[2,1.5]", "[2,\"3\"]", "[18446744073709551616]", "[2,3] trailing"} {
		t.Run("tolerant "+raw, func(t *testing.T) {
			f := newFixture(t, FallbackChain, 3)
			id := uint64(1)
			f.alias.DefaultChannelID = &id
			f.alias.FallbackIDs = raw
			p, err := f.r.resolve(t.Context(), "public", Auto)
			require.NoError(t, err)
			require.Equal(t, []uint64{1}, candidateIDs(p))
			require.Equal(t, []int64{1}, f.gets)
		})
	}
	for _, tc := range []struct {
		name string
		id   *uint64
		raw  string
	}{
		{"zero default", new(uint64), "[]"}, {"overflow default", ptr(uint64(math.MaxInt64) + 1), "[]"},
		{"zero fallback", nil, "[1,0]"}, {"overflow fallback", nil, "[9223372036854775808]"}, {"null element decodes zero", nil, "[null]"},
		{"too many bytes", nil, strings.Repeat(" ", maxFallbackBytes+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, FallbackChain, 1)
			f.alias.DefaultChannelID = tc.id
			f.alias.FallbackIDs = tc.raw
			p, err := f.r.resolve(t.Context(), "public", Auto)
			requireConfigError(t, p, err)
			require.Zero(t, f.getCalls.Load())
		})
	}
	t.Run("64KiB accepted", func(t *testing.T) {
		f := newFixture(t, FallbackChain, 1)
		f.alias.FallbackIDs = "[1]" + strings.Repeat(" ", maxFallbackBytes-3)
		p, err := f.r.resolve(t.Context(), "public", Auto)
		require.NoError(t, err)
		require.Equal(t, []uint64{1}, candidateIDs(p))
	})
	t.Run("MaxInt64 accepted", func(t *testing.T) {
		f := newFixture(t, FallbackChain, 1)
		f.rows[0].ID = math.MaxInt64
		f.alias.FallbackIDs = "[9223372036854775807]"
		p, err := f.r.resolve(t.Context(), "public", Auto)
		require.NoError(t, err)
		require.Equal(t, []uint64{math.MaxInt64}, candidateIDs(p))
	})
	for _, count := range []int{64, 65} {
		t.Run(fmt.Sprint("chain count ", count), func(t *testing.T) {
			f := newFixture(t, FallbackChain, count)
			ids := make([]uint64, count)
			for i := range ids {
				ids[i] = uint64(i + 1)
			}
			ids = append(ids, 1, 1)
			raw, err := json.Marshal(ids)
			require.NoError(t, err)
			f.alias.FallbackIDs = string(raw)
			p, err := f.r.resolve(t.Context(), "public", Auto)
			if count == 64 {
				require.NoError(t, err)
				require.Len(t, p.Candidates, 64)
			} else {
				requireConfigError(t, p, err)
				require.Zero(t, f.getCalls.Load())
			}
		})
	}
	t.Run("nil default and no fallback", func(t *testing.T) {
		f := newFixture(t, FallbackChain, 1)
		p, err := f.r.resolve(t.Context(), "public", Auto)
		require.Nil(t, p)
		require.ErrorIs(t, err, provider.ErrNoCandidate)
		require.Zero(t, f.getCalls.Load())
	})
}

func ptr[T any](value T) *T { return &value }

func TestResolverStableSetAndInitialLimit(t *testing.T) {
	f := newFixture(t, Weighted, 4)
	f.rows[0].Priority = -2
	f.rows[1].Priority = 2
	f.rows[2].Priority = 2
	f.rows = append(f.rows, f.rows[1])
	slices.Reverse(f.rows)
	f.alias.DefaultChannelID = ptr(uint64(0))
	f.alias.FallbackIDs = strings.Repeat("x", maxFallbackBytes+1)
	before := slices.Clone(f.rows)
	p, err := f.r.resolve(t.Context(), "public", Auto)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3, 4, 1}, candidateIDs(p))
	require.Equal(t, before, f.rows)
	require.EqualValues(t, 4, f.mappingCalls.Load())
	require.Zero(t, f.getCalls.Load())
	for _, tc := range []string{"duplicate conflict", "zero ID", "overflow ID", "64 unique", "65 unique even unhealthy", "65 unique even disabled"} {
		t.Run(tc, func(t *testing.T) {
			count := 1
			if strings.HasPrefix(tc, "64") {
				count = 64
			}
			if strings.HasPrefix(tc, "65") {
				count = 65
			}
			f := newFixture(t, Weighted, count)
			switch tc {
			case "duplicate conflict":
				row := f.rows[0]
				row.Weight = 2
				f.rows = append(f.rows, row)
			case "zero ID":
				f.rows[0].ID = 0
			case "overflow ID":
				f.rows[0].ID = math.MaxInt64 + 1
			case "65 unique even unhealthy":
				f.health.available = func(context.Context, int64) bool { return false }
			case "65 unique even disabled":
				for i := range f.rows {
					f.rows[i].Enabled = false
				}
			}
			p, err := f.r.resolve(t.Context(), "public", Auto)
			if tc == "64 unique" {
				require.NoError(t, err)
				require.Len(t, p.Candidates, 64)
			} else {
				requireConfigError(t, p, err)
				require.Zero(t, f.mappingCalls.Load())
				require.Zero(t, f.healthCalls.Load())
			}
		})
	}
}

func TestResolverChannelAndMappingValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*store.Channel)
	}{
		{"provider", func(c *store.Channel) { c.Provider = "mock" }}, {"weight zero", func(c *store.Channel) { c.Weight = 0 }}, {"weight negative", func(c *store.Channel) { c.Weight = -1 }}, {"weight too large", func(c *store.Channel) { c.Weight = 10001 }},
		{"empty name", func(c *store.Channel) { c.Name = "" }}, {"bad name", func(c *store.Channel) { c.Name = string([]byte{255}) }}, {"long name", func(c *store.Channel) { c.Name = strings.Repeat("x", 129) }},
		{"bad URL", func(c *store.Channel) { c.BaseURL = "file:///tmp/key" }}, {"credentials in URL", func(c *store.Channel) { c.BaseURL = "https://user:password@upstream.invalid" }}, {"query", func(c *store.Channel) { c.BaseURL += "?key=secret" }}, {"fragment", func(c *store.Channel) { c.BaseURL += "#" }}, {"port zero", func(c *store.Channel) { c.BaseURL = "http://localhost:0" }}, {"port overflow", func(c *store.Channel) { c.BaseURL = "http://localhost:65536" }}, {"empty port", func(c *store.Channel) { c.BaseURL = "http://localhost:" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, Weighted, 1)
			tc.change(&f.rows[0])
			p, err := f.r.resolve(t.Context(), "public", Auto)
			requireConfigError(t, p, err)
			require.Zero(t, f.mappingCalls.Load())
			require.NotContains(t, err.Error(), f.rows[0].BaseURL)
		})
	}
	for _, tc := range []string{"nil", "wrong channel", "wrong model", "empty upstream", "invalid upstream", "long upstream", "SQL", "missing"} {
		t.Run("mapping "+tc, func(t *testing.T) {
			f := newFixture(t, Weighted, 1)
			sqlErr := errors.New("mapping SQL failure")
			f.channels.mapping = func(context.Context, int64, string) (*store.ChannelModel, error) {
				m := &store.ChannelModel{ChannelID: 1, Model: "public", UpstreamModel: "upstream"}
				switch tc {
				case "nil":
					return nil, nil
				case "wrong channel":
					m.ChannelID = 2
				case "wrong model":
					m.Model = "other"
				case "empty upstream":
					m.UpstreamModel = ""
				case "invalid upstream":
					m.UpstreamModel = string([]byte{255})
				case "long upstream":
					m.UpstreamModel = strings.Repeat("模", 129)
				case "SQL":
					return nil, sqlErr
				case "missing":
					return nil, store.ErrNotFound
				}
				return m, nil
			}
			p, err := f.r.resolve(t.Context(), "public", Auto)
			if tc == "missing" {
				require.Nil(t, p)
				require.ErrorIs(t, err, provider.ErrNoCandidate)
			} else {
				requireConfigError(t, p, err)
			}
			if tc == "SQL" {
				require.ErrorIs(t, err, sqlErr)
			}
			require.Zero(t, f.healthCalls.Load())
			require.Zero(t, f.inspectCalls.Load())
		})
	}
	for _, tc := range []string{"list SQL", "get SQL", "nil get", "wrong get"} {
		t.Run(tc, func(t *testing.T) {
			f := newFixture(t, Weighted, 1)
			sqlErr := errors.New("candidate SQL failure")
			if tc == "list SQL" {
				f.channels.list = func(context.Context, string) ([]store.Channel, error) { return nil, sqlErr }
			} else {
				f.alias.Strategy = string(FallbackChain)
				f.alias.FallbackIDs = "[1]"
				f.channels.get = func(context.Context, int64) (*store.Channel, error) {
					switch tc {
					case "get SQL":
						return nil, sqlErr
					case "nil get":
						return nil, nil
					default:
						return &store.Channel{ID: 2}, nil
					}
				}
			}
			p, err := f.r.resolve(t.Context(), "public", Auto)
			requireConfigError(t, p, err)
			if strings.Contains(tc, "SQL") {
				require.ErrorIs(t, err, sqlErr)
			}
			require.Zero(t, f.mappingCalls.Load())
		})
	}
	t.Run("valid scalar boundaries", func(t *testing.T) {
		f := newFixture(t, Weighted, 2)
		f.rows[0].Weight = 10000
		f.rows[0].Name = strings.Repeat("模", 128)
		p, err := f.r.resolve(t.Context(), "public", Auto)
		require.NoError(t, err)
		require.Len(t, p.Candidates, 2)
	})
}

func TestResolverReadOnlyFiltering(t *testing.T) {
	for _, tc := range []struct {
		name       string
		available  bool
		view       BreakerView
		inspectErr error
		want       bool
		bad        bool
	}{
		{"healthy closed", true, BreakerView{BreakerClosed, true}, nil, true, false},
		{"closed capacity exhausted", true, BreakerView{BreakerClosed, false}, nil, false, false},
		{"health false or Redis fault", false, BreakerView{}, nil, false, false},
		{"open cooldown", true, BreakerView{BreakerOpen, false}, nil, false, false},
		{"open expired eligible", true, BreakerView{BreakerOpen, true}, nil, true, false},
		{"half open busy", true, BreakerView{BreakerHalfOpen, false}, nil, false, false},
		{"half open lease expired", true, BreakerView{BreakerHalfOpen, true}, nil, true, false},
		{"inspect explicit error", true, BreakerView{BreakerClosed, true}, errors.New("Redis fault"), false, true},
		{"unknown state", true, BreakerView{"unknown", true}, nil, false, true},
		{"zero view", true, BreakerView{}, nil, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, Weighted, 1)
			f.rows[0].HealthStatus = "disabled"
			f.rows[0].ErrorRateCache = 1
			f.rows[0].LatencyP99 = math.MaxInt32
			f.health.available = func(context.Context, int64) bool { return tc.available }
			f.breaker.inspect = func(context.Context, int64) (BreakerView, error) {
				f.inspectCalls.Add(1)
				return tc.view, tc.inspectErr
			}
			p, err := f.r.resolve(t.Context(), "public", Auto)
			if tc.want {
				require.NoError(t, err)
				require.Equal(t, []uint64{1}, candidateIDs(p))
			} else if tc.bad {
				requireConfigError(t, p, err)
			} else {
				require.Nil(t, p)
				require.ErrorIs(t, err, provider.ErrNoCandidate)
			}
			if tc.inspectErr != nil {
				require.ErrorIs(t, err, tc.inspectErr)
			}
			if !tc.available {
				require.Zero(t, f.inspectCalls.Load())
			}
		})
	}
	t.Run("disabled rows never map", func(t *testing.T) {
		f := newFixture(t, Weighted, 1)
		f.rows[0].Enabled = false
		p, err := f.r.resolve(t.Context(), "public", Auto)
		require.Nil(t, p)
		require.ErrorIs(t, err, provider.ErrNoCandidate)
		require.Zero(t, f.mappingCalls.Load())
	})
	t.Run("empty set", func(t *testing.T) {
		f := newFixture(t, Weighted, 0)
		p, err := f.r.resolve(t.Context(), "public", Auto)
		require.Nil(t, p)
		require.ErrorIs(t, err, provider.ErrNoCandidate)
		require.Zero(t, f.mappingCalls.Load())
		require.Zero(t, f.inspectCalls.Load())
	})
}

func TestResolverSnapshots(t *testing.T) {
	for _, strategy := range []RouteStrategy{Weighted, FallbackChain} {
		t.Run(string(strategy), func(t *testing.T) {
			f := newFixture(t, strategy, 2)
			f.alias.FallbackIDs = "[1,2]"
			first := uint64(1)
			f.alias.DefaultChannelID = &first
			p1, err := f.r.resolve(t.Context(), "public", Auto)
			require.NoError(t, err)
			p2, err := f.r.resolve(t.Context(), "public", Auto)
			require.NoError(t, err)
			original := *f.rows[0].LastCheckedAt
			*p1.Candidates[0].Channel.LastCheckedAt = original.Add(time.Hour)
			p1.Candidates[0].Channel.Name = "changed"
			p1.Candidates[0].UpstreamModel = "changed"
			p1.Model = "changed"
			require.Equal(t, original, *f.rows[0].LastCheckedAt)
			require.Equal(t, original, *p2.Candidates[0].Channel.LastCheckedAt)
			require.Equal(t, "channel-1", p2.Candidates[0].Channel.Name)
			require.Equal(t, "upstream-1", p2.Candidates[0].UpstreamModel)
			*f.rows[0].LastCheckedAt = original.Add(2 * time.Hour)
			f.rows[0].Name = "repo changed"
			f.alias.Strategy = "invalid"
			require.Equal(t, original, *p2.Candidates[0].Channel.LastCheckedAt)
			require.Equal(t, "channel-1", p2.Candidates[0].Channel.Name)
			require.Equal(t, strategy, p2.Strategy)
		})
	}
	t.Run("dependency reconfiguration cannot mutate current snapshot", func(t *testing.T) {
		f := newFixture(t, FallbackChain, 2)
		f.alias.FallbackIDs = "[1,2]"
		f.alias.DefaultChannelID = ptr(uint64(1))
		original := *f.rows[0].LastCheckedAt
		mapping := &store.ChannelModel{ChannelID: 1, Model: "public", UpstreamModel: "old upstream"}
		baseGet := f.channels.get
		f.channels.get = func(ctx context.Context, id int64) (*store.Channel, error) {
			*f.alias.DefaultChannelID = 99
			f.alias.FallbackIDs = "[99]"
			return baseGet(ctx, id)
		}
		baseMap := f.channels.mapping
		f.channels.mapping = func(ctx context.Context, id int64, name string) (*store.ChannelModel, error) {
			if id == 1 {
				return mapping, nil
			}
			return baseMap(ctx, id, name)
		}
		f.health.available = func(_ context.Context, id int64) bool {
			if id == 1 {
				mapping.UpstreamModel = "new upstream"
				*f.rows[0].LastCheckedAt = original.Add(time.Hour)
				f.rows[0].Name = "new channel"
			}
			return true
		}
		p, err := f.r.resolve(t.Context(), "public", Auto)
		require.NoError(t, err)
		require.Equal(t, []uint64{1, 2}, candidateIDs(p))
		require.Equal(t, "old upstream", p.Candidates[0].UpstreamModel)
		require.Equal(t, "channel-1", p.Candidates[0].Channel.Name)
		require.Equal(t, original, *p.Candidates[0].Channel.LastCheckedAt)
	})
}

func TestResolverConcurrentSnapshots(t *testing.T) {
	f := newFixture(t, Weighted, 2)
	original := *f.rows[0].LastCheckedAt
	const count = 32
	start := make(chan struct{})
	plans := make(chan *Plan, count)
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, err := f.r.resolve(t.Context(), "public", Auto)
			if err != nil {
				errs <- err
				return
			}
			p.Candidates[0].Channel.Name = "private"
			*p.Candidates[0].Channel.LastCheckedAt = original.Add(time.Hour)
			plans <- p
		}()
	}
	close(start)
	wg.Wait()
	close(plans)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Len(t, plans, count)
	var previous *Plan
	for p := range plans {
		if previous != nil {
			require.NotSame(t, previous.Candidates[0].Channel.LastCheckedAt, p.Candidates[0].Channel.LastCheckedAt)
		}
		previous = p
	}
	require.Equal(t, original, *f.rows[0].LastCheckedAt)
	require.Equal(t, "channel-1", f.rows[0].Name)
	require.EqualValues(t, count, f.resolveCalls.Load())
	require.EqualValues(t, 2*count, f.inspectCalls.Load())
}

func TestResolverCancellationStopsReads(t *testing.T) {
	for _, stage := range []string{"before", "resolve", "list", "get", "mapping", "health", "inspect"} {
		t.Run(stage, func(t *testing.T) {
			f := newFixture(t, Weighted, 2)
			ctx, cancel := context.WithCancelCause(t.Context())
			cause := errors.New("caller stopped")
			defer cancel(nil)
			switch stage {
			case "before":
				cancel(cause)
			case "resolve":
				base := f.models.resolve
				f.models.resolve = func(ctx context.Context, name string) (*store.ModelAlias, error) {
					a, e := base(ctx, name)
					cancel(cause)
					return a, e
				}
			case "list":
				base := f.channels.list
				f.channels.list = func(ctx context.Context, name string) ([]store.Channel, error) {
					rows, e := base(ctx, name)
					cancel(cause)
					return rows, e
				}
			case "get":
				f.alias.Strategy = string(FallbackChain)
				f.alias.FallbackIDs = "[1,2]"
				base := f.channels.get
				f.channels.get = func(ctx context.Context, id int64) (*store.Channel, error) {
					row, e := base(ctx, id)
					cancel(cause)
					return row, e
				}
			case "mapping":
				base := f.channels.mapping
				f.channels.mapping = func(ctx context.Context, id int64, name string) (*store.ChannelModel, error) {
					m, e := base(ctx, id, name)
					cancel(cause)
					return m, e
				}
			case "health":
				f.health.available = func(context.Context, int64) bool { cancel(cause); return false }
			case "inspect":
				f.breaker.inspect = func(context.Context, int64) (BreakerView, error) {
					f.inspectCalls.Add(1)
					cancel(cause)
					return BreakerView{BreakerClosed, true}, nil
				}
			}
			p, err := f.r.resolve(ctx, "public", Auto)
			require.Nil(t, p)
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, cause)
			if stage == "before" {
				require.Zero(t, f.resolveCalls.Load())
			}
			if stage == "get" {
				require.EqualValues(t, 1, f.getCalls.Load())
			}
			if stage == "mapping" {
				require.EqualValues(t, 1, f.mappingCalls.Load())
				require.Zero(t, f.healthCalls.Load())
			}
			if stage == "health" {
				require.Zero(t, f.inspectCalls.Load())
			}
			if stage == "inspect" {
				require.EqualValues(t, 1, f.mappingCalls.Load())
			}
		})
	}
}

// Mechanical zero-side-effect witness: production planning has no KeyRepo,
// picker, secret or Provider capability and no permit/report/ordering calls.
func TestResolverHasOnlyReadCapabilities(t *testing.T) {
	for _, name := range []string{"resolver.go", "types.go", "router.go"} {
		source, err := os.ReadFile(name)
		require.NoError(t, err)
		file, err := parser.ParseFile(token.NewFileSet(), name, source, 0)
		require.NoError(t, err)
		for _, imp := range file.Imports {
			require.NotContains(t, imp.Path.Value, "/secret")
			require.NotContains(t, imp.Path.Value, "/bootstrap")
			require.NotContains(t, imp.Path.Value, "/provider/openai")
		}
		ast.Inspect(file, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok {
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					require.NotContains(t, []string{"Pick", "Decrypt", "Chat", "ChatStream", "Acquire", "Complete", "Allow", "OnResult", "ReportRequestResult", "ListByChannel"}, selector.Sel.Name)
					if name != "router.go" {
						require.NotEqual(t, "Order", selector.Sel.Name)
					}
				}
			}
			if selector, ok := n.(*ast.SelectorExpr); ok {
				require.NotContains(t, []string{"Endpoint", "EndpointPicker", "KeyRepo", "ChannelKey"}, selector.Sel.Name)
				if pkg, ok := selector.X.(*ast.Ident); ok && pkg.Name == "provider" {
					require.NotEqual(t, "Provider", selector.Sel.Name)
				}
			}
			return true
		})
	}
}
