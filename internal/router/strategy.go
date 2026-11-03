package router

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"sort"
	"time"

	"gaigate/internal/store"
)

// routeStartedKey carries only this package's Route timestamp, never a new budget.
type routeStartedKey struct{}

type strategyEngine struct {
	prices store.PriceRepo
	logger *slog.Logger
	now    func() time.Time
	ticket func(int64) int64
	latencySamples
}

var _ StrategyEngine = (*strategyEngine)(nil)

// NewStrategyEngine assembles bounded local state without I/O or background work.
func NewStrategyEngine(prices store.PriceRepo, logger *slog.Logger) (StrategyEngine, error) {
	if nilDependency(prices) || logger == nil {
		return nil, failure("missing strategy dependency", nil)
	}
	return &strategyEngine{
		prices: prices, logger: logger, now: time.Now,
		// The standard library's top-level source is concurrent-safe and randomly seeded.
		ticket:         rand.Int64N,
		latencySamples: latencySamples{entries: make(map[latencyKey]*latencyEntry)},
	}, nil
}

func (s *strategyEngine) Order(ctx context.Context, model string, strategy RouteStrategy, candidates []Candidate) ([]Candidate, error) {
	if ctx == nil {
		return nil, failure("invalid ordering context", nil)
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if !validName(model) || !validStrategy(strategy) || len(candidates) > maxCandidates {
		return nil, failure("invalid ordering input", nil)
	}
	seen := make(map[uint64]bool, len(candidates))
	for _, c := range candidates {
		if !c.Channel.Enabled || !validChannel(c.Channel) || !validName(c.UpstreamModel) || seen[c.Channel.ID] {
			return nil, failure("invalid ordering candidate", nil)
		}
		seen[c.Channel.ID] = true
	}
	ordered := cloneCandidates(candidates)
	if strategy == FallbackChain {
		return ordered, contextError(ctx)
	}
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i].Channel, ordered[j].Channel
		return a.Priority > b.Priority || a.Priority == b.Priority && a.ID < b.ID
	})
	switch strategy {
	case Weighted:
		for start := 0; start < len(ordered); {
			end := start + 1
			for end < len(ordered) && ordered[end].Channel.Priority == ordered[start].Channel.Priority {
				end++
			}
			if err := s.weighted(ctx, ordered[start:end]); err != nil {
				return nil, err
			}
			start = end
		}
	case CostFirst:
		now, ok := ctx.Value(routeStartedKey{}).(time.Time)
		if !ok {
			now = s.now()
		}
		prices, err := s.costs(ctx, ordered, now.UTC())
		if err != nil {
			return nil, err
		}
		sort.Slice(ordered, func(i, j int) bool {
			a, b := ordered[i], ordered[j]
			return rankedLess(a, b, prices[a.UpstreamModel], prices[b.UpstreamModel])
		})
	case LatencyFirst:
		ranks := s.latencies(model, ordered, s.now())
		sort.Slice(ordered, func(i, j int) bool {
			a, b := ordered[i], ordered[j]
			return rankedLess(a, b, ranks[a.Channel.ID], ranks[b.Channel.ID])
		})
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	return ordered, nil
}

func (s *strategyEngine) weighted(ctx context.Context, group []Candidate) error {
	var total int64
	for _, c := range group {
		weight := int64(c.Channel.Weight)
		if weight <= 0 || total > math.MaxInt64-weight {
			return failure("invalid weight total", nil)
		}
		total += weight
	}
	remaining := append([]Candidate(nil), group...)
	for i := range group {
		if err := contextError(ctx); err != nil {
			return err
		}
		ticket := s.ticket(total)
		if ticket < 0 || ticket >= total {
			return failure("invalid random ticket", nil)
		}
		for j, c := range remaining {
			weight := int64(c.Channel.Weight)
			if ticket < weight {
				group[i] = c
				total -= weight
				remaining = append(remaining[:j], remaining[j+1:]...)
				break
			}
			ticket -= weight
		}
	}
	return nil
}

// Two nonnegative int64 prices sum to at most MaxUint64-1, exactly in uint64.
// The same rank representation holds nonnegative Duration values for latency.
type strategyRank struct {
	known bool
	value uint64
}

func rankedLess(a, b Candidate, ar, br strategyRank) bool {
	if a.Channel.Priority != b.Channel.Priority {
		return a.Channel.Priority > b.Channel.Priority
	}
	if ar.known != br.known {
		return ar.known
	}
	if ar.value != br.value {
		return ar.value < br.value
	}
	return a.Channel.ID < b.Channel.ID
}

func (s *strategyEngine) costs(ctx context.Context, candidates []Candidate, now time.Time) (map[string]strategyRank, error) {
	ranks := make(map[string]strategyRank, len(candidates))
	for _, c := range candidates {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		if _, ok := ranks[c.UpstreamModel]; ok {
			continue
		}
		price, err := s.prices.Effective(ctx, c.UpstreamModel, now)
		if canceled := contextError(ctx); canceled != nil {
			return nil, canceled
		}
		if errors.Is(err, store.ErrNoPrice) {
			ranks[c.UpstreamModel] = strategyRank{}
			continue
		}
		if err != nil {
			return nil, failure("price lookup failed", err)
		}
		if price == nil || price.InputPricePerM < 0 || price.OutputPricePerM < 0 || price.Currency != "CNY" {
			return nil, failure("invalid price configuration", nil)
		}
		ranks[c.UpstreamModel] = strategyRank{known: true, value: uint64(price.InputPricePerM) + uint64(price.OutputPricePerM)}
	}
	return ranks, nil
}
