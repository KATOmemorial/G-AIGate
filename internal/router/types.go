// Package router plans model candidates without selecting credentials or issuing requests.
package router

import (
	"context"
	"time"

	"gaigate/internal/store"
)

// RouteStrategy selects ordering within the configured candidate set.
type RouteStrategy string

const (
	Auto          RouteStrategy = ""
	Weighted      RouteStrategy = "weighted"
	CostFirst     RouteStrategy = "cost_first"
	LatencyFirst  RouteStrategy = "latency_first"
	FallbackChain RouteStrategy = "fallback_chain"
)

// Candidate is a channel and mapping snapshot; it never holds a channel Key.
type Candidate struct {
	Channel       store.Channel
	UpstreamModel string
}

// Plan is an optimistic request-local snapshot, with at most 64 unique candidates.
type Plan struct {
	Model      string
	Strategy   RouteStrategy
	Candidates []Candidate
}

// Router resolves a public alias and orders its eligible candidates.
type Router interface {
	Route(ctx context.Context, model string, strategy RouteStrategy) (*Plan, error)
}

// BreakerState describes the read-only view; actual permits belong to 16.3.
type BreakerState string

const (
	BreakerClosed   BreakerState = "closed"
	BreakerOpen     BreakerState = "open"
	BreakerHalfOpen BreakerState = "half-open"
)

// BreakerView reports eligibility without reserving a half-open probe.
type BreakerView struct {
	State    BreakerState
	Eligible bool
}

// BreakerInspector performs no state transitions or permit acquisition.
type BreakerInspector interface {
	Inspect(ctx context.Context, channelID int64) (BreakerView, error)
}

// Orderer returns a full permutation of the supplied candidates.
type Orderer interface {
	Order(ctx context.Context, model string, strategy RouteStrategy, candidates []Candidate) ([]Candidate, error)
}

// RouterOptions configures request planning; zero RouteTimeout defaults to 2s.
type RouterOptions struct {
	DefaultStrategy RouteStrategy
	RouteTimeout    time.Duration
}
