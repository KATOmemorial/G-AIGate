package router

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"gaigate/internal/channel"
	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

type modelRouter struct {
	resolver resolver
	orderer  Orderer
	timeout  time.Duration
	logger   *slog.Logger
}

var _ Router = (*modelRouter)(nil)

// NewRouter only assembles injected dependencies; it performs no I/O.
// A zero RouteTimeout selects the 2s library default. Explicit config values
// are validated separately so a configured zero is never silently defaulted.
func NewRouter(models store.ModelRepo, channels store.ChannelRepo, health channel.HealthRegistry,
	breaker BreakerInspector, orderer Orderer, opts RouterOptions, logger *slog.Logger) (Router, error) {
	for _, dependency := range []any{models, channels, health, breaker, orderer, logger} {
		if nilDependency(dependency) {
			return nil, failure("missing dependency", nil)
		}
	}
	if opts.RouteTimeout == 0 {
		opts.RouteTimeout = 2 * time.Second
	}
	if !validStrategy(opts.DefaultStrategy) || opts.RouteTimeout < time.Millisecond || opts.RouteTimeout > time.Minute || opts.RouteTimeout%time.Millisecond != 0 {
		return nil, failure("invalid router options", nil)
	}
	return &modelRouter{
		resolver: resolver{models: models, channels: channels, health: health, breaker: breaker, defaultStrategy: opts.DefaultStrategy},
		orderer:  orderer, timeout: opts.RouteTimeout, logger: logger,
	}, nil
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflect.ValueOf(value).IsNil()
	}
	return false
}

func (r *modelRouter) Route(parent context.Context, model string, strategy RouteStrategy) (plan *Plan, err error) {
	// This single deadline covers alias, all candidate reads and ordering. The
	// dependency calls remain synchronous and must honor this same context.
	ctx, cancel := context.WithTimeout(parent, r.timeout)
	defer cancel()
	defer func() {
		if err != nil {
			requestID, _ := middleware.RequestIDFromContext(parent)
			stage := "planning failed"
			var classified *stageError
			if errors.As(err, &classified) {
				stage = classified.stage
			}
			r.logger.ErrorContext(parent, "router planning failed", "stage", stage, "request_id", requestID)
		}
	}()
	plan, err = r.resolver.resolve(ctx, model, strategy)
	if err != nil {
		return nil, err
	}
	if err = contextError(ctx); err != nil {
		return nil, err
	}
	ordered, orderErr := r.orderer.Order(ctx, model, plan.Strategy, cloneCandidates(plan.Candidates))
	if err = contextError(ctx); err != nil {
		return nil, err
	}
	if orderErr != nil {
		return nil, failure("candidate ordering failed", orderErr)
	}
	candidates, permutationErr := checkedPermutation(plan.Candidates, ordered)
	if permutationErr != nil {
		return nil, permutationErr
	}
	if err = contextError(ctx); err != nil {
		return nil, err
	}
	plan.Candidates = candidates
	return plan, nil
}

func cloneCandidates(candidates []Candidate) []Candidate {
	cloned := make([]Candidate, len(candidates))
	for i, candidate := range candidates {
		candidate.Channel = cloneChannel(candidate.Channel)
		cloned[i] = candidate
	}
	return cloned
}

func checkedPermutation(expected, ordered []Candidate) ([]Candidate, error) {
	if len(expected) != len(ordered) {
		return nil, failure("invalid candidate permutation", nil)
	}
	byID := make(map[uint64]Candidate, len(expected))
	for _, candidate := range expected {
		byID[candidate.Channel.ID] = candidate
	}
	result := make([]Candidate, 0, len(expected))
	for _, candidate := range ordered {
		original, ok := byID[candidate.Channel.ID]
		if !ok || !reflect.DeepEqual(original, candidate) {
			return nil, failure("invalid candidate permutation", nil)
		}
		delete(byID, candidate.Channel.ID)
		// Return our canonical snapshot in the chosen order, never memory retained
		// by an injected orderer. This also isolates its input and output aliases.
		original.Channel = cloneChannel(original.Channel)
		result = append(result, original)
	}
	return result, nil
}
