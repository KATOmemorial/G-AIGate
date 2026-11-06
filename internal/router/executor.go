package router

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"gaigate/internal/access"
	"gaigate/internal/channel"
	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"
	"gaigate/internal/store"
)

// Executor executes an already ordered, request-local plan.
type Executor interface {
	Execute(context.Context, http.ResponseWriter, *access.Principal, *Plan, *provider.ChatRequest) error
}

type ExecutorOptions struct {
	Forward                          proxy.ForwardOptions
	StateTimeout, PreparationTimeout time.Duration
	MaxAttempts                      int
}

type planExecutor struct {
	picker     channel.EndpointPicker
	upstream   provider.Provider
	forwarder  proxy.Forwarder
	breaker    Breaker
	health     channel.HealthRegistry
	strategies StrategyEngine
	opts       ExecutorOptions
	logger     *slog.Logger
}

// NewExecutor validates without I/O or workers. Configuration supplies explicit
// positive timeouts and MaxAttempts (the configured default is three).
func NewExecutor(picker channel.EndpointPicker, upstream provider.Provider, forwarder proxy.Forwarder,
	breaker Breaker, health channel.HealthRegistry, strategies StrategyEngine, opts ExecutorOptions, logger *slog.Logger) (Executor, error) {
	for _, dep := range []any{picker, upstream, forwarder, breaker, health, strategies, logger} {
		if nilDependency(dep) {
			return nil, failure("missing executor dependency", nil)
		}
	}
	const maxLease = 24 * time.Hour
	const margin = 500 * time.Millisecond
	if opts.StateTimeout <= 0 || opts.PreparationTimeout <= 0 || opts.Forward.TTFBTimeout <= 0 ||
		opts.Forward.OverallTimeout <= 0 || opts.MaxAttempts < 1 || opts.MaxAttempts > 16 ||
		opts.StateTimeout > maxLease-margin || opts.Forward.OverallTimeout > maxLease-margin-opts.StateTimeout {
		return nil, failure("invalid executor options", nil)
	}
	return &planExecutor{picker, upstream, forwarder, breaker, health, strategies, opts, logger}, nil
}

func validateExecution(ctx context.Context, w http.ResponseWriter, p *access.Principal, plan *Plan, req *provider.ChatRequest) error {
	if ctx == nil || nilDependency(w) || p == nil || plan == nil || req == nil || !validName(req.Model) ||
		plan.Model != req.Model || !validStrategy(plan.Strategy) || len(plan.Candidates) > maxCandidates {
		return failure("invalid execution input", nil)
	}
	seen := make(map[uint64]bool, len(plan.Candidates))
	for _, c := range plan.Candidates {
		if !c.Channel.Enabled || !validChannel(c.Channel) || !validName(c.UpstreamModel) || seen[c.Channel.ID] {
			return failure("invalid execution candidate", nil)
		}
		seen[c.Channel.ID] = true
	}
	return nil
}

// committed recognizes the production Gin/WrapWriter chain without writing to it.
func committed(w http.ResponseWriter) bool {
	for range 64 {
		if nilDependency(w) {
			return true
		}
		if v, ok := w.(interface{ Written() bool }); ok && v.Written() {
			return true
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		w = u.Unwrap()
	}
	return true
}

func executionBoundary(ctx context.Context, w http.ResponseWriter) error {
	if ctx.Err() != nil {
		return &access.ExecutionError{Cause: errors.Join(ctx.Err(), context.Cause(ctx)),
			WroteHeader: committed(w), TimedOut: errors.Is(ctx.Err(), context.DeadlineExceeded)}
	}
	if committed(w) {
		return &access.ExecutionError{Cause: failure("writer already committed", nil), WroteHeader: true}
	}
	return nil
}

func (e *planExecutor) Execute(parent context.Context, w http.ResponseWriter, p *access.Principal, plan *Plan, req *provider.ChatRequest) error {
	if err := validateExecution(parent, w, p, plan, req); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, e.opts.Forward.OverallTimeout)
	defer cancel()
	var last error
	attempts := 0
	for _, c := range plan.Candidates {
		if err := executionBoundary(ctx, w); err != nil {
			return err
		}
		if attempts >= e.opts.MaxAttempts {
			break
		}
		result, attempted, skip, cause := e.candidate(ctx, w, c, req, &attempts)
		if skip {
			continue
		}
		if !attempted {
			return cause
		}
		if result.Kind == proxy.ResultSuccess && cause == nil {
			return nil
		}
		if cause == nil {
			cause = failure("upstream attempt failed", provider.ErrUpstreamUnavailable)
		}
		last = &access.ExecutionError{Cause: cause, WroteHeader: result.WroteHeader || committed(w),
			TimedOut: result.Kind == proxy.ResultTTFBTimeout || result.Kind == proxy.ResultOverallTimeout}
		// A proxy-latched timeout survives the subsequent net/http cancellation.
		// Inspect Kind before the next preparation boundary can see Canceled.
		if ctx.Err() == context.Canceled && (result.Kind == proxy.ResultOverallTimeout || result.Kind == proxy.ResultTTFBTimeout) {
			return &access.ExecutionError{Cause: errors.Join(cause, ctx.Err(), context.Cause(ctx)), WroteHeader: result.WroteHeader || committed(w), TimedOut: true}
		}
		_, _, retry := executionSignal(result.Kind, cause)
		if result.WroteHeader || committed(w) || !retry {
			return last
		}
	}
	if err := executionBoundary(ctx, w); err != nil {
		return err
	}
	if last != nil {
		return last
	}
	return failure("no execution candidate", provider.ErrNoCandidate)
}

// candidate owns every acquired permit, including preparation and panic exits.
func (e *planExecutor) candidate(ctx context.Context, w http.ResponseWriter, c Candidate, req *provider.ChatRequest, attempts *int) (result proxy.ForwardResult, attempted, skip bool, err error) {
	prep, cancel := context.WithTimeout(ctx, e.opts.PreparationTimeout)
	defer cancel()
	id := int64(c.Channel.ID)
	available := e.health.IsAvailable(prep, id)
	if err = e.preparationError(ctx, prep, w, nil); err != nil {
		return
	}
	if !available {
		skip = true
		return
	}
	deadline, _ := ctx.Deadline()
	lease := time.Until(deadline) + e.opts.StateTimeout + 500*time.Millisecond
	if lease <= 0 {
		err = executionBoundary(ctx, w)
		return
	}
	lease = ((lease + time.Millisecond - 1) / time.Millisecond) * time.Millisecond
	permit, acquireErr := e.breaker.Acquire(prep, id, lease)
	outcome, report := BreakerNeutral, false
	var elapsed time.Duration
	defer func() {
		if permit != nil {
			feedback := e.feedback(ctx, permit, id, outcome, report)
			if attempted {
				e.logAttempt(ctx, c, req.Model, result, *attempts, elapsed, feedback)
			}
		}
	}()
	if err = e.preparationError(ctx, prep, w, acquireErr); err != nil {
		return
	}
	if permit == nil {
		skip = true
		return
	}
	endpoint, pickErr := e.picker.Pick(prep, &c.Channel, req.Model)
	if err = e.preparationError(ctx, prep, w, nil); err != nil {
		return
	}
	if errors.Is(pickErr, provider.ErrNoCandidate) || errors.Is(pickErr, store.ErrNotFound) {
		skip = true
		return
	}
	if pickErr != nil {
		err = failure("endpoint preparation failed", pickErr)
		return
	}
	if endpoint == nil || endpoint.ChannelID != id || endpoint.Provider != c.Channel.Provider ||
		endpoint.Provider != e.upstream.Name() || endpoint.BaseURL != c.Channel.BaseURL || endpoint.APIKey == "" {
		err = failure("invalid execution endpoint", nil)
		return
	}
	if endpoint.Model != c.UpstreamModel {
		skip = true
		return
	}
	cancel() // opener must use proxy's context, never this preparation context.
	if err = executionBoundary(ctx, w); err != nil {
		return
	}
	start := time.Now()
	open := func(openCtx context.Context) (context.Context, error) {
		if err := executionBoundary(ctx, w); err != nil {
			return nil, err
		}
		if err := openCtx.Err(); err != nil {
			return nil, err
		}
		if attempted {
			return nil, failure("duplicate opener", nil)
		}
		attempted = true
		start = time.Now()
		*attempts++
		return middleware.WithChannelID(openCtx, id), nil
	}
	if req.Stream {
		result, err = e.forwarder.Forward(ctx, w, func(openCtx context.Context) (provider.Stream, error) {
			openCtx, err := open(openCtx)
			if err != nil {
				return nil, err
			}
			return e.upstream.ChatStream(openCtx, endpoint, req)
		}, e.opts.Forward, nil)
	} else {
		result, err = e.forwarder.ForwardNonStream(ctx, w, func(openCtx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
			openCtx, err := open(openCtx)
			if err != nil {
				return nil, nil, err
			}
			return e.upstream.Chat(openCtx, endpoint, req)
		}, e.opts.Forward)
	}
	elapsed = time.Since(start)
	if !attempted {
		if boundary := executionBoundary(ctx, w); boundary != nil {
			err = boundary
		} else {
			err = failure("forwarder did not open upstream", err)
		}
		return
	}
	if result.Kind == proxy.ResultSuccess && err == nil {
		e.strategies.ObserveSuccess(ctx, id, req.Model, elapsed)
	}
	outcome, report, _ = executionSignal(result.Kind, err)
	// TODO(17.1): consume authoritative usage and partial failure consumption (台账#21).
	// TODO(17.2): settle the logical request; Key feedback remains deferred (台账#22/#29).
	return
}

func (e *planExecutor) preparationError(ctx, prep context.Context, w http.ResponseWriter, cause error) error {
	if err := executionBoundary(ctx, w); err != nil {
		return err
	}
	if prep.Err() != nil {
		return failure("candidate preparation canceled", errors.Join(prep.Err(), context.Cause(prep), cause))
	}
	if cause != nil {
		return failure("candidate acquisition failed", cause)
	}
	return nil
}

func executionSignal(kind proxy.ResultKind, err error) (BreakerOutcome, bool, bool) {
	switch kind {
	case proxy.ResultSuccess:
		return BreakerSuccess, true, false
	case proxy.ResultTTFBTimeout, proxy.ResultOverallTimeout, proxy.ResultStreamInterrupted:
		return BreakerFailure, true, true
	case proxy.ResultUpstreamError:
		var u *provider.UpstreamError
		if errors.As(err, &u) && u != nil {
			if u.StatusCode == 400 || u.StatusCode == 404 || u.StatusCode == 422 {
				return BreakerNeutral, false, false
			}
			return BreakerFailure, true, u.Retryable && u.StatusCode != 401 && u.StatusCode != 403
		}
		return BreakerFailure, true, false
	default:
		return BreakerNeutral, false, false
	}
}

func (e *planExecutor) feedback(ctx context.Context, permit *Permit, id int64, outcome BreakerOutcome, report bool) time.Duration {
	start := time.Now()
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.opts.StateTimeout)
	defer cancel()
	requestID, _ := middleware.RequestIDFromContext(ctx)
	if err := e.breaker.Complete(cleanup, permit, outcome); err != nil {
		e.logger.WarnContext(cleanup, "breaker completion failed", "request_id", requestID, "channel_id", id)
	}
	if report {
		e.health.ReportRequestResult(cleanup, id, outcome == BreakerSuccess)
	}
	return time.Since(start)
}

func (e *planExecutor) logAttempt(ctx context.Context, c Candidate, model string, result proxy.ForwardResult, attempt int, elapsed, feedback time.Duration) {
	requestID, _ := middleware.RequestIDFromContext(ctx)
	// TODO(18.1): consume safe attempt summaries without recounting FirstByteMs (台账#25/#32).
	// TODO(18.2): consume request/attempt observations; no metrics protocol is owned here.
	kind := string(result.Kind)
	switch result.Kind {
	case proxy.ResultSuccess, proxy.ResultTTFBTimeout, proxy.ResultOverallTimeout, proxy.ResultStreamInterrupted, proxy.ResultUpstreamError, proxy.ResultClientCanceled, proxy.ResultWriteError:
	default:
		kind = "unknown"
	}
	e.logger.InfoContext(ctx, "upstream attempt finished", "request_id", requestID, "channel_id", int64(c.Channel.ID),
		"model", model, "result_kind", kind, "wrote_header", result.WroteHeader, "first_byte_ms", result.FirstByteMs,
		"chunks", result.Chunks, "attempt", attempt, "attempt_elapsed_ms", elapsed.Milliseconds(), "feedback_elapsed_ms", feedback.Milliseconds())
}
