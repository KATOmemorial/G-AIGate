// Package bootstrap contains temporary B3 orchestration, consumed only by cmd.
package bootstrap

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"reflect"

	"gaigate/internal/access"
	"gaigate/internal/channel"
	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"
	"gaigate/internal/store"
)

// AllowAll is the explicit B3 precheck stub.
// TODO(17.2): replace at assembly with real quota checks/settlement (台账#22).
type AllowAll struct{}

func (AllowAll) Precheck(context.Context, *access.Principal) error { return nil }

type executor struct {
	models    store.ModelRepo
	channels  store.ChannelRepo
	picker    channel.EndpointPicker
	upstream  provider.Provider
	forwarder proxy.Forwarder
	opts      proxy.ForwardOptions
	logger    *slog.Logger
}

// NewExecutor receives the endpoint picker and proxy options from assembly.
// Endpoint selection, model mapping and decryption belong to channel (14.2).
func NewExecutor(models store.ModelRepo, channels store.ChannelRepo, picker channel.EndpointPicker,
	upstream provider.Provider, forwarder proxy.Forwarder,
	opts proxy.ForwardOptions, logger *slog.Logger) (access.ChatExecutor, error) {
	for _, dependency := range []any{models, channels, picker, upstream, forwarder, logger} {
		if nilDependency(dependency) {
			return nil, errors.New("bootstrap: missing dependency")
		}
	}
	if opts.TTFBTimeout <= 0 || opts.OverallTimeout <= 0 {
		return nil, errors.New("bootstrap: invalid forwarding options")
	}
	return &executor{models: models, channels: channels, picker: picker, upstream: upstream,
		forwarder: forwarder, opts: opts, logger: logger}, nil
}

type stageError struct {
	message string
	cause   error
}

func (e *stageError) Error() string             { return e.message }
func (e *stageError) Unwrap() error             { return e.cause }
func failure(message string, cause error) error { return &stageError{message: message, cause: cause} }

func (e *executor) Execute(ctx context.Context, w http.ResponseWriter, _ *access.Principal, req *provider.ChatRequest) error {
	if req == nil {
		return failure("bootstrap: missing request", nil)
	}
	alias, err := e.models.Resolve(ctx, req.Model)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return failure("bootstrap: unknown model alias", errors.Join(access.ErrModelNotFound, err))
		}
		return failure("bootstrap: model lookup failed", err)
	}
	if alias == nil {
		return failure("bootstrap: invalid model alias", nil)
	}
	channels, err := e.channels.ListActiveByModel(ctx, req.Model)
	if err != nil {
		return failure("bootstrap: channel lookup failed", err)
	}
	if len(channels) == 0 {
		return failure("bootstrap: no channel", provider.ErrNoCandidate)
	}
	// TODO(16.1): replace priority/id selection with routing (台账#21).
	// TODO(16.4): replace this single attempt; B3 never retries/falls back (台账#21).
	ch := channels[0]
	for _, candidate := range channels[1:] {
		if candidate.Priority > ch.Priority || candidate.Priority == ch.Priority && candidate.ID < ch.ID {
			ch = candidate
		}
	}
	if ch.ID == 0 || ch.ID > math.MaxInt64 || ch.Provider != e.upstream.Name() {
		return failure("bootstrap: invalid channel configuration", nil)
	}
	ctx = middleware.WithChannelID(ctx, int64(ch.ID))
	endpoint, err := e.picker.Pick(ctx, &ch, req.Model)
	if err != nil {
		return failure("bootstrap: endpoint selection failed", err)
	}
	if endpoint == nil {
		return failure("bootstrap: missing endpoint", nil)
	}
	var result proxy.ForwardResult
	if req.Stream {
		result, err = e.forwarder.Forward(ctx, w, func(openCtx context.Context) (provider.Stream, error) {
			return e.upstream.ChatStream(openCtx, endpoint, req)
		}, e.opts, nil)
	} else {
		result, err = e.forwarder.ForwardNonStream(ctx, w, func(openCtx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
			return e.upstream.Chat(openCtx, endpoint, req)
		}, e.opts)
	}
	requestID, _ := middleware.RequestIDFromContext(ctx)
	level := slog.LevelInfo
	if err != nil {
		level = slog.LevelError
	}
	// TODO(18.1): consume result timing/kind without recounting opener time (台账#25).
	e.logger.Log(ctx, level, "upstream attempt finished", "request_id", requestID, "channel_id", endpoint.ChannelID,
		"result_kind", safeKind(result.Kind), "wrote_header", result.WroteHeader,
		"first_byte_ms", result.FirstByteMs, "chunks", result.Chunks)
	if err != nil {
		return &access.ExecutionError{Cause: err, WroteHeader: result.WroteHeader,
			TimedOut: result.Kind == proxy.ResultTTFBTimeout || result.Kind == proxy.ResultOverallTimeout}
	}
	return nil
}

func safeKind(kind proxy.ResultKind) string {
	switch kind {
	case proxy.ResultSuccess, proxy.ResultUpstreamError, proxy.ResultStreamInterrupted, proxy.ResultTTFBTimeout,
		proxy.ResultOverallTimeout, proxy.ResultClientCanceled, proxy.ResultWriteError:
		return string(kind)
	default:
		return "unknown"
	}
}

func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflect.ValueOf(value).IsNil()
	default:
		return false
	}
}
