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
	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

// AllowAll is the explicit B3 precheck stub.
// TODO(17.2): replace at assembly with real quota checks/settlement (台账#22).
type AllowAll struct{}

func (AllowAll) Precheck(context.Context, *access.Principal) error { return nil }

type executor struct {
	models    store.ModelRepo
	channels  store.ChannelRepo
	keys      store.KeyRepo
	upstream  provider.Provider
	forwarder proxy.Forwarder
	masterKey []byte
	opts      proxy.ForwardOptions
	logger    *slog.Logger
}

// NewExecutor receives decoded key material and proxy options from assembly;
// no business package reads global configuration or environment variables.
func NewExecutor(models store.ModelRepo, channels store.ChannelRepo, keys store.KeyRepo,
	upstream provider.Provider, forwarder proxy.Forwarder, masterKey []byte,
	opts proxy.ForwardOptions, logger *slog.Logger) (access.ChatExecutor, error) {
	for _, dependency := range []any{models, channels, keys, upstream, forwarder, logger} {
		if nilDependency(dependency) {
			return nil, errors.New("bootstrap: missing dependency")
		}
	}
	if len(masterKey) != 32 || opts.TTFBTimeout <= 0 || opts.OverallTimeout <= 0 {
		return nil, errors.New("bootstrap: invalid key or forwarding options")
	}
	return &executor{models: models, channels: channels, keys: keys, upstream: upstream,
		forwarder: forwarder, masterKey: append([]byte(nil), masterKey...), opts: opts, logger: logger}, nil
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
	// TODO(14.2): replace first enabled Key with the owned picker (台账#21).
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
	keys, err := e.keys.ListByChannel(ctx, int64(ch.ID))
	if err != nil {
		return failure("bootstrap: key lookup failed", err)
	}
	var selected *store.ChannelKey
	for i := range keys {
		if keys[i].Status == store.ChannelKeyStatusActive && (selected == nil || keys[i].ID < selected.ID) {
			selected = &keys[i]
		}
	}
	if selected == nil {
		return failure("bootstrap: no enabled key", provider.ErrNoCandidate)
	}
	mapping, err := e.channels.GetModel(ctx, int64(ch.ID), req.Model)
	if err != nil {
		return failure("bootstrap: model mapping lookup failed", err)
	}
	if mapping == nil || mapping.UpstreamModel == "" {
		return failure("bootstrap: invalid model mapping", nil)
	}
	key, err := secret.Decrypt(selected.ProviderKeyEnc, e.masterKey)
	if err != nil {
		return failure("bootstrap: key decryption failed", err)
	}
	if len(key) == 0 {
		return failure("bootstrap: empty upstream key", nil)
	}
	endpoint := &provider.Endpoint{ChannelID: int64(ch.ID), Provider: ch.Provider, BaseURL: ch.BaseURL,
		APIKey: string(key), Model: mapping.UpstreamModel}
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
