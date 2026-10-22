package bootstrap

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/access"
	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

type modelStub struct {
	store.ModelRepo
	resolve func(context.Context, string) (*store.ModelAlias, error)
	calls   int
}

func (s *modelStub) Resolve(ctx context.Context, name string) (*store.ModelAlias, error) {
	s.calls++
	return s.resolve(ctx, name)
}

type channelStub struct {
	store.ChannelRepo
	list            func(context.Context, string) ([]store.Channel, error)
	mapping         func(context.Context, int64, string) (*store.ChannelModel, error)
	lists, mappings int
}

func (s *channelStub) ListActiveByModel(ctx context.Context, name string) ([]store.Channel, error) {
	s.lists++
	return s.list(ctx, name)
}
func (s *channelStub) GetModel(ctx context.Context, id int64, name string) (*store.ChannelModel, error) {
	s.mappings++
	return s.mapping(ctx, id, name)
}

type keyStub struct {
	store.KeyRepo
	list     func(context.Context, int64) ([]store.ChannelKey, error)
	channels []int64
}

func (s *keyStub) ListByChannel(ctx context.Context, id int64) ([]store.ChannelKey, error) {
	s.channels = append(s.channels, id)
	return s.list(ctx, id)
}

type providerStub struct {
	chat   func(context.Context, *provider.Endpoint, *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error)
	stream func(context.Context, *provider.Endpoint, *provider.ChatRequest) (provider.Stream, error)
}

func (*providerStub) Name() string { return "openai" }
func (s *providerStub) Chat(ctx context.Context, ep *provider.Endpoint, req *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
	return s.chat(ctx, ep, req)
}
func (s *providerStub) ChatStream(ctx context.Context, ep *provider.Endpoint, req *provider.ChatRequest) (provider.Stream, error) {
	return s.stream(ctx, ep, req)
}

type forwarderStub struct {
	stream func(context.Context, http.ResponseWriter, proxy.StreamOpener, proxy.ForwardOptions, func(provider.StreamEvent)) (proxy.ForwardResult, error)
	chat   func(context.Context, http.ResponseWriter, proxy.ResponseOpener, proxy.ForwardOptions) (proxy.ForwardResult, error)
	calls  int
}

func (s *forwarderStub) Forward(ctx context.Context, w http.ResponseWriter, open proxy.StreamOpener, opts proxy.ForwardOptions, onChunk func(provider.StreamEvent)) (proxy.ForwardResult, error) {
	s.calls++
	return s.stream(ctx, w, open, opts, onChunk)
}
func (s *forwarderStub) ForwardNonStream(ctx context.Context, w http.ResponseWriter, open proxy.ResponseOpener, opts proxy.ForwardOptions) (proxy.ForwardResult, error) {
	s.calls++
	return s.chat(ctx, w, open, opts)
}

type harness struct {
	models    *modelStub
	channels  *channelStub
	keys      *keyStub
	upstream  *providerStub
	forwarder *forwarderStub
	master    []byte
	enc       []byte
	logs      bytes.Buffer
	opts      proxy.ForwardOptions
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{master: bytes.Repeat([]byte{9}, 32), opts: proxy.ForwardOptions{TTFBTimeout: 100 * time.Millisecond, OverallTimeout: time.Second}}
	var err error
	h.enc, err = secret.Encrypt([]byte("fixture-upstream-credential"), h.master)
	require.NoError(t, err)
	h.models = &modelStub{resolve: func(context.Context, string) (*store.ModelAlias, error) {
		id := uint64(9)
		return &store.ModelAlias{Name: "alias", DefaultChannelID: &id, FallbackIDs: "[9]"}, nil
	}}
	h.channels = &channelStub{list: func(context.Context, string) ([]store.Channel, error) {
		return []store.Channel{
			{ID: 9, Priority: 5, Provider: "openai", Enabled: true}, {ID: 2, Priority: 10, Provider: "openai", Enabled: true},
			{ID: 1, Priority: 10, Provider: "openai", BaseURL: "http://fixture.invalid/v1", Enabled: true, HealthStatus: "disabled"},
		}, nil
	}, mapping: func(_ context.Context, id int64, name string) (*store.ChannelModel, error) {
		return &store.ChannelModel{ChannelID: uint64(id), Model: name, UpstreamModel: "mapped"}, nil
	}}
	h.keys = &keyStub{list: func(context.Context, int64) ([]store.ChannelKey, error) {
		return []store.ChannelKey{
			{ID: 1, Status: store.ChannelKeyStatusRevoked, ProviderKeyEnc: []byte("bad")},
			{ID: 7, Status: store.ChannelKeyStatusActive, ProviderKeyEnc: []byte("bad")},
			{ID: 3, Status: store.ChannelKeyStatusActive, ProviderKeyEnc: h.enc},
		}, nil
	}}
	h.upstream = &providerStub{}
	h.forwarder = &forwarderStub{chat: func(context.Context, http.ResponseWriter, proxy.ResponseOpener, proxy.ForwardOptions) (proxy.ForwardResult, error) {
		return proxy.ForwardResult{Kind: proxy.ResultSuccess}, nil
	}}
	return h
}

func (h *harness) build(t *testing.T) access.ChatExecutor {
	t.Helper()
	e, err := NewExecutor(h.models, h.channels, h.keys, h.upstream, h.forwarder, h.master, h.opts, slog.New(slog.NewJSONHandler(&h.logs, nil)))
	require.NoError(t, err)
	return e
}

func testRequest(stream bool) *provider.ChatRequest {
	return &provider.ChatRequest{Model: "alias", Stream: stream, Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":[{"type":"text","text":"hi"}]}`)}, Extra: map[string]json.RawMessage{"stream_options": json.RawMessage(`{"include_usage":false,"custom":1}`)}}
}

func TestExecutorSelectionEndpointAndOpenerContext(t *testing.T) {
	type marker struct{}
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonstream", true: "stream"}[stream], func(t *testing.T) {
			h := newHarness(t)
			req := testRequest(stream)
			before, err := json.Marshal(req)
			require.NoError(t, err)
			parent := middleware.WithRequestID(context.Background(), "request-fixture")
			var openCtx context.Context
			providerCalls := 0
			check := func(ctx context.Context, ep *provider.Endpoint, got *provider.ChatRequest) {
				providerCalls++
				assert.Same(t, openCtx, ctx)
				assert.Equal(t, "budget", ctx.Value(marker{}))
				assert.Same(t, req, got)
				assert.Equal(t, &provider.Endpoint{ChannelID: 1, Provider: "openai", BaseURL: "http://fixture.invalid/v1", APIKey: "fixture-upstream-credential", Model: "mapped"}, ep)
				assert.EqualValues(t, 1, middleware.ChannelIDFromContext(ctx))
			}
			h.upstream.chat = func(ctx context.Context, ep *provider.Endpoint, got *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
				check(ctx, ep, got)
				return &provider.ChatResponse{Body: json.RawMessage(`{}`)}, nil, nil
			}
			h.upstream.stream = func(ctx context.Context, ep *provider.Endpoint, got *provider.ChatRequest) (provider.Stream, error) {
				check(ctx, ep, got)
				return nil, nil
			}
			prepare := func(ctx context.Context, opts proxy.ForwardOptions) context.CancelFunc {
				assert.Equal(t, h.opts, opts)
				assert.EqualValues(t, 1, middleware.ChannelIDFromContext(ctx))
				var cancel context.CancelFunc
				openCtx, cancel = context.WithTimeout(context.WithValue(ctx, marker{}, "budget"), 20*time.Millisecond)
				return cancel
			}
			h.forwarder.chat = func(ctx context.Context, _ http.ResponseWriter, open proxy.ResponseOpener, opts proxy.ForwardOptions) (proxy.ForwardResult, error) {
				cancel := prepare(ctx, opts)
				defer cancel()
				_, _, err := open(openCtx)
				return proxy.ForwardResult{Kind: proxy.ResultSuccess}, err
			}
			h.forwarder.stream = func(ctx context.Context, _ http.ResponseWriter, open proxy.StreamOpener, opts proxy.ForwardOptions, onChunk func(provider.StreamEvent)) (proxy.ForwardResult, error) {
				cancel := prepare(ctx, opts)
				defer cancel()
				assert.Nil(t, onChunk)
				_, err := open(openCtx)
				return proxy.ForwardResult{Kind: proxy.ResultSuccess}, err
			}
			e := h.build(t)
			clear(h.master) // The startup caller's mutable slice cannot change execution.
			require.NoError(t, e.Execute(parent, httptest.NewRecorder(), &access.Principal{UserID: 88}, req))
			assert.Equal(t, 1, providerCalls)
			assert.Equal(t, []int64{1}, h.keys.channels)
			assert.Equal(t, 1, h.channels.mappings)
			assert.Zero(t, middleware.ChannelIDFromContext(parent), "standard context values remain directional")
			after, err := json.Marshal(req)
			require.NoError(t, err)
			assert.Equal(t, before, after)
			assert.Contains(t, h.logs.String(), `"result_kind":"success"`)
			assert.NotContains(t, h.logs.String(), "fixture-upstream-credential")
			assert.NotContains(t, h.logs.String(), "fixture.invalid")
		})
	}
}

func TestExecutorLookupFailuresNeverTryBackup(t *testing.T) {
	private := errors.New("private database cause")
	for _, tc := range []struct {
		name   string
		change func(*harness)
		want   error
		lists  int
		keys   bool
	}{
		{"unknown alias", func(h *harness) {
			h.models.resolve = func(context.Context, string) (*store.ModelAlias, error) { return nil, store.ErrNotFound }
		}, access.ErrModelNotFound, 0, false},
		{"alias SQL", func(h *harness) {
			h.models.resolve = func(context.Context, string) (*store.ModelAlias, error) { return nil, private }
		}, private, 0, false},
		{"nil alias", func(h *harness) {
			h.models.resolve = func(context.Context, string) (*store.ModelAlias, error) { return nil, nil }
		}, nil, 0, false},
		{"channel SQL", func(h *harness) {
			h.channels.list = func(context.Context, string) ([]store.Channel, error) { return nil, private }
		}, private, 1, false},
		{"no channel", func(h *harness) {
			h.channels.list = func(context.Context, string) ([]store.Channel, error) { return nil, nil }
		}, provider.ErrNoCandidate, 1, false},
		{"unsupported provider", func(h *harness) {
			h.channels.list = func(context.Context, string) ([]store.Channel, error) {
				return []store.Channel{{ID: 1, Provider: "unsupported"}}, nil
			}
		}, nil, 1, false},
		{"no active key", func(h *harness) {
			h.keys.list = func(context.Context, int64) ([]store.ChannelKey, error) {
				return []store.ChannelKey{{ID: 1, Status: 0}}, nil
			}
		}, provider.ErrNoCandidate, 1, true},
		{"key SQL", func(h *harness) {
			h.keys.list = func(context.Context, int64) ([]store.ChannelKey, error) { return nil, private }
		}, private, 1, true},
		{"missing mapping", func(h *harness) {
			h.channels.mapping = func(context.Context, int64, string) (*store.ChannelModel, error) { return nil, store.ErrNotFound }
		}, store.ErrNotFound, 1, true},
		{"mapping SQL", func(h *harness) {
			h.channels.mapping = func(context.Context, int64, string) (*store.ChannelModel, error) { return nil, private }
		}, private, 1, true},
		{"empty mapping", func(h *harness) {
			h.channels.mapping = func(context.Context, int64, string) (*store.ChannelModel, error) { return &store.ChannelModel{}, nil }
		}, nil, 1, true},
		{"nil mapping", func(h *harness) {
			h.channels.mapping = func(context.Context, int64, string) (*store.ChannelModel, error) { return nil, nil }
		}, nil, 1, true},
		{"corrupt ciphertext", func(h *harness) { h.enc = []byte("invalid ciphertext") }, nil, 1, true},
		{"empty upstream key", func(h *harness) { var err error; h.enc, err = secret.Encrypt(nil, h.master); require.NoError(t, err) }, nil, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			tc.change(h)
			err := h.build(t).Execute(context.Background(), httptest.NewRecorder(), nil, testRequest(false))
			require.Error(t, err)
			if tc.want != nil {
				assert.ErrorIs(t, err, tc.want)
			}
			assert.NotContains(t, err.Error(), private.Error())
			assert.Equal(t, tc.lists, h.channels.lists)
			if tc.keys {
				assert.Equal(t, []int64{1}, h.keys.channels)
			} else {
				assert.Empty(t, h.keys.channels)
			}
			assert.Zero(t, h.forwarder.calls, "no forwarding or fallback on a lookup/configuration failure")
			if tc.name == "missing mapping" {
				assert.NotErrorIs(t, err, access.ErrModelNotFound)
			}
		})
	}
}

func TestExecutorForwardResultClassificationAndPrivacy(t *testing.T) {
	cause := errors.Join(errors.New("private cause with credentials"), context.Canceled, context.DeadlineExceeded)
	for _, kind := range []proxy.ResultKind{proxy.ResultSuccess, proxy.ResultUpstreamError, proxy.ResultStreamInterrupted, proxy.ResultTTFBTimeout, proxy.ResultOverallTimeout, proxy.ResultClientCanceled, proxy.ResultWriteError, "untrusted-kind-credential"} {
		for _, committed := range []bool{false, true} {
			for _, stream := range []bool{false, true} {
				t.Run(string(kind)+map[bool]string{false: "/pre", true: "/committed"}[committed]+map[bool]string{false: "/chat", true: "/stream"}[stream], func(t *testing.T) {
					h := newHarness(t)
					result := proxy.ForwardResult{Kind: kind, WroteHeader: committed, FirstByteMs: 7, Chunks: 3}
					h.forwarder.chat = func(context.Context, http.ResponseWriter, proxy.ResponseOpener, proxy.ForwardOptions) (proxy.ForwardResult, error) {
						return result, cause
					}
					h.forwarder.stream = func(context.Context, http.ResponseWriter, proxy.StreamOpener, proxy.ForwardOptions, func(provider.StreamEvent)) (proxy.ForwardResult, error) {
						return result, cause
					}
					err := h.build(t).Execute(middleware.WithRequestID(context.Background(), "request-fixture"), httptest.NewRecorder(), nil, testRequest(stream))
					var execution *access.ExecutionError
					require.ErrorAs(t, err, &execution)
					assert.ErrorIs(t, err, context.Canceled)
					assert.ErrorIs(t, err, context.DeadlineExceeded)
					assert.Same(t, cause, execution.Cause)
					assert.Equal(t, committed, execution.WroteHeader)
					assert.Equal(t, kind == proxy.ResultTTFBTimeout || kind == proxy.ResultOverallTimeout, execution.TimedOut)
					assert.Equal(t, []int64{1}, h.keys.channels)
					assert.Equal(t, 1, h.forwarder.calls)
					assert.NotContains(t, err.Error(), "credentials")
					assert.NotContains(t, h.logs.String(), "credentials")
					assert.NotContains(t, h.logs.String(), "untrusted-kind-credential")
					assert.Contains(t, h.logs.String(), `"result_kind":"`+safeKind(kind)+`"`)
					assert.Contains(t, h.logs.String(), `"first_byte_ms":7`)
				})
			}
		}
	}
}

func TestExecutorConstructionAndAllowAll(t *testing.T) {
	h := newHarness(t)
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	for _, test := range []struct {
		master []byte
		opts   proxy.ForwardOptions
	}{{nil, h.opts}, {h.master, proxy.ForwardOptions{}}, {h.master, proxy.ForwardOptions{TTFBTimeout: time.Second, OverallTimeout: -1}}} {
		_, err := NewExecutor(h.models, h.channels, h.keys, h.upstream, h.forwarder, test.master, test.opts, logger)
		require.Error(t, err)
	}
	_, err := NewExecutor((*modelStub)(nil), h.channels, h.keys, h.upstream, h.forwarder, h.master, h.opts, logger)
	require.Error(t, err)
	_, err = NewExecutor(h.models, h.channels, h.keys, h.upstream, h.forwarder, h.master, h.opts, nil)
	require.Error(t, err)
	e := h.build(t)
	require.Error(t, e.Execute(context.Background(), httptest.NewRecorder(), nil, nil))
	assert.Zero(t, h.models.calls)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NoError(t, (AllowAll{}).Precheck(ctx, nil), "B3 has no quota, limiter or identity side effects")
	assert.False(t, strings.Contains(h.logs.String(), "fixture-upstream-credential"))
}
