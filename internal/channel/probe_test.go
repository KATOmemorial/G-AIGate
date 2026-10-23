package channel

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/provider"
	"gaigate/internal/provider/openai"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

type pickerFunc func(context.Context, *store.Channel, string) (*provider.Endpoint, error)

func (f pickerFunc) Pick(c context.Context, ch *store.Channel, m string) (*provider.Endpoint, error) {
	return f(c, ch, m)
}

type upstreamFake struct {
	provider.Provider
	chat func(context.Context, *provider.Endpoint, *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error)
}

func (*upstreamFake) Name() string { return "openai" }
func (u *upstreamFake) Chat(c context.Context, e *provider.Endpoint, r *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
	return u.chat(c, e, r)
}

func TestProbePickerConfigurationSelectionAndIsolation(t *testing.T) {
	master := []byte(strings.Repeat("p", 32))
	enc, err := secret.Encrypt([]byte("probe-private"), master)
	require.NoError(t, err)
	mapping := &store.ChannelModel{ChannelID: 1, Model: "a", UpstreamModel: "mapped-a"}
	rows := []store.ChannelKey{{ID: 7, ChannelID: 1, Status: 1, ProviderKeyEnc: []byte("bad-unselected")}, {ID: 1, ChannelID: 1, Status: 0}, {ID: 2, ChannelID: 1, Status: 1, ProviderKeyEnc: enc}}
	ch := &store.Channel{ID: 1, Provider: "openai", BaseURL: "http://localhost/prefix", Enabled: false}
	c, k := &channelFake{}, &keyFake{}
	ctx := context.WithValue(context.Background(), testContextKey{}, "probe")
	k.list = func(got context.Context, id int64) ([]store.ChannelKey, error) {
		assert.Same(t, ctx, got)
		assert.Equal(t, int64(1), id)
		return rows, nil
	}
	c.ChannelRepo = &mappingRepoFake{get: func(got context.Context, id int64, model string) (*store.ChannelModel, error) {
		assert.Same(t, ctx, got)
		assert.Equal(t, "a", model)
		return mapping, nil
	}}
	p, err := NewProbePicker(c, k, master)
	require.NoError(t, err)
	master[0] = 'X'
	ep, err := p.Pick(ctx, ch, "a")
	require.NoError(t, err)
	assert.Equal(t, &provider.Endpoint{ChannelID: 1, Provider: "openai", BaseURL: ch.BaseURL, APIKey: "probe-private", Model: "mapped-a"}, ep)
	ep.Model = "caller"
	ep.APIKey = "caller"
	again, err := p.Pick(ctx, ch, "a")
	require.NoError(t, err)
	assert.Equal(t, "mapped-a", again.Model)
	assert.Equal(t, "probe-private", again.APIKey)
	assert.Equal(t, "mapped-a", mapping.UpstreamModel)
	assert.Equal(t, enc, rows[2].ProviderKeyEnc)
	rows[2].ProviderKeyEnc = []byte("corrupt-selected")
	_, err = p.Pick(ctx, ch, "a")
	require.Error(t, err)
	assert.NotErrorIs(t, err, provider.ErrNoCandidate)
	assert.NotContains(t, err.Error(), "corrupt-selected")
	rows = nil
	_, err = p.Pick(ctx, ch, "a")
	assert.ErrorIs(t, err, provider.ErrNoCandidate)
	for _, invalid := range []*store.Channel{nil, {ID: 0}, {ID: 1, Provider: "mock", BaseURL: "http://localhost"}, {ID: 1, Provider: "openai", BaseURL: "file:///private"}} {
		_, err = p.Pick(ctx, invalid, "a")
		assert.Error(t, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = p.Pick(cancelled, ch, "a")
	assert.ErrorIs(t, err, context.Canceled)
}

type mappingRepoFake struct {
	store.ChannelRepo
	get func(context.Context, int64, string) (*store.ChannelModel, error)
}

func (f *mappingRepoFake) GetModel(c context.Context, id int64, m string) (*store.ChannelModel, error) {
	return f.get(c, id, m)
}

func TestProbeConstructorsAndPreparationFailures(t *testing.T) {
	var nilKeys *keyFake
	var nilChannels *channelFake
	for _, deps := range []struct {
		c store.ChannelRepo
		k store.KeyRepo
		n int
	}{{nil, &keyFake{}, 32}, {nilChannels, &keyFake{}, 32}, {&channelFake{}, nilKeys, 32}, {&channelFake{}, &keyFake{}, 31}, {&channelFake{}, &keyFake{}, 33}} {
		got, err := NewProbePicker(deps.c, deps.k, make([]byte, deps.n))
		assert.Error(t, err)
		assert.Nil(t, got)
	}
	var nilUpstream *upstreamFake
	var nilPicker pickerFunc
	for _, deps := range []struct {
		p       EndpointPicker
		u       provider.Provider
		timeout time.Duration
	}{{nil, &upstreamFake{}, time.Second}, {nilPicker, &upstreamFake{}, time.Second}, {pickerFunc(func(context.Context, *store.Channel, string) (*provider.Endpoint, error) { return nil, nil }), nilUpstream, time.Second}, {nilPicker, &upstreamFake{}, 0}} {
		got, err := NewConnectivityTester(deps.p, deps.u, deps.timeout)
		assert.Error(t, err)
		assert.Nil(t, got)
	}
	for _, cause := range []error{store.ErrNotFound, errors.New("SQL private-cause"), provider.ErrNoCandidate, context.Canceled} {
		p := pickerFunc(func(context.Context, *store.Channel, string) (*provider.Endpoint, error) { return nil, cause })
		tester, err := NewConnectivityTester(p, &upstreamFake{chat: func(context.Context, *provider.Endpoint, *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
			t.Fatal("preparation failure called provider")
			return nil, nil, nil
		}}, time.Second)
		require.NoError(t, err)
		result, err := tester.Test(context.Background(), nil, "a")
		assert.ErrorIs(t, err, cause)
		assert.Equal(t, ProbeResult{}, result)
		assert.NotContains(t, err.Error(), "private")
	}
	c := &mappingRepoFake{get: func(context.Context, int64, string) (*store.ChannelModel, error) { return nil, store.ErrNotFound }}
	k := &keyFake{list: func(context.Context, int64) ([]store.ChannelKey, error) {
		return []store.ChannelKey{{ID: 1, ChannelID: 1, Status: 1}}, nil
	}}
	p, err := NewProbePicker(c, k, make([]byte, 32))
	require.NoError(t, err)
	_, err = p.Pick(context.Background(), &store.Channel{ID: 1, Provider: "openai", BaseURL: "http://localhost"}, "a")
	assert.ErrorIs(t, err, errProbeMapping)
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestConnectivityProbeRealHTTP(t *testing.T) {
	for _, mode := range []string{"normal", "503", "429", "refuse", "slow"} {
		t.Run(mode, func(t *testing.T) {
			observed := make(chan map[string]json.RawMessage, 1)
			ended := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(ended)
				assert.Equal(t, "/prefix/v1/chat/completions", r.URL.Path)
				assert.Equal(t, "Bearer probe-private", r.Header.Get("Authorization"))
				assert.Empty(t, r.Header.Get("X-Mock-Scenario"))
				var fields map[string]json.RawMessage
				require.NoError(t, json.NewDecoder(r.Body).Decode(&fields))
				observed <- fields
				w.Header().Set("Content-Type", "application/json")
				switch mode {
				case "503", "429":
					if mode == "503" {
						w.WriteHeader(503)
					} else {
						w.WriteHeader(429)
					}
					_, _ = io.WriteString(w, `{"error":{"message":"private-body"}}`)
				case "refuse":
					conn, _, err := w.(http.Hijacker).Hijack()
					require.NoError(t, err)
					_ = conn.Close()
				case "slow":
					<-r.Context().Done()
				default:
					_, _ = io.WriteString(w, `{"id":"probe","choices":[]}`)
				}
			}))
			defer srv.Close()
			master := make([]byte, 32)
			enc, err := secret.Encrypt([]byte("probe-private"), master)
			require.NoError(t, err)
			c := &mappingRepoFake{get: func(context.Context, int64, string) (*store.ChannelModel, error) {
				return &store.ChannelModel{ChannelID: 1, Model: "a", UpstreamModel: "mapped-a"}, nil
			}}
			k := &keyFake{list: func(context.Context, int64) ([]store.ChannelKey, error) {
				return []store.ChannelKey{{ID: 1, ChannelID: 1, Status: 1, Weight: 7, ProviderKeyEnc: enc}}, nil
			}}
			picker, err := NewProbePicker(c, k, master)
			require.NoError(t, err)
			upstream, err := openai.New(&http.Client{}, 1)
			require.NoError(t, err)
			tester, err := NewConnectivityTester(picker, upstream, 120*time.Millisecond)
			require.NoError(t, err)
			ch := &store.Channel{ID: 1, Provider: "openai", BaseURL: srv.URL + "/prefix", Enabled: false, HealthStatus: "cooling", Weight: 9}
			before := *ch
			result, err := tester.Test(context.Background(), ch, "a")
			require.NoError(t, err)
			fields := <-observed
			assert.JSONEq(t, `"mapped-a"`, string(fields["model"]))
			assert.JSONEq(t, `false`, string(fields["stream"]))
			assert.JSONEq(t, `1`, string(fields["max_tokens"]))
			assert.JSONEq(t, `[{"role":"user","content":"ping"}]`, string(fields["messages"]))
			assert.Len(t, fields, 4)
			assert.Equal(t, before, *ch)
			assert.Equal(t, int32(7), func() int32 { rows, _ := k.list(context.Background(), 1); return rows[0].Weight }())
			assert.Equal(t, mode == "normal", result.OK)
			switch mode {
			case "normal":
				assert.Equal(t, "success", result.Reason)
				assert.Equal(t, 200, result.StatusCode)
			case "slow":
				assert.Equal(t, "timeout", result.Reason)
				assert.GreaterOrEqual(t, result.LatencyMS, int64(100))
				assert.Less(t, result.LatencyMS, int64(1000))
			case "503":
				assert.Equal(t, 503, result.StatusCode)
			case "429":
				assert.Equal(t, 429, result.StatusCode)
			case "refuse":
				assert.Zero(t, result.StatusCode)
			}
			raw, err := json.Marshal(result)
			require.NoError(t, err)
			assert.NotContains(t, string(raw), "private")
			select {
			case <-ended:
			case <-time.After(time.Second):
				t.Fatal("probe HTTP owner did not finish")
			}
		})
	}
}

func TestConnectivityProbeOneBudgetAndParentCancellation(t *testing.T) {
	var shared context.Context
	picker := pickerFunc(func(ctx context.Context, _ *store.Channel, _ string) (*provider.Endpoint, error) {
		shared = ctx
		timer := time.NewTimer(60 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return &provider.Endpoint{Provider: "openai"}, nil
	})
	upstream := &upstreamFake{chat: func(ctx context.Context, _ *provider.Endpoint, _ *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
		assert.Same(t, shared, ctx, "picker and Chat share one total budget")
		deadline, _ := ctx.Deadline()
		assert.Less(t, time.Until(deadline), 120*time.Millisecond)
		<-ctx.Done()
		return nil, nil, &provider.UpstreamError{Cause: ctx.Err()}
	}}
	tester, err := NewConnectivityTester(picker, upstream, 150*time.Millisecond)
	require.NoError(t, err)
	start := time.Now()
	result, err := tester.Test(context.Background(), nil, "a")
	require.NoError(t, err)
	assert.Equal(t, "timeout", result.Reason)
	assert.GreaterOrEqual(t, result.LatencyMS, int64(130))
	assert.Less(t, time.Since(start), time.Second)
	entered, ended := make(chan struct{}), make(chan struct{})
	picker = func(ctx context.Context, _ *store.Channel, _ string) (*provider.Endpoint, error) {
		return &provider.Endpoint{Provider: "openai"}, nil
	}
	upstream.chat = func(ctx context.Context, _ *provider.Endpoint, _ *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
		close(entered)
		defer close(ended)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	tester, err = NewConnectivityTester(picker, upstream, time.Second)
	require.NoError(t, err)
	parent, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := tester.Test(parent, nil, "a"); done <- err }()
	<-entered
	cancel()
	assert.ErrorIs(t, <-done, context.Canceled)
	<-ended
	parent, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	upstream.chat = func(ctx context.Context, _ *provider.Endpoint, _ *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
		deadline, _ := ctx.Deadline()
		pd, _ := parent.Deadline()
		assert.Equal(t, pd, deadline)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	_, err = tester.Test(parent, nil, "a")
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}
