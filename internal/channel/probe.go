package channel

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

var errProbeMapping = errors.New("channel: probe model is not mapped")

type probePicker struct {
	channels  store.ChannelRepo
	keys      store.KeyRepo
	masterKey []byte
}

func NewProbePicker(channels store.ChannelRepo, keys store.KeyRepo, masterKey []byte) (EndpointPicker, error) {
	if nilDependency(channels) || nilDependency(keys) || len(masterKey) != 32 {
		return nil, failure("invalid probe picker dependencies", nil)
	}
	return &probePicker{channels: channels, keys: keys, masterKey: append([]byte(nil), masterKey...)}, nil
}

func validateEndpointChannel(ch *store.Channel, model string) error {
	if ch == nil || !validID(ch.ID) || ch.Provider != "openai" || !validBaseURL(ch.BaseURL) || !validName(model) {
		return failure("invalid endpoint configuration", nil)
	}
	return nil
}

// assembleEndpoint is also the 14.2 reuse point. Each call owns its endpoint;
// no decrypted credential or input pointer is cached or modified.
func assembleEndpoint(ctx context.Context, ch *store.Channel, mapping *store.ChannelModel, key *store.ChannelKey, master []byte) (*provider.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, failure("endpoint assembly canceled", err)
	}
	if ch == nil || mapping == nil || key == nil || mapping.ChannelID != ch.ID || key.ChannelID != ch.ID ||
		!validID(key.ID) || !validName(mapping.UpstreamModel) {
		return nil, failure("invalid endpoint mapping or Key", nil)
	}
	raw, err := secret.Decrypt(key.ProviderKeyEnc, master)
	if err != nil {
		return nil, failure("endpoint Key decryption failed", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, failure("endpoint assembly canceled", err)
	}
	return &provider.Endpoint{ChannelID: int64(ch.ID), Provider: ch.Provider, BaseURL: ch.BaseURL, APIKey: string(raw), Model: mapping.UpstreamModel}, nil
}

func (p *probePicker) Pick(ctx context.Context, ch *store.Channel, model string) (*provider.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, failure("probe selection canceled", err)
	}
	if err := validateEndpointChannel(ch, model); err != nil {
		return nil, err
	}
	keys, err := p.keys.ListByChannel(ctx, int64(ch.ID))
	if err != nil {
		return nil, failure("probe Key lookup failed", err)
	}
	var selected *store.ChannelKey
	for i := range keys {
		if keys[i].Status == store.ChannelKeyStatusActive && (selected == nil || keys[i].ID < selected.ID) {
			selected = &keys[i]
		}
	}
	if selected == nil {
		return nil, failure("no active probe Key", provider.ErrNoCandidate)
	}
	mapping, err := p.channels.GetModel(ctx, int64(ch.ID), model)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, failure("probe model missing", errors.Join(errProbeMapping, err))
		}
		return nil, failure("probe mapping lookup failed", err)
	}
	if mapping == nil || mapping.Model != model {
		return nil, failure("invalid probe mapping", nil)
	}
	return assembleEndpoint(ctx, ch, mapping, selected, p.masterKey)
}

type connectivityTester struct {
	picker   EndpointPicker
	upstream provider.Provider
	timeout  time.Duration
}

func NewConnectivityTester(picker EndpointPicker, upstream provider.Provider, timeout time.Duration) (ConnectivityTester, error) {
	if nilDependency(picker) || nilDependency(upstream) || timeout < time.Millisecond {
		return nil, failure("invalid connectivity tester dependencies", nil)
	}
	return &connectivityTester{picker: picker, upstream: upstream, timeout: timeout}, nil
}

func (t *connectivityTester) Test(parent context.Context, ch *store.Channel, model string) (ProbeResult, error) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(parent, t.timeout)
	defer cancel()
	ep, err := t.picker.Pick(ctx, ch, model)
	if err != nil {
		return ProbeResult{}, failure("probe preparation failed", err)
	}
	if ep == nil || ep.Provider != t.upstream.Name() {
		return ProbeResult{}, failure("probe provider mismatch", nil)
	}
	if err := ctx.Err(); err != nil {
		return ProbeResult{}, failure("probe preparation canceled", err)
	}
	request := &provider.ChatRequest{Model: model, Stream: false,
		Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"ping"}`)},
		Extra:    map[string]json.RawMessage{"max_tokens": json.RawMessage("1")}}
	response, _, err := t.upstream.Chat(ctx, ep, request)
	if parent.Err() != nil {
		return ProbeResult{}, failure("probe parent canceled", errors.Join(parent.Err(), err))
	}
	result := ProbeResult{LatencyMS: time.Since(start).Milliseconds(), Reason: "upstream_error"}
	if ctx.Err() != nil || errors.Is(err, context.DeadlineExceeded) {
		result.Reason = "timeout"
	}
	if err != nil {
		var upstream *provider.UpstreamError
		if errors.As(err, &upstream) {
			result.StatusCode = upstream.StatusCode
		}
		return result, nil
	}
	if ctx.Err() == nil && response != nil {
		result.OK, result.StatusCode, result.Reason = true, 200, "success"
	}
	return result, nil
}
