package meter

import (
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"

	"gaigate/internal/provider"
	"github.com/stretchr/testify/require"
)

func request() *provider.ChatRequest {
	return &provider.ChatRequest{Model: "test", Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"中🙂"}`)}}
}

func newTestCollector(t *testing.T, c float64) Collector {
	t.Helper()
	v, err := NewCollector(request(), c)
	require.NoError(t, err)
	return v
}

func chunk(raw string) provider.StreamEvent {
	return provider.StreamEvent{Kind: provider.Chunk, Raw: []byte(raw)}
}

func TestUsageSnapshots(t *testing.T) {
	for _, u := range []provider.Usage{{}, {InputTokens: 7, OutputTokens: 3}, {InputTokens: math.MaxInt64}} {
		t.Run(strings.ReplaceAll(strings.TrimSpace(string(mustJSON(t, u))), " ", ""), func(t *testing.T) {
			c := newTestCollector(t, 4)
			old := provider.Usage{InputTokens: 1}
			c.Observe(provider.StreamEvent{Kind: provider.UsageDone, Raw: []byte(`{"choices":[]}`), Usage: &old})
			copyUsage := u
			c.Observe(provider.StreamEvent{Kind: provider.UsageDone, Raw: []byte(`{"choices":[]}`), Usage: &copyUsage})
			copyUsage.InputTokens = -1
			m, err := c.Finish(&u, nil)
			require.NoError(t, err)
			require.Equal(t, Measurement{Usage: u}, m)
			m.Usage.InputTokens = -3
			c.Observe(chunk(`broken`))
			again, err := c.Finish(&u, nil)
			require.NoError(t, err)
			require.Equal(t, Measurement{Usage: u}, again)
			_, err = c.Finish(nil, nil)
			require.ErrorIs(t, err, errFinished)
		})
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, e := json.Marshal(v)
	require.NoError(t, e)
	return b
}

func TestUsageSources(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		final, snapshot, response *provider.Usage
		body                      string
		wantErr                   bool
	}{
		{"all agree", &provider.Usage{InputTokens: 2}, &provider.Usage{InputTokens: 2}, &provider.Usage{InputTokens: 2}, `{"usage":{"prompt_tokens":2,"completion_tokens":0}}`, false},
		{"body only", nil, nil, nil, `{"usage":{"prompt_tokens":2,"completion_tokens":0}}`, false},
		{"response only", nil, nil, &provider.Usage{InputTokens: 2}, `{}`, false},
		{"final conflict", &provider.Usage{InputTokens: 3}, &provider.Usage{InputTokens: 2}, nil, `{}`, true},
		{"response conflict", nil, &provider.Usage{InputTokens: 2}, &provider.Usage{InputTokens: 3}, `{}`, true},
		{"body conflict", &provider.Usage{InputTokens: 3}, nil, nil, `{"usage":{"prompt_tokens":2,"completion_tokens":0}}`, true},
		{"negative", &provider.Usage{InputTokens: -1}, nil, nil, `{}`, true},
		{"sum overflow", &provider.Usage{InputTokens: math.MaxInt64, OutputTokens: 1}, nil, nil, `{}`, true},
		{"bad body usage", nil, nil, nil, `{"usage":{"prompt_tokens":2}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCollector(t, 4)
			if tc.snapshot != nil {
				c.Observe(provider.StreamEvent{Kind: provider.UsageDone, Raw: []byte(`{}`), Usage: tc.snapshot})
			}
			m, err := c.Finish(tc.final, &provider.ChatResponse{Body: []byte(tc.body), Usage: tc.response})
			if tc.wantErr {
				require.Error(t, err)
				require.Equal(t, Measurement{}, m)
			} else {
				require.NoError(t, err)
				require.Equal(t, Measurement{Usage: provider.Usage{InputTokens: 2}}, m)
			}
		})
	}
}

func TestCanonicalInput(t *testing.T) {
	// [{"content":"中🙂","role":"user"}] contains 32 runes: 30 structure + 2 text.
	// Selected extras add [] (2), "auto" (6), and {"type":"json_object"} (22).
	for _, message := range []string{`{"role":"user","content":"中🙂"}`, ` { "content" : "\u4e2d\ud83d\ude42", "role":"user" } `} {
		r := request()
		r.Messages[0] = []byte(message)
		r.Extra = map[string]json.RawMessage{"tools": []byte(` [ ] `), "tool_choice": []byte(`"auto"`), "response_format": []byte(`{"type":"json_object"}`), "ignored": []byte(`{"private":"secret"}`)}
		before := mustJSON(t, r)
		c, err := NewCollector(r, 1)
		require.NoError(t, err)
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.Equal(t, Measurement{Usage: provider.Usage{InputTokens: 62}, Estimated: true}, m)
		require.Equal(t, before, mustJSON(t, r))
	}
}

func TestMultimodalInput(t *testing.T) {
	r := request()
	// Canonical message array is 74 ASCII runes, including URL and structure.
	r.Messages[0] = []byte(`{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}`)
	c, err := NewCollector(r, 1)
	require.NoError(t, err)
	m, err := c.Finish(nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 74, m.Usage.InputTokens)
}

func TestOutputFragments(t *testing.T) {
	for _, fragments := range [][]string{{"中🙂abc"}, {"中", "🙂", "a", "bc"}, {"中🙂", "abc"}} {
		c := newTestCollector(t, 4)
		for _, part := range fragments {
			c.Observe(chunk(string(mustJSON(t, map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"content": part}}}}))))
		}
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.Equal(t, Measurement{Usage: provider.Usage{InputTokens: 8, OutputTokens: 2}, Estimated: true}, m)
	}
	for _, field := range []string{"delta", "message"} {
		c := newTestCollector(t, 1)
		// 2 text + 2 tool name + 7 tool args + 5 structured content + 7 unknown JSON = 23.
		raw := `{"choices":[{"FIELD":{"role":"assistant","content":"中🙂","tool_calls":[{"index":0,"id":"ignored","type":"function","function":{"name":"fn","arguments":"{\"x\":1}"}}]}},{"FIELD":{"content":["中"],"future":{"x":1},"finish_reason":"stop"}}]}`
		raw = strings.ReplaceAll(raw, "FIELD", field)
		var response *provider.ChatResponse
		if field == "delta" {
			c.Observe(chunk(raw))
		} else {
			response = &provider.ChatResponse{Body: []byte(raw)}
		}
		m, err := c.Finish(nil, response)
		require.NoError(t, err)
		require.EqualValues(t, 23, m.Usage.OutputTokens)
	}
}

func TestRatioAndIntegerLimits(t *testing.T) {
	for _, c := range []float64{0, -1, 0.000999, 1024.000001, 1.0000001, math.NaN(), math.Inf(1), math.Nextafter(0.1, 1)} {
		_, err := NewCollector(request(), c)
		require.ErrorIs(t, err, errRatio)
	}
	for _, tc := range []struct {
		c    float64
		want int64
	}{{4, 8}, {0.001, 32000}, {1024, 1}, {0.123456, 260}, {0.1, 320}} {
		c := newTestCollector(t, tc.c)
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.Equal(t, tc.want, m.Usage.InputTokens)
	}
	for _, tc := range []struct {
		chars, num, den, want int64
		overflow              bool
	}{
		{math.MaxInt64, 1, 1, math.MaxInt64, false},
		{math.MaxInt64, 1024, 1, 9007199254740992, false},
		{math.MaxInt64, 1, 1000, 0, true},
		{3, 1, 10, 30, false},
		{math.MaxInt64 / 1000, 1, 1000, 9223372036854775000, false},
	} {
		n, err := ceilTokens(tc.chars, tc.num, tc.den)
		if tc.overflow {
			require.ErrorIs(t, err, errOverflow)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.want, n)
		}
	}
	_, err := add(math.MaxInt64, 1)
	require.ErrorIs(t, err, errOverflow)
	c := newTestCollector(t, 1).(*collector)
	c.inputChars = math.MaxInt64
	c.outputChars = 1
	_, err = c.Finish(nil, nil)
	require.ErrorIs(t, err, errOverflow)
}

func TestInvalidInputAndLatchedErrors(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `1`, `{}`, `{"role":""}`, `{"role":1}`, `{"role":"user"} trailing`, "{\"role\":\"\xff\"}"} {
		r := request()
		r.Messages[0] = []byte(raw)
		_, err := NewCollector(r, 4)
		require.Error(t, err)
	}
	_, err := NewCollector(nil, 4)
	require.Error(t, err)
	r := request()
	r.Messages = nil
	_, err = NewCollector(r, 4)
	require.Error(t, err)
	for _, e := range []provider.StreamEvent{
		chunk(`{"secret":"sensitive`), chunk("{\"x\":\"\xff\"}"), chunk(`{"choices":42}`),
		{Kind: provider.UsageDone, Raw: []byte(`{}`)},
		{Kind: provider.UsageDone, Raw: []byte(`{"usage":{"prompt_tokens":3,"completion_tokens":0}}`), Usage: &provider.Usage{InputTokens: 4}},
		{Kind: provider.UsageDone, Raw: []byte(`{}`), Usage: &provider.Usage{InputTokens: -1}},
	} {
		c := newTestCollector(t, 4)
		require.NotPanics(t, func() { c.Observe(e) })
		c.Observe(chunk(`{"choices":[{"delta":{"content":"good"}}]}`))
		m, err := c.Finish(&provider.Usage{}, nil)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "sensitive")
		require.Equal(t, Measurement{}, m)
		_, again := c.Finish(&provider.Usage{}, nil)
		require.Equal(t, err, again)
	}
}

func TestOwnershipAndConcurrentCollectors(t *testing.T) {
	r := request()
	raw := []byte(`{"choices":[{"delta":{"content":"中🙂abc"}}]}`)
	beforeReq, beforeRaw := mustJSON(t, r), string(raw)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			c, err := NewCollector(r, 4)
			if err != nil {
				t.Error(err)
				return
			}
			c.Observe(provider.StreamEvent{Kind: provider.Chunk, Raw: raw})
			m, err := c.Finish(nil, nil)
			if err != nil || m != (Measurement{Usage: provider.Usage{InputTokens: 8, OutputTokens: 2}, Estimated: true}) {
				t.Errorf("unexpected measurement: %v %v", m, err)
			}
		})
	}
	wg.Wait()
	require.Equal(t, beforeReq, mustJSON(t, r))
	require.Equal(t, beforeRaw, string(raw))
	c := newTestCollector(t, 1)
	c.Observe(provider.StreamEvent{Kind: provider.Chunk, Raw: raw})
	clear(raw)
	m, err := c.Finish(nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 5, m.Usage.OutputTokens)
	response := &provider.ChatResponse{Body: []byte(`{"choices":[{"message":{"content":"abc"}}]}`)}
	c = newTestCollector(t, 1)
	m, err = c.Finish(nil, response)
	require.NoError(t, err)
	require.EqualValues(t, 3, m.Usage.OutputTokens)
	again, err := c.Finish(nil, &provider.ChatResponse{Body: append([]byte(nil), response.Body...)})
	require.NoError(t, err)
	require.Equal(t, m, again)
	response.Body[0] = '['
	_, err = c.Finish(nil, response)
	require.ErrorIs(t, err, errFinished)
}
