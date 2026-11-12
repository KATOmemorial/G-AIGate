package meter

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gaigate/internal/provider"
	"github.com/stretchr/testify/require"
)

func TestInputIsConsumedOnceAndOnlySelectedExtras(t *testing.T) {
	r := request()
	r.Extra = map[string]json.RawMessage{"tools": []byte(`{"n":99999999999999999}`), "ignored": []byte(`not JSON`), "max_tokens": []byte(`1000`)}
	// 32 message runes + 23 tool runes; UseNumber must not turn the 17-digit
	// integer into an 18-digit rounded binary64 value.
	c, err := NewCollector(r, 1)
	require.NoError(t, err)
	r.Messages[0][0] = '!'
	r.Extra["tools"][0] = '!'
	r.Model = "changed"
	r.Stream = true
	m, err := c.Finish(nil, nil)
	require.NoError(t, err)
	require.Equal(t, Measurement{Usage: provider.Usage{InputTokens: 55}, Estimated: true}, m)
	for _, raw := range []string{`{"role":"assistant"}`, `{"role":"assistant","content":null}`, `{"role":"user","content":17}`, `{"role":"user","content":[]}`} {
		r := request()
		r.Messages[0] = []byte(raw)
		c, err := NewCollector(r, 1)
		require.NoError(t, err)
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.True(t, m.Estimated)
	}
	for _, key := range []string{"tools", "tool_choice", "response_format"} {
		for _, raw := range []string{`broken`, "\xff", `{} {}`} {
			r := request()
			r.Extra = map[string]json.RawMessage{key: []byte(raw)}
			_, err := NewCollector(r, 4)
			require.ErrorIs(t, err, errInput)
		}
	}
}

func TestEveryTextAndToolPartition(t *testing.T) {
	text := []rune("中🙂abcdef") // eight runes, ceil(8/3)=3 regardless of 128 partitions.
	for _, tool := range []bool{false, true} {
		for mask := 0; mask < (1 << (len(text) - 1)); mask++ {
			c := newTestCollector(t, 3)
			start := 0
			for end := 1; end <= len(text); end++ {
				if end < len(text) && mask&(1<<(end-1)) == 0 {
					continue
				}
				p := string(text[start:end])
				start = end
				delta := map[string]any{"content": p}
				if tool {
					delta = map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"arguments": p}}}}
				}
				c.Observe(provider.StreamEvent{Kind: provider.Chunk, Raw: mustJSON(t, map[string]any{"choices": []any{map[string]any{"delta": delta}}})})
			}
			m, err := c.Finish(nil, nil)
			require.NoError(t, err)
			require.EqualValues(t, 3, m.Usage.OutputTokens)
		}
	}
}

func TestUnknownStructuredValuesAndNoCompletionTextSupport(t *testing.T) {
	for _, tc := range []struct {
		delta string
		want  int64
	}{
		{`{"content":[{"b":2,"a":"中"}]}`, 17},
		{`{"future":{"b":2,"a":"中"}}`, 15},
		{`{"tool_calls":{"future":true}}`, 15},
		{`{"tool_calls":[{"function":{"arguments":null,"name":17,"future":[1]}}]}`, 9},
		{`{"tool_calls":[{"function":"x","future":true}]}`, 7},
	} {
		c := newTestCollector(t, 1)
		c.Observe(chunk(`{"choices":[{"delta":` + tc.delta + `}]}`))
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.Equal(t, tc.want, m.Usage.OutputTokens, tc.delta)
	}
	c := newTestCollector(t, 1)
	c.Observe(chunk(`{"choices":[{"text":"not a chat delta"}]}`))
	m, err := c.Finish(nil, nil)
	require.NoError(t, err)
	require.Zero(t, m.Usage.OutputTokens)
}

func TestFinishFreezeAndSafeFailureMatrix(t *testing.T) {
	secret := "sensitive-credential-marker"
	for _, raw := range []string{`{"usage":{}}`, `{"usage":{"prompt_tokens":null,"completion_tokens":0}}`, `{"usage":{"prompt_tokens":1.5,"completion_tokens":0}}`, `{"usage":{"prompt_tokens":1e0,"completion_tokens":0}}`, `{"usage":{"prompt_tokens":0,"completion_tokens":-1}}`, `{"usage":{"prompt_tokens":9223372036854775807,"completion_tokens":1}}`, `{"usage":{"prompt_tokens":9223372036854775808,"completion_tokens":0}}`, `{"choices":[null]}`, `{"choices":[{"message":17}]}`, `{"choices":{}}`, `[]`, "{\"" + secret + "\":\"\xff\"}", `{"` + secret + `":`} {
		t.Run(fmt.Sprintf("body-%x", []byte(raw)[:min(len(raw), 12)]), func(t *testing.T) {
			c := newTestCollector(t, 4)
			response := &provider.ChatResponse{Body: []byte(raw)}
			before := append([]byte(nil), response.Body...)
			m, err := c.Finish(nil, response)
			require.Error(t, err)
			require.Equal(t, Measurement{}, m)
			require.NotContains(t, err.Error(), secret)
			require.Equal(t, before, []byte(response.Body))
			_, again := c.Finish(nil, response)
			require.Same(t, err, again)
			_, err = c.Finish(&provider.Usage{}, response)
			require.ErrorIs(t, err, errFinished)
		})
	}
	for _, e := range []provider.StreamEvent{
		{Kind: provider.Error, Err: errors.New(secret)}, {Kind: provider.Done}, {Kind: 255},
		{Kind: provider.Chunk, Raw: []byte(`{}`), Usage: &provider.Usage{}},
		{Kind: provider.UsageDone, Raw: []byte(`{}`), Usage: &provider.Usage{InputTokens: math.MaxInt64, OutputTokens: 1}},
	} {
		c := newTestCollector(t, 4)
		require.NotPanics(t, func() { c.Observe(e) })
		_, err := c.Finish(nil, nil)
		require.Error(t, err)
		require.NotContains(t, err.Error(), secret)
	}
	c := newTestCollector(t, 1)
	u := provider.Usage{InputTokens: 3, OutputTokens: 4}
	r := &provider.ChatResponse{Body: []byte(`{"choices":[]}`), Usage: &u}
	m, err := c.Finish(&u, r)
	require.NoError(t, err)
	copyUsage := u
	again, err := c.Finish(&copyUsage, &provider.ChatResponse{Body: append([]byte(nil), r.Body...), Usage: &copyUsage})
	require.NoError(t, err)
	require.Equal(t, m, again)
	u.OutputTokens++
	_, err = c.Finish(&copyUsage, r)
	require.ErrorIs(t, err, errFinished)
	_, err = c.Finish(&copyUsage, &provider.ChatResponse{Body: r.Body, Usage: &copyUsage})
	require.NoError(t, err)
}

func TestBoundedRetentionAndIndependentAttempts(t *testing.T) {
	// Structural evidence: retained state has no variable-length buffers, maps,
	// strings or pointer fields that could grow with request/output size. Only
	// static error sentinels are stored through the two error interfaces.
	typ := reflect.TypeOf(collector{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		require.NotContains(t, []reflect.Kind{reflect.Slice, reflect.Map, reflect.String, reflect.Pointer, reflect.Func, reflect.Chan}, f.Type.Kind(), f.Name)
		if f.Type.Kind() == reflect.Interface {
			require.Equal(t, reflect.TypeFor[error](), f.Type)
		}
	}
	c := newTestCollector(t, 4)
	for range 4096 {
		c.Observe(chunk(`{"choices":[{"delta":{"content":"中🙂abc"}}]}`))
	}
	m, err := c.Finish(nil, nil)
	require.NoError(t, err)
	require.EqualValues(t, 5120, m.Usage.OutputTokens)
	var wg sync.WaitGroup
	shared := []byte(`{"choices":[{"delta":{"content":"中🙂"}}]}`)
	for i := 1; i <= 32; i++ {
		wg.Go(func() {
			r := request()
			r.Messages[0] = mustJSON(t, map[string]any{"role": "user", "content": strings.Repeat("x", i)})
			c, err := NewCollector(r, 1)
			if err != nil {
				t.Error(err)
				return
			}
			c.Observe(provider.StreamEvent{Kind: provider.Chunk, Raw: shared})
			m, err := c.Finish(nil, nil)
			if err != nil || m.Usage.InputTokens != int64(30+i) || m.Usage.OutputTokens != 2 {
				t.Errorf("attempt %d: %v %v", i, m, err)
			}
		})
	}
	wg.Wait()
}
