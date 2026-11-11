package meter

import (
	"math"
	"testing"

	"gaigate/internal/provider"
	"github.com/stretchr/testify/require"
)

func TestToolRefragmentation(t *testing.T) {
	// Function name (2) and arguments (7) total 9 runes: ceil(9/4)=3.
	for _, parts := range [][][2]string{
		{{"fn", `{"中":1}`}},
		{{"f", `{"`}, {"n", `中`}, {"", `":1}`}},
	} {
		c := newTestCollector(t, 4)
		for _, p := range parts {
			raw := mustJSON(t, map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"tool_calls": []any{map[string]any{"function": map[string]any{"name": p[0], "arguments": p[1]}}}}}}})
			c.Observe(provider.StreamEvent{Kind: provider.Chunk, Raw: raw})
		}
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, 3, m.Usage.OutputTokens)
	}
}

func TestUnknownAndEmptyOutput(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		chars int64
	}{
		{`{}`, 0}, {`{"choices":[]}`, 0},
		{`{"choices":[{"delta":{"role":"assistant"},"finish_reason":"stop"}]}`, 0},
		{`{"choices":[{"delta":{"content":""}}]}`, 0},
		{`{"choices":[{"delta":{"content":null}}]}`, 4},
		{`{"choices":[{"delta":{"future":"中🙂"}}]}`, 4},
		{`{"choices":[{"delta":{"tool_calls":[17]}}]}`, 2},
	} {
		c := newTestCollector(t, 1)
		c.Observe(chunk(tc.raw))
		m, err := c.Finish(nil, nil)
		require.NoError(t, err)
		require.Equal(t, tc.chars, m.Usage.OutputTokens)
	}
}

func TestOutputCountOverflowIsLatched(t *testing.T) {
	c := newTestCollector(t, 1).(*collector)
	c.outputChars = math.MaxInt64
	c.Observe(chunk(`{"choices":[{"delta":{"content":"x"}}]}`))
	_, err := c.Finish(&provider.Usage{}, nil)
	require.ErrorIs(t, err, errOverflow)
}
