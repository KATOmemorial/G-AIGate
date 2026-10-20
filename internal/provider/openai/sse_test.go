package openai

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"gaigate/internal/provider"

	"github.com/stretchr/testify/require"
)

func TestSSELineRulesAndPayloadOwnership(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n"} {
		input := strings.Join([]string{
			": heartbeat", "event: ignored", "id: 123", "retry: 5", "", "", // no data
			"data:  {", `data: "choices": [],`, `data: "usage": null`, "data: } ", "",
			"data", "", // data without a colon is an empty payload, not a heartbeat
			"data:\t{}", "",
			"data: [DONE]", "", "",
		}, newline)
		p := newSSEParser(strings.NewReader(input))
		first, err := p.next()
		require.NoError(t, err)
		require.Equal(t, " {\n\"choices\": [],\n\"usage\": null\n} ", string(first))
		saved := bytes.Clone(first)
		event, err := parseEvent(first)
		require.NoError(t, err)
		require.Equal(t, provider.Chunk, event.Kind)
		raw, err := p.next()
		require.NoError(t, err)
		require.Empty(t, raw)
		_, err = parseEvent(raw)
		require.Error(t, err)
		raw, err = p.next()
		require.NoError(t, err)
		require.Equal(t, "\t{}", string(raw), "remove at most one ASCII space")
		raw, err = p.next()
		require.NoError(t, err)
		require.Equal(t, "[DONE]", string(raw))
		require.Equal(t, saved, first, "later reader activity cannot overwrite a prior payload")
		_, err = p.next()
		require.ErrorIs(t, err, io.EOF)
	}
}

func TestSSEEventInputLimit(t *testing.T) {
	for _, newline := range []string{"\n", "\r\n"} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%q/%d", newline, delta), func(t *testing.T) {
				size := maxEventInput + delta
				base := "data: {\"padding\":\"\"}" + newline + newline
				input := "data: {\"padding\":\"" + strings.Repeat("x", size-len(base)) + "\"}" + newline + newline
				require.Len(t, input, size)
				raw, err := newSSEParser(strings.NewReader(input)).next()
				if delta > 0 {
					require.ErrorContains(t, err, "exceeds 1 MiB")
				} else {
					require.NoError(t, err, "must support lines larger than Scanner's default")
					_, err = parseEvent(raw)
					require.NoError(t, err)
				}
			})
		}
	}
	for _, prefix := range []string{":", "event:", "id:", "retry:", "unknown:"} {
		// All lines count, even when individually short and carrying no data.
		input := strings.Repeat(prefix+strings.Repeat("x", 127)+"\n", maxEventInput/128+1)
		_, err := newSSEParser(strings.NewReader(input)).next()
		require.ErrorContains(t, err, "exceeds 1 MiB")
	}
	// Ignored lines count together with data and the delimiter, including at the boundary.
	for _, delta := range []int{0, 1} {
		base := ":\ndata: {}\n\n"
		input := ":" + strings.Repeat("x", maxEventInput-len(base)+delta) + "\ndata: {}\n\n"
		_, err := newSSEParser(strings.NewReader(input)).next()
		if delta == 0 {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestSSEClassifiesFrozenSubset(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		kind  provider.StreamEventKind
		valid bool
	}{
		{`{}`, provider.Chunk, true}, {`{"usage":null}`, provider.Chunk, true},
		{`{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":1}}`, provider.UsageDone, true},
		{`[DONE]`, provider.Done, true},
		{` [DONE]`, 0, false}, {`[DONE] `, 0, false}, {``, 0, false}, {`null`, 0, false}, {`[]`, 0, false}, {`{} {}`, 0, false},
		{`{"usage":{"prompt_tokens":0,"completion_tokens":1}}`, 0, false},
		{`{"choices":null,"usage":{"prompt_tokens":0,"completion_tokens":1}}`, 0, false},
		{`{"choices":{},"usage":{"prompt_tokens":0,"completion_tokens":1}}`, 0, false},
		{`{"choices":[{"delta":{"content":"mixed"}}],"usage":{"prompt_tokens":0,"completion_tokens":1}}`, 0, false},
		{`{"choices":[],"usage":{"prompt_tokens":-1,"completion_tokens":1}}`, 0, false},
		{`{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":9223372036854775808}}`, 0, false},
	} {
		event, err := parseEvent([]byte(tc.raw))
		if !tc.valid {
			require.Error(t, err, tc.raw)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, tc.kind, event.Kind)
		require.Equal(t, tc.raw, string(event.Raw))
		require.Nil(t, event.Err)
		if tc.kind == provider.UsageDone {
			require.Equal(t, &provider.Usage{OutputTokens: 1}, event.Usage)
		} else {
			require.Nil(t, event.Usage)
		}
	}
}

func TestSSEReadFailureRetainsCause(t *testing.T) {
	cause := errors.New("scripted read error")
	_, err := newSSEParser(io.MultiReader(strings.NewReader("data: {"), failingReader{cause})).next()
	require.ErrorIs(t, err, cause)
	for _, input := range []string{"data: [DONE]", "data: [DONE]\n", "data: {}", ":heartbeat\n"} {
		_, err := newSSEParser(strings.NewReader(input)).next()
		require.ErrorIs(t, err, io.EOF, "incomplete event must not be dispatched")
	}
}
