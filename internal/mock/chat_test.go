package mock_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/mock"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	chatPath  = "/v1/chat/completions"
	chatModel = "upstream/模型-v2"
	chatKey   = "sk-mock-private-test-key"
)

type wireUsage struct {
	Prompt     int `json:"prompt_tokens"`
	Completion int `json:"completion_tokens"`
	Total      int `json:"total_tokens"`
}

type wireChoice struct {
	Index   int               `json:"index"`
	Delta   map[string]string `json:"delta"`
	Message map[string]string `json:"message"`
	Finish  *string           `json:"finish_reason"`
}

type wireCompletion struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	Model   string          `json:"model"`
	Choices []wireChoice    `json:"choices"`
	Usage   json.RawMessage `json:"usage"`
}

type wireEvent struct {
	done bool
	data wireCompletion
}

func chatBody(stream, usage bool) string {
	return fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"private-body-marker"}],"stream":%t,"stream_options":{"include_usage":%t}}`, chatModel, stream, usage)
}

func newChatServer(t *testing.T, e mock.Engine, wrap func(http.Handler) http.Handler) *httptest.Server {
	t.Helper()
	r := gin.New() // Deliberately no Gin Recover: abort must reach net/http.
	mock.RegisterChatRoutes(r, e)
	var handler http.Handler = r
	if wrap != nil {
		handler = wrap(handler)
	}
	s := httptest.NewServer(handler)
	s.Client().Timeout = 3 * time.Second
	t.Cleanup(s.Close)
	return s
}

func chatHTTPRequest(t *testing.T, ctx context.Context, s *httptest.Server, body, key, scenario string) *http.Request {
	t.Helper()
	r, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL+chatPath, strings.NewReader(body))
	require.NoError(t, err)
	r.Header.Set("Content-Type", "application/json; charset=utf-8")
	r.Header.Set("Authorization", "Bearer "+key)
	if scenario != "" {
		r.Header.Set("X-Mock-Scenario", scenario)
	}
	return r
}

func doChat(t *testing.T, s *httptest.Server, stream, usage bool, key, scenario string) *http.Response {
	t.Helper()
	r := chatHTTPRequest(t, t.Context(), s, chatBody(stream, usage), key, scenario)
	resp, err := s.Client().Do(r)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func readEvents(t *testing.T, body []byte) []wireEvent {
	t.Helper()
	require.NotEmpty(t, body)
	require.True(t, bytes.HasSuffix(body, []byte("\n\n")), "each event must have a blank-line delimiter: %q", body)
	blocks := strings.Split(strings.TrimSuffix(string(body), "\n\n"), "\n\n")
	events := make([]wireEvent, 0, len(blocks))
	for _, block := range blocks {
		require.True(t, strings.HasPrefix(block, "data: "), "unexpected SSE event: %q", block)
		require.NotContains(t, block, "\n", "one data line per event")
		payload := strings.TrimPrefix(block, "data: ")
		if payload == "[DONE]" {
			events = append(events, wireEvent{done: true})
			continue
		}
		var decoded wireCompletion
		require.NoError(t, json.Unmarshal([]byte(payload), &decoded))
		events = append(events, wireEvent{data: decoded})
	}
	return events
}

func assertContentFrames(t *testing.T, events []wireEvent, count int, complete bool, started, ended int64) {
	t.Helper()
	require.GreaterOrEqual(t, len(events), count)
	first := events[0].data
	assert.True(t, strings.HasPrefix(first.ID, "chatcmpl-"))
	assert.Greater(t, len(first.ID), len("chatcmpl-"))
	assert.GreaterOrEqual(t, first.Created, started)
	assert.LessOrEqual(t, first.Created, ended)
	for i, event := range events[:count] {
		require.False(t, event.done)
		frame := event.data
		assert.Equal(t, first.ID, frame.ID)
		assert.Equal(t, first.Created, frame.Created)
		assert.Equal(t, chatModel, frame.Model)
		assert.Equal(t, "chat.completion.chunk", frame.Object)
		require.Len(t, frame.Choices, 1)
		choice := frame.Choices[0]
		assert.Equal(t, 0, choice.Index)
		assert.Equal(t, fmt.Sprintf("mock-%d ", i+1), choice.Delta["content"])
		assert.Nil(t, choice.Message)
		if i == 0 {
			assert.Equal(t, "assistant", choice.Delta["role"])
		} else {
			assert.NotContains(t, choice.Delta, "role")
		}
		if complete && i == count-1 {
			require.NotNil(t, choice.Finish)
			assert.Equal(t, "stop", *choice.Finish)
		} else {
			assert.Nil(t, choice.Finish)
		}
		if len(frame.Usage) > 0 {
			assert.JSONEq(t, "null", string(frame.Usage))
		}
	}
}

func assertUsage(t *testing.T, raw json.RawMessage, prompt, completion int) {
	t.Helper()
	var usage wireUsage
	require.NoError(t, json.Unmarshal(raw, &usage))
	assert.Equal(t, wireUsage{Prompt: prompt, Completion: completion, Total: prompt + completion}, usage)
}

func assertJSONError(t *testing.T, resp *http.Response, status int, code string) {
	t.Helper()
	assert.Equal(t, status, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	assert.NotContains(t, resp.Header.Get("Content-Type"), "text/event-stream")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "HTTP error responses must complete normally")
	var decoded struct {
		Error struct{ Message, Type, Code string }
	}
	require.NoError(t, json.Unmarshal(body, &decoded))
	assert.Equal(t, "mock_error", decoded.Error.Type)
	assert.Equal(t, code, decoded.Error.Code)
	assert.NotEmpty(t, decoded.Error.Message)
	assert.NotContains(t, string(body), chatKey)
	assert.NotContains(t, string(body), "private-body-marker")
}

// §5①: default N=8, optional empty-choices usage, and exactly one DONE.
func TestChatNormalStreamFrames(t *testing.T) {
	for _, includeUsage := range []bool{false, true} {
		t.Run(fmt.Sprintf("usage=%t", includeUsage), func(t *testing.T) {
			s := newChatServer(t, mock.NewEngine(), nil)
			started := time.Now().Unix()
			resp := doChat(t, s, true, includeUsage, chatKey, "")
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, 1, resp.ProtoMajor)
			assert.Equal(t, "text/event-stream; charset=utf-8", resp.Header.Get("Content-Type"))
			assert.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))
			assert.Empty(t, resp.Header.Get("Content-Length"))
			assert.Empty(t, resp.Header.Get("Content-Encoding"))
			assert.Contains(t, resp.TransferEncoding, "chunked")
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			events := readEvents(t, body)
			wantCount := 9
			if includeUsage {
				wantCount = 10
			}
			require.Len(t, events, wantCount)
			assertContentFrames(t, events, 8, true, started, time.Now().Unix())
			if includeUsage {
				for _, event := range events[:8] {
					assert.JSONEq(t, "null", string(event.data.Usage))
				}
				usage := events[8].data
				require.NotNil(t, usage.Choices, "choices must be [], not null")
				assert.Empty(t, usage.Choices)
				assert.Equal(t, events[0].data.ID, usage.ID)
				assert.Equal(t, events[0].data.Created, usage.Created)
				assert.Equal(t, chatModel, usage.Model)
				assert.Equal(t, "chat.completion.chunk", usage.Object)
				assertUsage(t, usage.Usage, 12, 34)
			}
			assert.True(t, events[len(events)-1].done)
		})
	}
}

func TestChatSingleContentFrameAndZeroUsage(t *testing.T) {
	e := mock.NewEngine()
	p := mock.DefaultScenarioParams()
	p.Chunks, p.ChunkInterval, p.TTFBDelay, p.PromptTokens, p.CompletionTokens = 1, 0, 0, 0, 0
	require.NoError(t, e.Set(chatKey, mock.Normal, p))
	s := newChatServer(t, e, nil)
	started := time.Now().Unix()
	resp := doChat(t, s, true, true, chatKey, "")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	events := readEvents(t, body)
	require.Len(t, events, 3)
	assertContentFrames(t, events, 1, true, started, time.Now().Unix())
	assertUsage(t, events[1].data.Usage, 0, 0)
	assert.True(t, events[2].done)
}

// §5⑥: only the structural chat subset is consumed. Nonstream never waits for
// ChunkInterval; synthetic usage is unrelated to max_tokens or content length.
func TestChatNonstreamTemplateAndIgnoredFields(t *testing.T) {
	for _, scenario := range []mock.Scenario{mock.Normal, mock.Slow} {
		t.Run(string(scenario), func(t *testing.T) {
			e := mock.NewEngine()
			p := mock.DefaultScenarioParams()
			p.Chunks, p.ChunkInterval, p.TTFBDelay = 3, time.Hour, 0
			p.PromptTokens, p.CompletionTokens = 91, 0
			require.NoError(t, e.Set(chatKey, scenario, p))
			s := newChatServer(t, e, nil)
			body := fmt.Sprintf(`{"model":%q,"messages":[{}, {"content":17,"unknown":null}],"tools":null,"temperature":{},"max_tokens":1,"user":"ignored","stream_options":{"include_usage":true,"unknown":[]}}`, chatModel)
			r := chatHTTPRequest(t, t.Context(), s, body, chatKey, "")
			started := time.Now()
			resp, err := s.Client().Do(r)
			require.NoError(t, err)
			defer resp.Body.Close()
			assert.Less(t, time.Since(started), time.Second)
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
			var completion wireCompletion
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&completion))
			assert.Equal(t, "chat.completion", completion.Object)
			assert.True(t, strings.HasPrefix(completion.ID, "chatcmpl-"))
			assert.GreaterOrEqual(t, completion.Created, started.Unix())
			assert.LessOrEqual(t, completion.Created, time.Now().Unix())
			assert.Equal(t, chatModel, completion.Model)
			require.Len(t, completion.Choices, 1)
			assert.Equal(t, map[string]string{"role": "assistant", "content": "mock-1 mock-2 mock-3 "}, completion.Choices[0].Message)
			assert.Nil(t, completion.Choices[0].Delta)
			assert.Equal(t, 0, completion.Choices[0].Index)
			require.NotNil(t, completion.Choices[0].Finish)
			assert.Equal(t, "stop", *completion.Choices[0].Finish)
			assertUsage(t, completion.Usage, 91, 0)
		})
	}
}

// §5②: httptrace measures the first response byte, including headers. Normal
// uses an hour-long D to make any accidental TTFB wait fail the client budget.
func TestChatSlowDelaysHeaders(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, scenario := range []mock.Scenario{mock.Normal, mock.Slow} {
			t.Run(fmt.Sprintf("%s/stream=%t", scenario, stream), func(t *testing.T) {
				e := mock.NewEngine()
				p := mock.DefaultScenarioParams()
				p.Chunks, p.ChunkInterval, p.TTFBDelay = 2, 0, 100*time.Millisecond
				if scenario == mock.Normal {
					p.TTFBDelay = time.Hour
				}
				require.NoError(t, e.Set(chatKey, scenario, p))
				s := newChatServer(t, e, nil)
				firstByte := make(chan time.Time, 1)
				trace := &httptrace.ClientTrace{GotFirstResponseByte: func() { firstByte <- time.Now() }}
				r := chatHTTPRequest(t, httptrace.WithClientTrace(t.Context(), trace), s, chatBody(stream, false), chatKey, "")
				started := time.Now()
				resp, err := s.Client().Do(r)
				require.NoError(t, err)
				defer resp.Body.Close()
				elapsed := (<-firstByte).Sub(started)
				if scenario == mock.Slow {
					assert.GreaterOrEqual(t, elapsed, 90*time.Millisecond)
				}
				assert.Less(t, elapsed, time.Second)
				assert.Equal(t, http.StatusOK, resp.StatusCode)
				_, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				t.Logf("first response byte after %s", elapsed)
			})
		}
	}
}

func readOneEvent(t *testing.T, reader *bufio.Reader) []byte {
	t.Helper()
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	blank, err := reader.ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "\n", blank)
	return []byte(line + blank)
}

func TestChatContentFramesFlushWithIntervals(t *testing.T) {
	e := mock.NewEngine()
	p := mock.DefaultScenarioParams()
	p.Chunks, p.ChunkInterval = 3, 100*time.Millisecond
	require.NoError(t, e.Set(chatKey, mock.Normal, p))
	flushedAt := make(chan time.Time, 4) // Three content flushes and DONE.
	s := newChatServer(t, e, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			probe := &responseProbe{ResponseWriter: w, onFlush: func() { flushedAt <- time.Now() }}
			next.ServeHTTP(probe, r)
		})
	})
	start := time.Now()
	resp := doChat(t, s, true, false, chatKey, "")
	reader := bufio.NewReader(resp.Body)
	first := readOneEvent(t, reader)
	assert.Less(t, time.Since(start), time.Second)
	for i := 1; i < 3; i++ {
		next := readOneEvent(t, reader)
		first = append(first, next...)
	}
	remaining, err := io.ReadAll(reader)
	require.NoError(t, err)
	first = append(first, remaining...)
	events := readEvents(t, first)
	require.Len(t, events, 4)
	assertContentFrames(t, events, 3, true, start.Unix(), time.Now().Unix())
	assert.True(t, events[3].done)
	previous := <-flushedAt
	assert.Less(t, previous.Sub(start), time.Second)
	for range 2 {
		next := <-flushedAt
		// Server flush timestamps avoid interpreting client scheduling pauses as
		// a missing interval when multiple received frames are already buffered.
		assert.GreaterOrEqual(t, next.Sub(previous), 90*time.Millisecond)
		assert.Less(t, next.Sub(previous), time.Second)
		previous = next
	}
}

// §5③④: error status/body must arrive as a completed JSON transfer, for both
// stream flags. A refused request must fail before any HTTP response.
func TestChatHTTPFaultsAndConnectionSurvives(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, scenario := range []mock.Scenario{mock.Downtime, mock.RateLimited} {
			t.Run(fmt.Sprintf("%s/stream=%t", scenario, stream), func(t *testing.T) {
				s := newChatServer(t, mock.NewEngine(), nil)
				resp := doChat(t, s, stream, true, chatKey, string(scenario))
				if scenario == mock.Downtime {
					assertJSONError(t, resp, 503, "mock_unavailable")
				} else {
					assert.Equal(t, "1", resp.Header.Get("Retry-After"))
					assertJSONError(t, resp, 429, "mock_rate_limited")
				}
			})
		}
		for _, scenario := range []mock.Scenario{mock.Downtime, mock.Interrupted} {
			if scenario == mock.Interrupted && stream {
				continue // Streaming interruption has its own byte-level assertions.
			}
			t.Run(fmt.Sprintf("disconnect/%s/stream=%t", scenario, stream), func(t *testing.T) {
				e := mock.NewEngine()
				p := mock.DefaultScenarioParams()
				p.DowntimeMode, p.ChunkInterval = "refuse", 0
				require.NoError(t, e.Set(chatKey, scenario, p))
				s := newChatServer(t, e, nil)
				r := chatHTTPRequest(t, t.Context(), s, chatBody(stream, true), chatKey, "")
				resp, err := s.Client().Do(r)
				require.Error(t, err)
				require.Nil(t, resp, "fault must happen before an HTTP response")
				assertHealthyChat(t, s)
			})
		}
	}
}

func assertHealthyChat(t *testing.T, s *httptest.Server) {
	t.Helper()
	resp := doChat(t, s, false, false, "sk-mock-other-key", "normal")
	require.Equal(t, 200, resp.StatusCode, "same listener must survive injected faults")
	var completion wireCompletion
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&completion))
	assert.Equal(t, "chat.completion", completion.Object)
	require.Len(t, completion.Choices, 1)
	assert.NotEmpty(t, completion.Choices[0].Message["content"])
}

// §5⑤: actual HTTP/1.1 chunked response, flushed content, no successful tail,
// and a transport read error (not ordinary EOF or a client timeout).
func TestChatInterruptedIsRealHTTPFailure(t *testing.T) {
	for _, k := range []int{1, 3, 7} {
		t.Run(fmt.Sprintf("k=%d", k), func(t *testing.T) {
			e := mock.NewEngine()
			p := mock.DefaultScenarioParams()
			p.ChunkInterval, p.FailAfterChunks = 0, k
			require.NoError(t, e.Set(chatKey, mock.Interrupted, p))
			var serverLog bytes.Buffer
			r := gin.New()
			mock.RegisterChatRoutes(r, e)
			s := httptest.NewUnstartedServer(r)
			s.Config.ErrorLog = log.New(&serverLog, "", 0)
			s.Start()
			s.Client().Timeout = 3 * time.Second
			defer s.Close()
			started := time.Now()
			resp := doChat(t, s, true, true, chatKey, "")
			require.Equal(t, 200, resp.StatusCode)
			require.Equal(t, 1, resp.ProtoMajor)
			require.Contains(t, resp.TransferEncoding, "chunked")
			body, err := io.ReadAll(resp.Body)
			require.Error(t, err)
			assert.NotErrorIs(t, err, io.EOF)
			assert.NotErrorIs(t, err, context.DeadlineExceeded)
			assert.Less(t, time.Since(started), time.Second)
			events := readEvents(t, body)
			require.Len(t, events, k)
			assertContentFrames(t, events, k, false, started.Unix(), time.Now().Unix())
			for _, event := range events {
				assert.False(t, event.done)
				assert.JSONEq(t, "null", string(event.data.Usage))
			}
			assertHealthyChat(t, s)
			s.Close() // Synchronize before inspecting the server's error logger.
			assert.Empty(t, serverLog.String(), "ErrAbortHandler must not emit panic stacks")
			t.Logf("received %d flushed content frames then transport error: %v", k, err)
		})
	}
}

type countingEngine struct {
	mock.Engine
	calls atomic.Int32
}

func (e *countingEngine) Resolve(key, header string) (mock.Scenario, mock.ScenarioParams, error) {
	e.calls.Add(1)
	return e.Engine.Resolve(key, header)
}

func changeChatField(t *testing.T, field, raw string) string {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(chatBody(true, true)), &fields))
	if raw == "" {
		delete(fields, field)
	} else {
		fields[field] = json.RawMessage(raw)
	}
	body, err := json.Marshal(fields)
	require.NoError(t, err)
	return string(body)
}

// §5⑫: malformed input cannot reach even Resolve, let alone a one-hour slow
// wait. Use actual HTTP responses and assert both code and mechanism.
func TestChatInputValidationBeforeResolve(t *testing.T) {
	e := &countingEngine{Engine: mock.NewEngine()}
	p := mock.DefaultScenarioParams()
	p.TTFBDelay = time.Hour
	require.NoError(t, e.Set(chatKey, mock.Slow, p))
	s := newChatServer(t, e, nil)
	cases := []struct {
		name, field, raw string
	}{
		{"missing model", "model", ""}, {"empty model", "model", `""`},
		{"blank model", "model", `" \t"`}, {"null model", "model", "null"},
		{"numeric model", "model", "1"}, {"object model", "model", "{}"},
		{"missing messages", "messages", ""}, {"null messages", "messages", "null"},
		{"empty messages", "messages", "[]"}, {"object messages", "messages", "{}"},
		{"string messages", "messages", `"text"`}, {"null message", "messages", "[null]"},
		{"numeric message", "messages", "[1]"}, {"array message", "messages", "[[]]"},
		{"mixed messages", "messages", "[{},false]"},
		{"null stream", "stream", "null"}, {"numeric stream", "stream", "1"},
		{"string stream", "stream", `"true"`}, {"array stream", "stream", "[]"},
		{"null options", "stream_options", "null"}, {"array options", "stream_options", "[]"},
		{"string options", "stream_options", `"options"`}, {"numeric options", "stream_options", "1"},
		{"null usage", "stream_options", `{"include_usage":null}`},
		{"numeric usage", "stream_options", `{"include_usage":1}`},
		{"string usage", "stream_options", `{"include_usage":"false"}`},
		{"array usage", "stream_options", `{"include_usage":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := chatHTTPRequest(t, t.Context(), s, changeChatField(t, tc.field, tc.raw), chatKey, "")
			resp, err := s.Client().Do(r)
			require.NoError(t, err)
			defer resp.Body.Close()
			assertJSONError(t, resp, 400, "mock_invalid_request")
			assert.Zero(t, e.calls.Load())
		})
	}
	for _, body := range []string{"", "null", "[]", "1", `"text"`, "{", chatBody(true, true) + " {}", chatBody(true, true) + " garbage"} {
		t.Run("json/"+body[:min(len(body), 16)], func(t *testing.T) {
			r := chatHTTPRequest(t, t.Context(), s, body, chatKey, "")
			resp, err := s.Client().Do(r)
			require.NoError(t, err)
			defer resp.Body.Close()
			assertJSONError(t, resp, 400, "mock_invalid_request")
			assert.Zero(t, e.calls.Load())
		})
	}
	for _, auth := range []string{"", "Basic abc", "Bearer", "Bearer ", "Bearer  sk-mock-a", "Bearer sk mock", "Bearer sk\u00a0mock"} {
		t.Run("auth/"+auth, func(t *testing.T) {
			r := chatHTTPRequest(t, t.Context(), s, chatBody(true, true), chatKey, "")
			r.Header.Set("Authorization", auth)
			resp, err := s.Client().Do(r)
			require.NoError(t, err)
			defer resp.Body.Close()
			assertJSONError(t, resp, 401, "mock_unauthorized")
			assert.Zero(t, e.calls.Load())
		})
	}
	for _, mediaType := range []string{"", "text/plain", "application/problem+json", "application/json; charset"} {
		t.Run("media/"+mediaType, func(t *testing.T) {
			r := chatHTTPRequest(t, t.Context(), s, chatBody(true, true), chatKey, "")
			r.Header.Set("Content-Type", mediaType)
			resp, err := s.Client().Do(r)
			require.NoError(t, err)
			defer resp.Body.Close()
			assertJSONError(t, resp, 415, "mock_unsupported_media_type")
			assert.Zero(t, e.calls.Load())
		})
	}
	before := e.Snapshot()
	for _, scenario := range []string{"unknown", "NORMAL"} {
		resp := doChat(t, s, true, true, chatKey, scenario)
		assertJSONError(t, resp, 400, "mock_invalid_request")
	}
	assert.Equal(t, int32(2), e.calls.Load())
	assert.Equal(t, before, e.Snapshot())
}

func TestChatBodySizeBoundary(t *testing.T) {
	e := &countingEngine{Engine: mock.NewEngine()}
	s := newChatServer(t, e, nil)
	base := strings.TrimSuffix(chatBody(false, false), "}") + `,"padding":""}`
	for _, extra := range []int{0, 1} {
		body := strings.TrimSuffix(base, `"}`) + strings.Repeat("x", (1<<20)-len(base)+extra) + `"}`
		require.Len(t, body, (1<<20)+extra)
		r := chatHTTPRequest(t, t.Context(), s, body, chatKey, "")
		resp, err := s.Client().Do(r)
		require.NoError(t, err)
		if extra == 0 {
			assert.Equal(t, 200, resp.StatusCode)
			_, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
		} else {
			assertJSONError(t, resp, 413, "mock_request_too_large")
		}
		resp.Body.Close()
	}
	assert.Equal(t, int32(1), e.calls.Load(), "oversized body must not resolve a scenario")
}

// §5⑦: data-plane precedence and invalid final header/config combinations.
func TestChatKeyIsolationAndHeaderPrecedence(t *testing.T) {
	e := mock.NewEngine()
	p := mock.DefaultScenarioParams()
	p.Chunks, p.ChunkInterval = 1, 0 // Default k=3 cannot be overridden to interrupted.
	require.NoError(t, e.Set("sk-mock-a", mock.Downtime, p))
	require.NoError(t, e.Set("sk-mock-b", mock.RateLimited, mock.DefaultScenarioParams()))
	before := e.Snapshot()
	s := newChatServer(t, e, nil)
	assertJSONError(t, doChat(t, s, false, false, "sk-mock-a", ""), 503, "mock_unavailable")
	assertJSONError(t, doChat(t, s, false, false, "sk-mock-b", ""), 429, "mock_rate_limited")
	assertHealthyChat(t, s)
	resp := doChat(t, s, true, true, "sk-mock-a", "normal")
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	events := readEvents(t, body)
	require.Len(t, events, 3, "header override preserves the key's N=1 parameter")
	assert.True(t, events[2].done)
	assertJSONError(t, doChat(t, s, true, true, "sk-mock-a", "interrupted"), 400, "mock_invalid_request")
	assertJSONError(t, doChat(t, s, false, false, "sk-mock-a", "unknown"), 400, "mock_invalid_request")
	assert.Equal(t, before, e.Snapshot())
	assertJSONError(t, doChat(t, s, false, false, "sk-mock-a", ""), 503, "mock_unavailable")
}

func TestChatOnlyRegistersPOSTChat(t *testing.T) {
	s := newChatServer(t, mock.NewEngine(), nil)
	for _, tc := range []struct{ method, path string }{
		{http.MethodGet, chatPath}, {http.MethodPut, chatPath},
		{http.MethodGet, "/v1/models"}, {http.MethodPost, "/v1/responses"},
		{http.MethodPut, "/admin/scenario"}, {http.MethodGet, "/admin/scenarios"},
	} {
		r, err := http.NewRequestWithContext(t.Context(), tc.method, s.URL+tc.path, nil)
		require.NoError(t, err)
		resp, err := s.Client().Do(r)
		require.NoError(t, err)
		assert.Equal(t, 404, resp.StatusCode)
		resp.Body.Close()
	}
}

// Done is evaluated by production waitContext's select. This gives a precise
// server-side wait-entry signal without a production test hook or a sleep.
type waitObservedContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *waitObservedContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

type responseProbe struct {
	http.ResponseWriter
	onWrite func()
	onFlush func()
}

func (w *responseProbe) WriteHeader(code int) {
	if w.onWrite != nil {
		w.onWrite()
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseProbe) Write(data []byte) (int, error) {
	if w.onWrite != nil {
		w.onWrite()
	}
	return w.ResponseWriter.Write(data)
}

func (w *responseProbe) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err == nil && w.onFlush != nil {
		w.onFlush()
	}
	return err
}

type httpResult struct {
	resp *http.Response
	err  error
}

func awaitSignal(t *testing.T, ch <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

// §5⑨: real client cancellation, plus independent proof that the server handler
// exits while its configured 5-second wait still has time remaining.
func TestChatCancellationExitsServerHandler(t *testing.T) {
	for _, phase := range []string{"slow-stream", "slow-nonstream", "stream-interval"} {
		t.Run(phase, func(t *testing.T) {
			e := mock.NewEngine()
			p := mock.DefaultScenarioParams()
			p.TTFBDelay, p.ChunkInterval = 5*time.Second, 5*time.Second
			scenario := mock.Slow
			if phase == "stream-interval" {
				scenario = mock.Normal
			}
			require.NoError(t, e.Set(chatKey, scenario, p))
			entered, wrote := make(chan struct{}), make(chan struct{})
			exited := make(chan error, 1)
			var writeOnce sync.Once
			s := newChatServer(t, e, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ctx := &waitObservedContext{Context: r.Context(), entered: entered}
					defer func() { exited <- ctx.Err() }()
					probe := &responseProbe{ResponseWriter: w, onWrite: func() { writeOnce.Do(func() { close(wrote) }) }}
					next.ServeHTTP(probe, r.WithContext(ctx))
				})
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			r := chatHTTPRequest(t, ctx, s, chatBody(phase != "slow-nonstream", true), chatKey, "")
			result := make(chan httpResult, 1)
			go func() {
				resp, err := s.Client().Do(r)
				result <- httpResult{resp: resp, err: err}
			}()
			var resp *http.Response
			if phase == "stream-interval" {
				got := <-result
				require.NoError(t, got.err)
				resp = got.resp
				defer resp.Body.Close()
				first := readEvents(t, readOneEvent(t, bufio.NewReader(resp.Body)))
				require.Len(t, first, 1)
				assert.Equal(t, "mock-1 ", first[0].data.Choices[0].Delta["content"])
			}
			awaitSignal(t, entered, "server wait select")
			if scenario == mock.Slow {
				select {
				case <-wrote:
					t.Fatal("slow wrote a response before its first-byte wait ended")
				default:
				}
			}
			select {
			case err := <-exited:
				t.Fatalf("handler exited before cancellation: %v", err)
			default:
			}
			started := time.Now()
			timer := time.AfterFunc(100*time.Millisecond, cancel)
			defer timer.Stop()
			select {
			case err := <-exited:
				require.ErrorIs(t, err, context.Canceled)
				elapsed := time.Since(started)
				assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond)
				assert.Less(t, elapsed, 800*time.Millisecond)
				t.Logf("server handler exited after %s with a 100ms cancellation budget", elapsed)
			case <-time.After(time.Second):
				t.Fatal("server handler remained in its 5s wait after cancellation")
			}
			if resp != nil {
				rest, err := io.ReadAll(resp.Body)
				require.Error(t, err)
				assert.Empty(t, rest, "canceled stream must not produce subsequent frames")
			} else {
				got := <-result
				require.Error(t, got.err)
				assert.Nil(t, got.resp)
			}
		})
	}
}

type gatedEngine struct {
	mock.Engine
	calls    atomic.Int32
	resolved chan struct{}
	release  <-chan struct{}
}

func (e *gatedEngine) Resolve(key, header string) (mock.Scenario, mock.ScenarioParams, error) {
	s, p, err := e.Engine.Resolve(key, header)
	if e.calls.Add(1) == 1 {
		close(e.resolved)
		<-e.release
	}
	return s, p, err
}

func replaceWhileInFlight(t *testing.T, e mock.Engine) {
	t.Helper()
	updated := make(chan error, 1)
	go func() { updated <- e.Set(chatKey, mock.Downtime, mock.DefaultScenarioParams()) }()
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Set blocked on an in-flight request")
	}
}

// §5⑩: gate the first Resolve after the real Engine has copied its old value.
// New requests see the committed replacement; the old slow request stays slow
// and succeeds with old N/usage after being released.
func TestChatSlowRequestKeepsResolvedSnapshot(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	e := &gatedEngine{Engine: mock.NewEngine(), resolved: make(chan struct{}), release: release}
	p := mock.DefaultScenarioParams()
	p.Chunks, p.ChunkInterval, p.TTFBDelay, p.PromptTokens = 3, 0, 100*time.Millisecond, 77
	require.NoError(t, e.Set(chatKey, mock.Slow, p))
	s := newChatServer(t, e, nil)
	r := chatHTTPRequest(t, t.Context(), s, chatBody(false, false), chatKey, "")
	result := make(chan httpResult, 1)
	go func() {
		resp, err := s.Client().Do(r)
		result <- httpResult{resp: resp, err: err}
	}()
	awaitSignal(t, e.resolved, "old request's resolved snapshot")
	replaceWhileInFlight(t, e)
	assertJSONError(t, doChat(t, s, false, false, chatKey, ""), 503, "mock_unavailable")
	started := time.Now()
	releaseOnce.Do(func() { close(release) })
	got := <-result
	require.NoError(t, got.err)
	defer got.resp.Body.Close()
	assert.Equal(t, 200, got.resp.StatusCode)
	assert.GreaterOrEqual(t, time.Since(started), 90*time.Millisecond)
	assert.Less(t, time.Since(started), time.Second)
	var completion wireCompletion
	require.NoError(t, json.NewDecoder(got.resp.Body).Decode(&completion))
	assert.Equal(t, "mock-1 mock-2 mock-3 ", completion.Choices[0].Message["content"])
	assertUsage(t, completion.Usage, 77, 34)
	assert.Equal(t, int32(2), e.calls.Load(), "each request must resolve exactly once")
}

// §5⑩: first actual Flush is the barrier for an in-flight stream. Engine Set
// must finish while the network writer is paused, and the old stream must still
// send its original N, usage and DONE after the replacement.
func TestChatStreamKeepsSnapshotWhileNetworkWritePaused(t *testing.T) {
	e := &countingEngine{Engine: mock.NewEngine()}
	p := mock.DefaultScenarioParams()
	p.Chunks, p.ChunkInterval, p.PromptTokens = 4, 0, 55
	require.NoError(t, e.Set(chatKey, mock.Normal, p))
	flushed, release := make(chan struct{}), make(chan struct{})
	var first, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	s := newChatServer(t, e, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			probe := &responseProbe{ResponseWriter: w, onFlush: func() {
				first.Do(func() {
					close(flushed)
					<-release
				})
			}}
			next.ServeHTTP(probe, r)
		})
	})
	started := time.Now().Unix()
	resp := doChat(t, s, true, true, chatKey, "")
	reader := bufio.NewReader(resp.Body)
	firstFrame := readOneEvent(t, reader)
	awaitSignal(t, flushed, "first frame flushed on TCP")
	replaceWhileInFlight(t, e)
	assertJSONError(t, doChat(t, s, false, false, chatKey, ""), 503, "mock_unavailable")
	releaseOnce.Do(func() { close(release) })
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	events := readEvents(t, append(firstFrame, rest...))
	require.Len(t, events, 6)
	assertContentFrames(t, events, 4, true, started, time.Now().Unix())
	assertUsage(t, events[4].data.Usage, 55, 34)
	assert.True(t, events[5].done)
	assert.Equal(t, int32(2), e.calls.Load())
}
