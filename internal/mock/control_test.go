package mock_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/mock"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type controlWire struct {
	KeyID     string         `json:"key_id"`
	KeyPrefix string         `json:"key_prefix"`
	Scenario  mock.Scenario  `json:"scenario"`
	Params    map[string]any `json:"params"`
}

func controlServer(t *testing.T, e mock.Engine, wrap func(http.Handler) http.Handler) *httptest.Server {
	t.Helper()
	r := gin.New()
	mock.RegisterControlRoutes(r, e)
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

func controlHTTP(t *testing.T, s *httptest.Server, method, path, body, media string) (*http.Response, []byte) {
	t.Helper()
	r, err := http.NewRequestWithContext(t.Context(), method, s.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	r.Header.Set("Content-Type", media)
	resp, err := s.Client().Do(r)
	require.NoError(t, err)
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	return resp, data
}

func putControl(t *testing.T, s *httptest.Server, key string, scenario mock.Scenario, params string) controlWire {
	t.Helper()
	body := fmt.Sprintf(`{"key":%q,"scenario":%q`, key, scenario)
	if params != "" {
		body += `,"params":` + params
	}
	resp, data := controlHTTP(t, s, http.MethodPut, "/admin/scenario", body+"}", "application/json; charset=utf-8")
	require.Equal(t, 200, resp.StatusCode, "%s", data)
	var config controlWire
	require.NoError(t, json.Unmarshal(data, &config))
	return config
}

func getControl(t *testing.T, s *httptest.Server) []controlWire {
	t.Helper()
	resp, data := controlHTTP(t, s, http.MethodGet, "/admin/scenarios", "", "")
	require.Equal(t, 200, resp.StatusCode)
	var list struct {
		Scenarios []controlWire `json:"scenarios"`
	}
	require.NoError(t, json.Unmarshal(data, &list))
	require.NotNil(t, list.Scenarios, "initial list must be [] rather than null")
	return list.Scenarios
}

func assertControlParams(t *testing.T, config controlWire, key string, scenario mock.Scenario, p mock.ScenarioParams) {
	t.Helper()
	assert.Equal(t, fmt.Sprintf("%x", sha256.Sum256([]byte(key))), config.KeyID)
	assert.Equal(t, scenario, config.Scenario)
	assert.Len(t, config.Params, 7, "responses must include the complete parameters")
	for field, expected := range map[string]int{
		"chunks": p.Chunks, "fail_after_chunks": p.FailAfterChunks,
		"prompt_tokens": p.PromptTokens, "completion_tokens": p.CompletionTokens,
	} {
		// Compare raw numeric JSON in precision tests below; small values
		// here arrive through the independent HTTP wire representation.
		assert.Equal(t, float64(expected), config.Params[field], field)
	}
	assert.Equal(t, p.DowntimeMode, config.Params["downtime_mode"])
	assert.Equal(t, p.ChunkInterval.String(), config.Params["chunk_interval"])
	assert.Equal(t, p.TTFBDelay.String(), config.Params["ttfb_delay"])
}

// 12.2 §5.1①②: every scenario, all three patch forms, default rebuilding,
// explicit zero and submillisecond precision (including canonical output).
func TestControlReplacementDefaultsAndZeros(t *testing.T) {
	e := mock.NewEngine()
	s := controlServer(t, e, nil)
	for _, scenario := range []mock.Scenario{mock.Normal, mock.Slow, mock.Downtime, mock.RateLimited, mock.Interrupted} {
		for _, params := range []string{"", "{}", `{"ttfb_delay":"5s","prompt_tokens":7}`} {
			t.Run(string(scenario)+"/"+params, func(t *testing.T) {
				putControl(t, s, chatKey, scenario, `{"ttfb_delay":"5s","chunks":9,"completion_tokens":99}`)
				config := putControl(t, s, chatKey, scenario, params)
				want := mock.DefaultScenarioParams()
				if params != "" && params != "{}" {
					want.TTFBDelay, want.PromptTokens = 5*time.Second, 7
				}
				assertControlParams(t, config, chatKey, scenario, want)
				storedScenario, storedParams := e.Get(chatKey)
				assert.Equal(t, scenario, storedScenario)
				assert.Equal(t, want, storedParams)
			})
		}
	}
	p := mock.DefaultScenarioParams()
	p.Chunks, p.ChunkInterval, p.TTFBDelay, p.FailAfterChunks, p.PromptTokens, p.CompletionTokens = 1, 0, 0, 0, 0, 0
	got := putControl(t, s, chatKey, mock.Slow, `{"chunks":1,"chunk_interval":"0s","ttfb_delay":"0s","fail_after_chunks":0,"prompt_tokens":0,"completion_tokens":0}`)
	assertControlParams(t, got, chatKey, mock.Slow, p)
	p = mock.DefaultScenarioParams()
	p.ChunkInterval, p.TTFBDelay = 500*time.Microsecond, 1500*time.Microsecond
	got = putControl(t, s, chatKey, mock.Normal, `{"chunk_interval":"500us","ttfb_delay":"1.5ms"}`)
	assertControlParams(t, got, chatKey, mock.Normal, p)
}

type setCountingEngine struct {
	mock.Engine
	calls atomic.Int32
}

func (e *setCountingEngine) Set(key string, scenario mock.Scenario, p mock.ScenarioParams) error {
	e.calls.Add(1)
	return e.Engine.Set(key, scenario, p)
}

func TestControlRejectsInvalidRequestsWithoutSet(t *testing.T) {
	e := &setCountingEngine{Engine: mock.NewEngine()}
	s := controlServer(t, e, nil)
	putControl(t, s, chatKey, mock.Slow, `{"ttfb_delay":"5s"}`)
	before, calls := e.Snapshot(), e.calls.Load()
	base := fmt.Sprintf(`{"key":%q,"scenario":"normal"}`, chatKey)
	cases := []struct{ name, body string }{
		{"empty", ""}, {"malformed", "{"}, {"root null", "null"}, {"root array", "[]"},
		{"root number", "1"}, {"root string", `"str"`}, {"root bool", "true"},
		{"missing key", `{"scenario":"normal"}`}, {"missing scenario", `{"key":"test"}`},
		{"unknown scenario", `{"key":"test","scenario":"unknown"}`},
		{"empty scenario", `{"key":"test","scenario":""}`},
		{"scenario case", `{"key":"test","scenario":"Normal"}`},
		{"duplicate key", `{"key":"test","key":"test2","scenario":"normal"}`},
		{"duplicate scenario", `{"key":"test","scenario":"normal","scenario":"slow"}`},
		{"duplicate params", `{"key":"test","scenario":"normal","params":{},"params":{}}`},
		{"escaped duplicate", `{"key":"test","k\u0065y":"test","scenario":"normal"}`},
		{"unknown root", strings.TrimSuffix(base, "}") + `,"private-body-marker":1}`},
		{"case mismatch", `{"Key":"test","scenario":"normal"}`},
		{"second object", base + " {}"}, {"second null", base + " null"}, {"trailing junk", base + " junk"},
	}
	for _, key := range []string{"", " ", " test", "test ", "te st", "te\tst", "te\nst", "te\x00st", "te\u00a0st"} {
		encoded, err := json.Marshal(key)
		require.NoError(t, err)
		cases = append(cases, struct{ name, body string }{"invalid key", fmt.Sprintf(`{"key":%s,"scenario":"normal"}`, encoded)})
	}
	for _, field := range []string{"key", "scenario", "params"} {
		for _, raw := range []string{"null", "0", "true", "[]", "{}", `"text"`} {
			if (field != "params" && raw == `"text"`) || (field == "params" && raw == "{}") {
				continue
			}
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(base), &fields))
			fields[field] = json.RawMessage(raw)
			body, err := json.Marshal(fields)
			require.NoError(t, err)
			cases = append(cases, struct{ name, body string }{field + "/" + raw, string(body)})
		}
	}
	maxInt := int(^uint(0) >> 1)
	for _, field := range []string{"chunks", "fail_after_chunks", "prompt_tokens", "completion_tokens", "chunk_interval", "ttfb_delay", "downtime_mode"} {
		bad := []string{"null", "false", "[]", "{}"}
		switch field {
		case "chunk_interval", "ttfb_delay":
			bad = append(bad, "0", `""`, `"-1ns"`, `"bad"`, `"999999999999h"`)
		case "downtime_mode":
			bad = append(bad, "0", `""`, `"reset"`)
		default:
			bad = append(bad, "-1", "1.5", "1.0", "1e0", `"1"`, strconv.FormatUint(uint64(maxInt)+1, 10))
			if field == "chunks" {
				bad = append(bad, "0")
			}
		}
		for _, raw := range bad {
			cases = append(cases, struct{ name, body string }{field + "/" + raw, strings.TrimSuffix(base, "}") + fmt.Sprintf(`,"params":{%q:%s}}`, field, raw)})
		}
	}
	for _, params := range []string{
		`{"chunks":4,"chunks":5}`, `{"chunks":4,"ch\u0075nks":5}`, `{"unknown":1}`,
		fmt.Sprintf(`{"prompt_tokens":%d,"completion_tokens":1}`, maxInt),
		fmt.Sprintf(`{"prompt_tokens":1,"completion_tokens":%d}`, maxInt),
	} {
		cases = append(cases, struct{ name, body string }{"params/" + params, strings.TrimSuffix(base, "}") + `,"params":` + params + "}"})
	}
	for _, params := range []string{`{"fail_after_chunks":0}`, `{"fail_after_chunks":8}`, `{"chunks":1}`, `{"chunks":3,"fail_after_chunks":3}`} {
		cases = append(cases, struct{ name, body string }{"interrupted bounds/" + params, `{"key":"test","scenario":"interrupted","params":` + params + "}"})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, data := controlHTTP(t, s, http.MethodPut, "/admin/scenario", tc.body, "application/json")
			assert.Equal(t, 400, resp.StatusCode, "%s", data)
			assertControlError(t, data, "mock_invalid_request")
			assert.Equal(t, before, e.Snapshot())
			assert.Equal(t, calls, e.calls.Load(), "invalid input must never call Set")
		})
	}
	for _, media := range []string{"", "text/plain", "application/problem+json", "application/json; charset"} {
		resp, data := controlHTTP(t, s, http.MethodPut, "/admin/scenario", base, media)
		assert.Equal(t, 415, resp.StatusCode)
		assertControlError(t, data, "mock_unsupported_media_type")
		assert.Equal(t, before, e.Snapshot())
		assert.Equal(t, calls, e.calls.Load())
	}
}

func assertControlError(t *testing.T, body []byte, code string) {
	t.Helper()
	var envelope struct {
		Error struct{ Code, Type, Message string }
	}
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Equal(t, code, envelope.Error.Code)
	assert.Equal(t, "mock_error", envelope.Error.Type)
	assert.NotEmpty(t, envelope.Error.Message)
	assert.NotContains(t, string(body), chatKey)
	assert.NotContains(t, string(body), "private-body-marker")
}

func TestControlBodyLimitAndIntegerPrecision(t *testing.T) {
	e := &setCountingEngine{Engine: mock.NewEngine()}
	s := controlServer(t, e, nil)
	base := `{"key":"precision","scenario":"normal"}`
	for _, chunked := range []bool{false, true} {
		for _, extra := range []int{0, 1} {
			before, calls := e.Snapshot(), e.calls.Load()
			body := base + strings.Repeat(" ", (64<<10)-len(base)+extra)
			r, err := http.NewRequestWithContext(t.Context(), http.MethodPut, s.URL+"/admin/scenario", strings.NewReader(body))
			require.NoError(t, err)
			r.Header.Set("Content-Type", "application/json")
			if chunked {
				r.ContentLength = -1
			}
			resp, err := s.Client().Do(r)
			require.NoError(t, err)
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.NoError(t, err)
			assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
			if extra == 0 {
				assert.Equal(t, 200, resp.StatusCode)
				assert.Equal(t, calls+1, e.calls.Load())
			} else {
				assert.Equal(t, 413, resp.StatusCode)
				assertControlError(t, data, "mock_request_too_large")
				assert.Equal(t, before, e.Snapshot())
				assert.Equal(t, calls, e.calls.Load())
			}
		}
	}
	maxInt := int(^uint(0) >> 1)
	for _, value := range []int{maxInt, maxInt - 1} {
		body := fmt.Sprintf(`{"key":"precision","scenario":"normal","params":{"chunks":%d,"fail_after_chunks":%d,"prompt_tokens":%d,"completion_tokens":0}}`, value, value, value)
		resp, data := controlHTTP(t, s, http.MethodPut, "/admin/scenario", body, "application/json")
		require.Equal(t, 200, resp.StatusCode)
		// Inspect only numeric fields without coercing the response to float64.
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(data, &fields))
		var params map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(fields["params"], &params))
		for _, field := range []string{"chunks", "fail_after_chunks", "prompt_tokens"} {
			assert.Equal(t, strconv.Itoa(value), string(params[field]))
		}
		_, p := e.Get("precision")
		assert.Equal(t, value, p.Chunks)
		assert.Equal(t, value, p.PromptTokens)
	}
}

func TestControlListFingerprintsAndReadIsolation(t *testing.T) {
	e := mock.NewEngine()
	s := controlServer(t, e, nil)
	assert.Empty(t, getControl(t, s))
	resp := doChat(t, s, false, false, "unconfigured", "")
	_, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Empty(t, getControl(t, s), "ordinary default reads must not create entries")
	keys := []string{"sk-mock-a", "sk-mock-b", "a", "12345678", "测试标识", "测试标识甲乙丙丁戊"}
	var expected []controlWire
	for _, key := range keys {
		got := putControl(t, s, key, mock.Normal, "")
		assertControlParams(t, got, key, mock.Normal, mock.DefaultScenarioParams())
		if len([]rune(key)) <= 8 {
			assert.Empty(t, got.KeyPrefix)
		} else {
			assert.Equal(t, string([]rune(key)[:8]), got.KeyPrefix)
		}
		expected = append(expected, got)
	}
	sort.Slice(expected, func(i, j int) bool { return expected[i].KeyID < expected[j].KeyID })
	assert.Equal(t, expected, getControl(t, s))
	_, data := controlHTTP(t, s, http.MethodGet, "/admin/scenarios", "", "")
	assert.NotContains(t, string(data), `"key":`)
	for _, key := range keys {
		assert.NotContains(t, string(data), strconv.Quote(key))
	}
	list := getControl(t, s)
	list[0].Params["chunks"] = float64(999)
	list[0].Scenario = mock.Downtime
	assert.Equal(t, expected, getControl(t, s), "editing decoded responses cannot mutate Engine")
	for _, tc := range []struct{ method, path string }{
		{http.MethodDelete, "/admin/scenario"}, {http.MethodPatch, "/admin/scenario"},
		{http.MethodGet, "/admin/scenario"}, {http.MethodGet, "/admin/scenarios/a"},
	} {
		r, err := http.NewRequestWithContext(t.Context(), tc.method, s.URL+tc.path, nil)
		require.NoError(t, err)
		resp, err := s.Client().Do(r)
		require.NoError(t, err)
		assert.Equal(t, 404, resp.StatusCode)
		resp.Body.Close()
	}
}

func TestControlChatSharedEngineAndHeaderIsolation(t *testing.T) {
	e := mock.NewEngine()
	s := controlServer(t, e, nil)
	putControl(t, s, "sk-control-a", mock.Downtime, `{"chunks":2,"chunk_interval":"0s","prompt_tokens":7,"completion_tokens":9}`)
	putControl(t, s, "sk-control-b", mock.RateLimited, "")
	before := e.Snapshot()
	assertJSONError(t, doChat(t, s, false, false, "sk-control-a", ""), 503, "mock_unavailable")
	assertJSONError(t, doChat(t, s, false, false, "sk-control-b", ""), 429, "mock_rate_limited")
	resp := doChat(t, s, true, true, "sk-control-a", "normal")
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	events := readEvents(t, data)
	require.Len(t, events, 4)
	assertUsage(t, events[2].data.Usage, 7, 9)
	assert.True(t, events[3].done)
	assert.Equal(t, before, e.Snapshot())
	assertJSONError(t, doChat(t, s, false, false, "sk-control-a", "unknown"), 400, "mock_invalid_request")
	putControl(t, s, "sk-control-a", mock.Normal, "")
	resp = doChat(t, s, false, false, "sk-control-a", "")
	var completion wireCompletion
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&completion))
	require.Len(t, completion.Choices, 1)
	assert.Equal(t, "mock-1 mock-2 mock-3 mock-4 mock-5 mock-6 mock-7 mock-8 ", completion.Choices[0].Message["content"])
	assertUsage(t, completion.Usage, 12, 34)
	assertJSONError(t, doChat(t, s, false, false, "sk-control-b", ""), 429, "mock_rate_limited")
}

func TestControlPUTKeepsInflightSnapshots(t *testing.T) {
	for _, phase := range []string{"slow", "stream"} {
		t.Run(phase, func(t *testing.T) {
			release, entered := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			// Release before the server cleanup even on a failed assertion.
			e := mock.NewEngine()
			engine := e
			var wrap func(http.Handler) http.Handler
			if phase == "slow" {
				engine = &gatedEngine{Engine: e, resolved: entered, release: release}
			} else {
				var first sync.Once
				wrap = func(next http.Handler) http.Handler {
					return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == chatPath {
							w = &responseProbe{ResponseWriter: w, onFlush: func() { first.Do(func() { close(entered); <-release }) }}
						}
						next.ServeHTTP(w, r)
					})
				}
			}
			s := controlServer(t, engine, wrap)
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			scenario := mock.Normal
			if phase == "slow" {
				scenario = mock.Slow
			}
			putControl(t, s, chatKey, scenario, `{"chunks":4,"chunk_interval":"0s","ttfb_delay":"100ms","prompt_tokens":77,"completion_tokens":8}`)
			r := chatHTTPRequest(t, t.Context(), s, chatBody(true, true), chatKey, "")
			result := make(chan httpResult, 1)
			go func() { resp, err := s.Client().Do(r); result <- httpResult{resp: resp, err: err} }()
			awaitSignal(t, entered, "old HTTP request snapshot or first flush")
			putControl(t, s, chatKey, mock.Downtime, "")
			assertJSONError(t, doChat(t, s, false, false, chatKey, ""), 503, "mock_unavailable")
			started := time.Now()
			releaseOnce.Do(func() { close(release) })
			got := <-result
			require.NoError(t, got.err)
			defer got.resp.Body.Close()
			data, err := io.ReadAll(bufio.NewReader(got.resp.Body))
			require.NoError(t, err)
			assert.Equal(t, 200, got.resp.StatusCode)
			if phase == "slow" {
				assert.GreaterOrEqual(t, time.Since(started), 90*time.Millisecond)
			}
			events := readEvents(t, data)
			require.Len(t, events, 6)
			assertUsage(t, events[4].data.Usage, 77, 8)
			assert.True(t, events[5].done)
			for i := range 4 {
				assert.Equal(t, fmt.Sprintf("mock-%d ", i+1), events[i].data.Choices[0].Delta["content"])
			}
		})
	}
}

type failingSetEngine struct {
	mock.Engine
	failure error
}

type gatedSetEngine struct {
	mock.Engine
	entered chan struct{}
	release <-chan struct{}
}

func (e *gatedSetEngine) Set(key string, scenario mock.Scenario, p mock.ScenarioParams) error {
	close(e.entered)
	<-e.release
	return e.Engine.Set(key, scenario, p)
}

func TestControlSuccessWaitsForSetCompletion(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	e := &gatedSetEngine{Engine: mock.NewEngine(), entered: make(chan struct{}), release: release}
	var writes atomic.Int32
	s := controlServer(t, e, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/admin/scenario" {
				w = &responseProbe{ResponseWriter: w, onWrite: func() { writes.Add(1) }}
			}
			next.ServeHTTP(w, r)
		})
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	r, err := http.NewRequestWithContext(t.Context(), http.MethodPut, s.URL+"/admin/scenario", strings.NewReader(`{"key":"sk-set-barrier","scenario":"downtime"}`))
	require.NoError(t, err)
	r.Header.Set("Content-Type", "application/json")
	result := make(chan httpResult, 1)
	go func() { resp, err := s.Client().Do(r); result <- httpResult{resp: resp, err: err} }()
	awaitSignal(t, e.entered, "control Set entry")
	assert.Zero(t, writes.Load(), "no response bytes or headers before Set completes")
	assert.Empty(t, e.Snapshot())
	resp := doChat(t, s, false, false, "sk-set-barrier", "")
	assert.Equal(t, 200, resp.StatusCode)
	_, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	releaseOnce.Do(func() { close(release) })
	got := <-result
	require.NoError(t, got.err)
	defer got.resp.Body.Close()
	_, err = io.ReadAll(got.resp.Body)
	require.NoError(t, err)
	assert.Equal(t, 200, got.resp.StatusCode)
	assert.Equal(t, "no-store", got.resp.Header.Get("Cache-Control"))
	assertJSONError(t, doChat(t, s, false, false, "sk-set-barrier", ""), 503, "mock_unavailable")
}

func (e *failingSetEngine) Set(string, mock.Scenario, mock.ScenarioParams) error { return e.failure }

func TestControlSetErrorsAreClassifiedAndPrivate(t *testing.T) {
	// Obtain a genuine wrapped Engine validation error, without exporting a new
	// sentinel or adding any requirement to the frozen Engine interface.
	invalid := mock.NewEngine().Set("bad key", mock.Normal, mock.DefaultScenarioParams())
	require.Error(t, invalid)
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"validation", fmt.Errorf("wrapped: %w", invalid), 400, "mock_invalid_request"},
		{"runtime", errors.New(chatKey + " private-body-marker"), 500, "mock_internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			e := &failingSetEngine{Engine: mock.NewEngine(), failure: tc.err}
			r := gin.New()
			r.Use(mock.RequestLog(mock.NewLogger(&logs)))
			mock.RegisterControlRoutes(r, e)
			s := httptest.NewServer(r)
			defer s.Close()
			resp, data := controlHTTP(t, s, http.MethodPut, "/admin/scenario", fmt.Sprintf(`{"key":%q,"scenario":"normal"}`, chatKey), "application/json")
			assert.Equal(t, tc.status, resp.StatusCode)
			assertControlError(t, data, tc.code)
			assert.Empty(t, e.Snapshot())
			s.Close()
			assert.NotContains(t, logs.String(), chatKey)
			assert.NotContains(t, logs.String(), "private-body-marker")
			if tc.status == 500 {
				assert.Contains(t, logs.String(), `"level":"ERROR"`)
			}
			assert.Contains(t, logs.String(), `"request_id":"req_`)
			assert.Contains(t, logs.String(), `"ts":`)
		})
	}
}

func TestControlConcurrentPUTGETAndChat(t *testing.T) {
	s := controlServer(t, mock.NewEngine(), nil)
	const key = "sk-control-concurrent"
	putControl(t, s, key, mock.Normal, `{"chunks":1,"chunk_interval":"0s","ttfb_delay":"0s","fail_after_chunks":0,"prompt_tokens":11,"completion_tokens":22}`)
	a := `{"key":"sk-control-concurrent","scenario":"normal","params":{"chunks":1,"chunk_interval":"0s","ttfb_delay":"0s","fail_after_chunks":0,"prompt_tokens":11,"completion_tokens":22}}`
	b := `{"key":"sk-control-concurrent","scenario":"normal","params":{"chunks":2,"chunk_interval":"1ns","ttfb_delay":"2ns","downtime_mode":"refuse","fail_after_chunks":1,"prompt_tokens":33,"completion_tokens":44}}`
	expectedA := map[string]any{
		"chunks": float64(1), "chunk_interval": "0s", "ttfb_delay": "0s", "downtime_mode": "503",
		"fail_after_chunks": float64(0), "prompt_tokens": float64(11), "completion_tokens": float64(22),
	}
	expectedB := map[string]any{
		"chunks": float64(2), "chunk_interval": "1ns", "ttfb_delay": "2ns", "downtime_mode": "refuse",
		"fail_after_chunks": float64(1), "prompt_tokens": float64(33), "completion_tokens": float64(44),
	}
	var group errgroup.Group
	start := make(chan struct{})
	for worker := range 12 {
		group.Go(func() error {
			<-start
			for i := range 30 {
				method, path, body := http.MethodGet, "/admin/scenarios", ""
				if worker%3 == 0 {
					method, path, body = http.MethodPut, "/admin/scenario", a
					if i%2 == 1 {
						body = b
					}
				}
				if worker%3 == 2 {
					method, path, body = http.MethodPost, chatPath, chatBody(false, false)
				}
				r, err := http.NewRequestWithContext(t.Context(), method, s.URL+path, strings.NewReader(body))
				if err != nil {
					return err
				}
				r.Header.Set("Content-Type", "application/json")
				r.Header.Set("Authorization", "Bearer "+key)
				resp, err := s.Client().Do(r)
				if err != nil {
					return err
				}
				data, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || resp.StatusCode != 200 {
					return fmt.Errorf("HTTP %d, read: %v", resp.StatusCode, err)
				}
				if worker%3 == 2 {
					var completion wireCompletion
					if err := json.Unmarshal(data, &completion); err != nil {
						return err
					}
					var u wireUsage
					if err := json.Unmarshal(completion.Usage, &u); err != nil {
						return err
					}
					if len(completion.Choices) != 1 {
						return errors.New("missing completion choice")
					}
					content := completion.Choices[0].Message["content"]
					matchesA := content == "mock-1 " && u == (wireUsage{11, 22, 33})
					matchesB := content == "mock-1 mock-2 " && u == (wireUsage{33, 44, 77})
					if !matchesA && !matchesB {
						return errors.New("chat read mixed configuration")
					}
				} else {
					var config controlWire
					if worker%3 == 1 {
						var list struct {
							Scenarios []controlWire `json:"scenarios"`
						}
						if err := json.Unmarshal(data, &list); err != nil {
							return err
						}
						if len(list.Scenarios) != 1 {
							return errors.New("GET lost explicit key")
						}
						config = list.Scenarios[0]
					} else if err := json.Unmarshal(data, &config); err != nil {
						return err
					}
					if config.Scenario != mock.Normal || (!reflect.DeepEqual(expectedA, config.Params) && !reflect.DeepEqual(expectedB, config.Params)) {
						return errors.New("control read mixed configuration")
					}
					if worker%3 == 0 {
						want := expectedA
						if body == b {
							want = expectedB
						}
						if !reflect.DeepEqual(want, config.Params) {
							return errors.New("PUT returned a later writer's configuration")
						}
					}
				}
			}
			return nil
		})
	}
	close(start)
	require.NoError(t, group.Wait())
}
