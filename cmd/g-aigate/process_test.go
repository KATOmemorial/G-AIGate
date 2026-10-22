package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/access"
	"gaigate/internal/store"
)

const processChat = `{"model":"gpt-4o-demo","messages":[{"role":"user","content":"hi"}],"stream":true}`
const probeChat = `{"model":"owned-probe","messages":[{"role":"user","content":"hi"}],"stream":true}`

func frameCounts(t *testing.T, data []byte) (int, int, int) {
	t.Helper()
	content, usage, done := 0, 0, 0
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if line == "" {
			continue
		}
		require.True(t, strings.HasPrefix(line, "data: "), "unexpected SSE line")
		payload := strings.TrimPrefix(line, "data: ")
		if payload == "[DONE]" {
			done++
			continue
		}
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(payload), &fields))
		if raw, ok := fields["usage"]; ok && string(raw) != "null" {
			usage++
		} else {
			content++
		}
	}
	return content, usage, done
}

type processCurlResult struct {
	code         int
	status       int
	first, total float64
	body         []byte
	requestID    string
}

func curlProcess(t *testing.T, f *gatewayProcessFixture, base, raw, body string) processCurlResult {
	t.Helper()
	name := "curl"
	if runtime.GOOS == "windows" {
		name = "curl.exe"
	}
	binary, err := exec.LookPath(name)
	require.NoError(t, err)
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--disable", "--silent", "--noproxy", "*", "--http1.1", "--no-buffer", "--connect-timeout", "2", "--max-time", "10",
		"--config", "-", "--request", "POST", "--header", "Content-Type: application/json", "--data-binary", body,
		"--output", filepath.Join(dir, "body"), "--dump-header", filepath.Join(dir, "headers"), "--write-out", "%{http_code} %{time_starttransfer} %{time_total}\n", "--url", base+"/v1/chat/completions")
	command.Stdin = strings.NewReader(fmt.Sprintf("header = \"Authorization: Bearer %s\"\n", raw))
	var stats, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stats, &stderr
	err = command.Run()
	require.NoError(t, ctx.Err(), "fixture watchdog must not terminate curl")
	result := processCurlResult{}
	if err != nil {
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		result.code = exit.ExitCode()
	}
	_, err = fmt.Sscan(stats.String(), &result.status, &result.first, &result.total)
	require.NoError(t, err)
	result.body, err = os.ReadFile(filepath.Join(dir, "body"))
	require.NoError(t, err)
	headers, err := os.ReadFile(filepath.Join(dir, "headers"))
	require.NoError(t, err)
	for _, line := range strings.Split(string(headers), "\n") {
		name, value, _ := strings.Cut(line, ":")
		if strings.EqualFold(name, "X-Request-Id") {
			result.requestID = strings.TrimSpace(value)
		}
	}
	assert.False(t, strings.Contains(stderr.String(), raw), "curl diagnostics leaked credentials")
	return result
}

func configureOwnedMock(t *testing.T, f *gatewayProcessFixture, scenario string, params string) {
	t.Helper()
	if params == "" {
		params = "{}"
	}
	body := `{"key":"sk-mock-a","scenario":"` + scenario + `","params":` + params + `}`
	resp, data, err := gatewayRequest(t, f.client, f.mockURL, "", "PUT", "/admin/scenario", body)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, "owned mock control failed: %s", data)
}

type upstreamObservation struct {
	mode       int32
	model      string
	fields     map[string]json.RawMessage
	mockHeader string
	emitted    int
	canceled   bool
}
type processUpstream struct {
	server         *httptest.Server
	mode           atomic.Int32
	entered, ended chan upstreamObservation
	release        chan struct{}
	once           sync.Once
	raw            string
}

const (
	probeNormal int32 = iota
	probe401
	probe403
	probe400
	probe404
	probe422
	probe429
	probe503
	probeTruncated
	probeHTML
	probeClientEcho
	probeProviderEcho
	probeHeaders
	probeFirstFrame
	probeAfterFrame
	probeSlowReader
	probeConcurrent
)

func newProcessUpstream(t *testing.T, raw string) *processUpstream {
	t.Helper()
	u := &processUpstream{entered: make(chan upstreamObservation, 128), ended: make(chan upstreamObservation, 128), release: make(chan struct{}), raw: raw}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observation := upstreamObservation{mode: u.mode.Load()}
		defer func() { u.ended <- observation }()
		if err := json.NewDecoder(r.Body).Decode(&observation.fields); err != nil {
			t.Error("cannot decode owned upstream request")
			w.WriteHeader(500)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = json.Unmarshal(observation.fields["model"], &observation.model)
		observation.mockHeader = r.Header.Get("X-Mock-Scenario")
		if r.Header.Get("Authorization") != "Bearer owned-upstream-key" {
			t.Error("gateway did not use channel credentials")
			w.WriteHeader(500)
			return
		}
		u.entered <- observation
		w.Header().Set("Content-Type", "application/json")
		safe := `{"error":{"message":"bad input","type":"invalid_request_error","param":null,"code":"owned_error"},"private":"discard"}`
		switch observation.mode {
		case probe401, probe403, probe400, probe404, probe422, probe429, probe503:
			status := map[int32]int{probe401: 401, probe403: 403, probe400: 400, probe404: 404, probe422: 422, probe429: 429, probe503: 503}[observation.mode]
			w.Header().Set("X-Private-Upstream", "discard")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, safe)
		case probeTruncated:
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"error":{"message":"`+strings.Repeat("x", 1500)+`","type":"invalid_request_error"}}`)
		case probeHTML:
			w.WriteHeader(400)
			_, _ = io.WriteString(w, "<html>upstream diagnostics</html>")
		case probeClientEcho:
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"error":{"message":"\u0073`+u.raw[1:]+`","type":"invalid_request_error"}}`)
		case probeProviderEcho:
			w.WriteHeader(400)
			_, _ = io.WriteString(w, strings.ReplaceAll(safe, "bad input", "owned-upstream-key"))
		case probeHeaders, probeFirstFrame, probeAfterFrame:
			if observation.mode != probeHeaders {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(200)
				if observation.mode == probeAfterFrame {
					_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
				}
				w.(http.Flusher).Flush()
			}
			select {
			case <-r.Context().Done():
				observation.canceled = true
			case <-time.After(7 * time.Second):
				t.Error("owned upstream cancellation watchdog expired")
			}
		case probeSlowReader:
			w.Header().Set("Content-Type", "text/event-stream")
			_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(10 * time.Second))
			frame := "data: {\"choices\":[{\"delta\":{\"content\":\"" + strings.Repeat("x", 768*1024) + "\"}}]}\n\n"
			for range 80 {
				if r.Context().Err() != nil {
					observation.canceled = true
					return
				}
				if _, err := io.WriteString(w, frame); err != nil {
					observation.canceled = true
					return
				}
				w.(http.Flusher).Flush()
				observation.emitted++
			}
		case probeConcurrent:
			select {
			case <-u.release:
			case <-r.Context().Done():
				observation.canceled = true
				return
			}
			_, _ = io.WriteString(w, `{"choices":[]}`)
		default:
			if string(observation.fields["stream"]) == "true" {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"transparent\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":34}}\n\ndata: [DONE]\n\n")
			} else {
				_, _ = io.WriteString(w, `{"choices":[],"opaque":{"n":9007199254740993}}`)
			}
		}
	}))
	t.Cleanup(func() { u.once.Do(func() { close(u.release) }); u.server.Close() })
	return u
}

func waitUpstreamObservation(t *testing.T, events <-chan upstreamObservation, mode int32) upstreamObservation {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.mode == mode {
				return event
			}
		case <-timer.C:
			t.Fatal("owned upstream stage/exit not observed")
			return upstreamObservation{}
		}
	}
}

func processEnvelope(t *testing.T, resp *http.Response, body []byte, status int, code string) {
	t.Helper()
	require.Equal(t, status, resp.StatusCode)
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		}
		RequestID string `json:"request_id"`
	}
	require.NoError(t, json.Unmarshal(body, &envelope))
	assert.Equal(t, code, envelope.Error.Code)
	assert.Equal(t, resp.Header.Get("X-Request-Id"), envelope.RequestID)
}

func TestGatewayProcessAcceptance(t *testing.T) {
	f := newProcessFixture(t)
	gateway, base := f.startGateway(t, time.Second, 3*time.Second)
	var initialRows int64
	require.NoError(t, f.db.Model(&store.APIKey{}).Count(&initialRows).Error)
	require.Zero(t, initialRows, "startup seed must not issue user Keys")
	created := f.issueKey(t)
	second := f.issueKey(t)
	upstream := newProcessUpstream(t, created.Key)
	probeChannel := gatewayChannel(t, f.db, f.master, "owned-probe", "owned-probe", "up-probe", upstream.server.URL, "owned-upstream-key", 20)
	models := store.NewModelRepo(f.db, store.NewConfigCache(0))
	require.NoError(t, models.Create(context.Background(), &store.ModelAlias{Name: "owned-probe"}))
	post := func(body string, status int, code string) (*http.Response, []byte) {
		t.Helper()
		resp, data, err := gatewayRequest(t, f.client, base, created.Key, "POST", "/v1/chat/completions", body)
		require.NoError(t, err)
		require.False(t, bytes.Contains(data, []byte(created.Key)), "credential leaked into response")
		if code != "" {
			processEnvelope(t, resp, data, status, code)
		} else {
			assert.Equal(t, status, resp.StatusCode)
		}
		_ = summaryFor(t, gateway.logs, resp)
		return resp, data
	}

	t.Run("M0 script and immutable caller state", func(t *testing.T) {
		resp, before, err := gatewayRequest(t, f.client, f.mockURL, "", "GET", "/admin/scenarios", "")
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		temp := t.TempDir()
		code, out, stderr := runM0Script(t, f, base, created.Key, temp)
		require.Zero(t, code, "M0 script failed: %s", stderr)
		assert.Empty(t, stderr)
		assert.Equal(t, 7, strings.Count(out, "[PASS]"))
		assert.False(t, strings.Contains(out+stderr, created.Key))
		t.Logf("M0 script output:\n%s", out)
		files, err := os.ReadDir(temp)
		require.NoError(t, err)
		assert.Empty(t, files, "script must clean temporary files")
		_, after, err := gatewayRequest(t, f.client, f.mockURL, "", "GET", "/admin/scenarios", "")
		require.NoError(t, err)
		assert.JSONEq(t, string(before), string(after), "script cannot mutate mock scenarios")
		keys, err := store.NewAPIKeyRepo(f.db).ListByUser(context.Background(), 1)
		require.NoError(t, err)
		require.Len(t, keys, 2)
		for _, key := range keys {
			assert.Equal(t, store.APIKeyStatusActive, key.Status)
		}
		code, _, stderr = runM0Script(t, f, base, "", temp)
		assert.Equal(t, 1, code)
		assert.Contains(t, stderr, "GAIGATE_E2E_API_KEY")
		code, _, stderr = runM0Script(t, f, "http://"+processPort(t), created.Key, temp)
		assert.Contains(t, []int{7, 28}, code, "closed-port curl result is platform dependent")
		assert.Contains(t, stderr, fmt.Sprintf("curl=%d", code), "script must preserve the actual curl transport result")
		t.Logf("M0 negative transport preserved curl exit=%d", code)
		configureOwnedMock(t, f, "interrupted", "")
		code, _, stderr = runM0Script(t, f, base, created.Key, temp)
		assert.Contains(t, []int{18, 56}, code)
		assert.Contains(t, stderr, fmt.Sprintf("curl=%d", code), "script must preserve incomplete-transfer result")
		configureOwnedMock(t, f, "normal", "")
		files, err = os.ReadDir(temp)
		require.NoError(t, err)
		assert.Empty(t, files)
	})
	t.Run("identity validation and discovery", func(t *testing.T) {
		before := len(processLogEntries(t, f.mock.logs))
		for _, tc := range []struct {
			body, media string
			status      int
			code        string
		}{
			{`{"model":"gpt-4o-demo","model":"x","messages":[{"role":"user"}]}`, "application/json", 400, "gaigate_invalid_request"},
			{`{"model":"gpt-4o-demo","messages":[{"role":"user"}],"stream":null}`, "application/json", 400, "gaigate_invalid_request"},
			{`{"model":"gpt-4o-demo","messages":[{"role":"user"}],"stream_options":{"include_usage":null}}`, "application/json", 400, "gaigate_invalid_request"},
			{processChat, "text/plain", 415, "gaigate_unsupported_media_type"},
			{strings.Repeat(" ", 10*1024*1024+1), "application/json", 413, "gaigate_request_too_large"},
		} {
			req, err := http.NewRequest("POST", base+"/v1/chat/completions", strings.NewReader(tc.body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", tc.media)
			req.Header.Set("Authorization", "Bearer "+created.Key)
			resp, err := f.client.Do(req)
			require.NoError(t, err)
			data, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			_ = resp.Body.Close()
			processEnvelope(t, resp, data, tc.status, tc.code)
		}
		assert.Equal(t, before, len(processLogEntries(t, f.mock.logs)), "invalid input must make zero upstream calls")
		resp, data, err := gatewayRequest(t, f.client, base, "", "POST", "/v1/chat/completions", strings.Repeat("x", 11*1024*1024))
		require.NoError(t, err)
		processEnvelope(t, resp, data, 401, "gaigate_unauthorized")
		bad, _, _, err := access.GenerateKey()
		require.NoError(t, err)
		resp, data, err = gatewayRequest(t, f.client, base, bad, "POST", "/v1/chat/completions", processChat)
		require.NoError(t, err)
		processEnvelope(t, resp, data, 401, "gaigate_unauthorized")
		valid := `{"model":"gpt-4o-demo","messages":[{"role":"user"}],"stream":false}`
		valid += strings.Repeat(" ", 10*1024*1024-len(valid))
		post(valid, 200, "")
		expired := time.Now().UTC().Add(-time.Hour)
		require.NoError(t, f.db.Model(&store.APIKey{}).Where("id = ?", created.ID).Update("expired_at", expired).Error)
		post(processChat, 403, "gaigate_key_expired")
		require.NoError(t, f.db.Model(&store.APIKey{}).Where("id = ?", created.ID).Update("expired_at", nil).Error)
		owner, err := store.NewUserRepo(f.db).FindByUsername(context.Background(), "admin")
		require.NoError(t, err)
		require.NoError(t, f.db.Model(&store.User{}).Where("id = ?", owner.ID).Update("status", 0).Error)
		post(processChat, 403, "gaigate_user_disabled")
		require.NoError(t, f.db.Model(&store.User{}).Where("id = ?", owner.ID).Update("status", 1).Error)
		require.NoError(t, models.Create(context.Background(), &store.ModelAlias{Name: "configured-no-channel"}))
		post(`{"model":"not-configured","messages":[{"role":"user"}]}`, 404, "gaigate_model_not_found")
		post(`{"model":"configured-no-channel","messages":[{"role":"user"}]}`, 503, "gaigate_no_candidate")
		var primary store.Channel
		require.NoError(t, f.db.Where("name = ?", "demo-primary").First(&primary).Error)
		primaryKeys, err := store.NewKeyRepo(f.db).ListByChannel(context.Background(), int64(primary.ID))
		require.NoError(t, err)
		require.Len(t, primaryKeys, 1)
		func() {
			require.NoError(t, f.db.Model(&store.ChannelKey{}).Where("id = ?", primaryKeys[0].ID).Update("status", 0).Error)
			defer func() {
				require.NoError(t, f.db.Model(&store.ChannelKey{}).Where("id = ?", primaryKeys[0].ID).Update("status", 1).Error)
			}()
			post(processChat, 503, "gaigate_no_candidate")
		}()
		func() {
			require.NoError(t, f.db.Model(&store.ChannelKey{}).Where("id = ?", primaryKeys[0].ID).Update("provider_key_enc", []byte("corrupt fixture ciphertext")).Error)
			defer func() {
				require.NoError(t, f.db.Model(&store.ChannelKey{}).Where("id = ?", primaryKeys[0].ID).Update("provider_key_enc", primaryKeys[0].ProviderKeyEnc).Error)
			}()
			post(processChat, 500, "gaigate_internal")
		}()
		resp, data, err = gatewayRequest(t, f.client, base, created.Key, "GET", "/v1/models", "")
		require.NoError(t, err)
		assert.Equal(t, 200, resp.StatusCode)
		assert.Contains(t, string(data), "configured-no-channel")
		assert.NotContains(t, string(data), "enabled")
		require.NoError(t, f.db.Exec("RENAME TABLE model_aliases TO m3_model_aliases").Error)
		func() {
			defer func() { require.NoError(t, f.db.Exec("RENAME TABLE m3_model_aliases TO model_aliases").Error) }()
			resp, data, err = gatewayRequest(t, f.client, base, created.Key, "GET", "/v1/models", "")
			require.NoError(t, err)
			processEnvelope(t, resp, data, 500, "gaigate_internal")
		}()
	})
	t.Run("owned mock faults interrupted and TTFB", func(t *testing.T) {
		for _, scenario := range []string{"downtime", "rate_limited"} {
			configureOwnedMock(t, f, scenario, "")
			post(processChat, 502, "gaigate_upstream_error")
		}
		configureOwnedMock(t, f, "downtime", `{"downtime_mode":"refuse"}`)
		post(processChat, 502, "gaigate_upstream_error")
		configureOwnedMock(t, f, "interrupted", "")
		got := curlProcess(t, f, base, created.Key, processChat)
		assert.Equal(t, 200, got.status)
		assert.Contains(t, []int{18, 56}, got.code)
		c, u, d := frameCounts(t, got.body)
		assert.Equal(t, 3, c)
		assert.Zero(t, u)
		assert.Zero(t, d)
		t.Logf("interrupted HTTP=%d curl=%d content=%d usage=%d DONE=%d", got.status, got.code, c, u, d)
		waitProcessLog(t, gateway.logs, 0, func(e map[string]any) bool { return e["request_id"] == got.requestID && e["msg"] == "request_done" })
		configureOwnedMock(t, f, "normal", "")
		_, data := post(processChat, 200, "")
		c, u, d = frameCounts(t, data)
		assert.Equal(t, 8, c)
		assert.Equal(t, 1, u)
		assert.Equal(t, 1, d)
		health, _, err := gatewayRequest(t, f.client, base, "", "GET", "/healthz", "")
		require.NoError(t, err)
		assert.Equal(t, 200, health.StatusCode, "same process remains healthy after interrupted and normal requests")
		configureOwnedMock(t, f, "slow", "")
		offset := len(processLogEntries(t, f.mock.logs))
		got = curlProcess(t, f, base, created.Key, processChat)
		assert.Equal(t, 502, got.status)
		assert.Zero(t, got.code)
		assert.Contains(t, string(got.body), "gaigate_upstream_timeout")
		assert.GreaterOrEqual(t, got.first, 0.75)
		assert.Less(t, got.first, 1.9)
		waitProcessLog(t, f.mock.logs, offset, func(e map[string]any) bool { return e["scenario"] == "slow" && e["outcome"] == "canceled" })
		t.Logf("slow headers=2s TTFB=1s HTTP=%d curl=%d observed=%0.6fs", got.status, got.code, got.first)
		configureOwnedMock(t, f, "normal", "")
	})
	t.Run("transparent request and safe upstream errors", func(t *testing.T) {
		body := `{"model":"owned-probe","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"c","function":{"arguments":"{}"}}]},{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:fixture"}}]}],"stream":true,"opaque":{"large":9007199254740993},"stream_options":{"include_usage":false,"extension":{"x":1}}}`
		upstream.mode.Store(probeNormal)
		post(body, 200, "")
		captured := waitUpstreamObservation(t, upstream.entered, probeNormal)
		assert.Equal(t, "up-probe", captured.model)
		var original map[string]json.RawMessage
		require.NoError(t, json.Unmarshal([]byte(body), &original))
		for _, name := range []string{"messages", "opaque"} {
			assert.JSONEq(t, string(original[name]), string(captured.fields[name]))
		}
		assert.JSONEq(t, `{"include_usage":true,"extension":{"x":1}}`, string(captured.fields["stream_options"]))
		for _, tc := range []struct {
			mode   int32
			status int
			code   string
		}{{probe401, 502, "gaigate_upstream_auth_failed"}, {probe403, 502, "gaigate_upstream_auth_failed"}, {probe400, 400, "owned_error"}, {probe404, 404, "owned_error"}, {probe422, 422, "owned_error"}, {probe429, 502, "gaigate_upstream_error"}, {probe503, 502, "gaigate_upstream_error"}, {probeTruncated, 502, "gaigate_upstream_error"}, {probeHTML, 502, "gaigate_upstream_error"}, {probeClientEcho, 502, "gaigate_upstream_error"}, {probeProviderEcho, 502, "gaigate_upstream_error"}} {
			upstream.mode.Store(tc.mode)
			resp, data := post(probeChat, tc.status, tc.code)
			assert.Empty(t, resp.Header.Get("X-Private-Upstream"))
			assert.NotContains(t, string(data), "private")
			assert.NotContains(t, string(data), "owned-upstream-key")
		}
	})
	t.Run("first frame timeout Overall after frame and cancellation", func(t *testing.T) {
		upstream.mode.Store(probeFirstFrame)
		start := time.Now()
		post(probeChat, 502, "gaigate_upstream_timeout")
		event := waitUpstreamObservation(t, upstream.ended, probeFirstFrame)
		assert.True(t, event.canceled)
		assert.GreaterOrEqual(t, time.Since(start), 750*time.Millisecond)
		assert.Less(t, time.Since(start), 2*time.Second)
		upstream.mode.Store(probeAfterFrame)
		start = time.Now()
		resp, data, err := gatewayRequest(t, f.client, base, created.Key, "POST", "/v1/chat/completions", probeChat)
		assert.Equal(t, 200, resp.StatusCode)
		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
		c, u, d := frameCounts(t, data)
		assert.Equal(t, 1, c)
		assert.Zero(t, u)
		assert.Zero(t, d)
		assert.GreaterOrEqual(t, time.Since(start), 2500*time.Millisecond)
		assert.Less(t, time.Since(start), 4*time.Second)
		_ = summaryFor(t, gateway.logs, resp)
		assert.True(t, waitUpstreamObservation(t, upstream.ended, probeAfterFrame).canceled)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "POST", base+"/v1/chat/completions", strings.NewReader(probeChat))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+created.Key)
		resp, err = f.client.Do(req)
		require.NoError(t, err)
		_, err = bufio.NewReader(resp.Body).ReadString('\n')
		require.NoError(t, err)
		start = time.Now()
		cancel()
		_ = resp.Body.Close()
		assert.True(t, waitUpstreamObservation(t, upstream.ended, probeAfterFrame).canceled)
		_ = summaryFor(t, gateway.logs, resp)
		elapsed := time.Since(start)
		assert.Less(t, elapsed, 500*time.Millisecond)
		t.Logf("process client cancel observed=%s", elapsed)
	})
	t.Run("real slow reader leaves no active upstream", func(t *testing.T) {
		upstream.mode.Store(probeSlowReader)
		addr := strings.TrimPrefix(base, "http://")
		connection, err := net.Dial("tcp", addr)
		require.NoError(t, err)
		defer connection.Close()
		tcp := connection.(*net.TCPConn)
		// Keep a bounded receive window without the 1KiB window's delayed
		// reopening on Linux. No reads occur until the upstream has exited;
		// the 60MiB source still exceeds all transport buffers by a wide margin.
		require.NoError(t, tcp.SetReadBuffer(64*1024))
		_, err = fmt.Fprintf(connection, "POST /v1/chat/completions HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", addr, created.Key, len(probeChat), probeChat)
		require.NoError(t, err)
		waitUpstreamObservation(t, upstream.entered, probeSlowReader)
		start := time.Now()
		observation := waitUpstreamObservation(t, upstream.ended, probeSlowReader)
		elapsed := time.Since(start)
		assert.True(t, observation.canceled)
		assert.Less(t, observation.emitted, 80, "source cannot finish 60MiB while the downstream refuses reads")
		assert.GreaterOrEqual(t, elapsed, 2500*time.Millisecond)
		assert.Less(t, elapsed, 4*time.Second)
		require.NoError(t, tcp.SetReadBuffer(4*1024*1024))
		require.NoError(t, connection.SetReadDeadline(time.Now().Add(5*time.Second)))
		resp, err := http.ReadResponse(bufio.NewReader(connection), nil)
		require.NoError(t, err)
		assert.Equal(t, 200, resp.StatusCode)
		_, err = io.Copy(io.Discard, resp.Body)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		_ = resp.Body.Close()
		summary := summaryFor(t, gateway.logs, resp)
		assert.EqualValues(t, probeChannel.ID, summary["channel_id"])
		var kind string
		for _, entry := range gateway.logs.entries(t, resp.Header.Get("X-Request-Id")) {
			if entry["msg"] == "upstream attempt finished" {
				kind, _ = entry["result_kind"].(string)
			}
		}
		assert.Equal(t, "overall_timeout", kind)
		t.Logf("real slow reader emitted=%d/80 large frames; upstream/handler exit=%s; HTTP=200 read=UnexpectedEOF Kind=%s", observation.emitted, elapsed, kind)
	})
	t.Run("concurrent process summaries", func(t *testing.T) {
		upstream.mode.Store(probeConcurrent)
		channels := []store.Channel{gatewayChannel(t, f.db, f.master, "process-a", "process-a", "up-a", upstream.server.URL, "owned-upstream-key", 10), gatewayChannel(t, f.db, f.master, "process-b", "process-b", "up-b", upstream.server.URL, "owned-upstream-key", 10)}
		for _, name := range []string{"process-a", "process-b"} {
			require.NoError(t, models.Create(context.Background(), &store.ModelAlias{Name: name}))
		}
		type result struct {
			index int
			resp  *http.Response
			err   error
		}
		done := make(chan result, 2)
		for i, key := range []string{created.Key, second.Key} {
			go func(index int, raw string) {
				resp, _, err := gatewayRequest(t, f.client, base, raw, "POST", "/v1/chat/completions", fmt.Sprintf(`{"model":"process-%s","messages":[{"role":"user"}]}`, []string{"a", "b"}[index]))
				done <- result{index, resp, err}
			}(i, key)
		}
		first := waitUpstreamObservation(t, upstream.entered, probeConcurrent)
		next := waitUpstreamObservation(t, upstream.entered, probeConcurrent)
		assert.NotEqual(t, first.model, next.model)
		upstream.once.Do(func() { close(upstream.release) })
		ids := map[string]bool{}
		for range 2 {
			select {
			case result := <-done:
				require.NoError(t, result.err)
				summary := summaryFor(t, gateway.logs, result.resp)
				assert.EqualValues(t, channels[result.index].ID, summary["channel_id"])
				assert.Equal(t, []string{"process-a", "process-b"}[result.index], summary["model"])
				id := result.resp.Header.Get("X-Request-Id")
				assert.False(t, ids[id])
				ids[id] = true
			case <-time.After(5 * time.Second):
				t.Fatal("concurrent process requests did not finish")
			}
		}
	})
	t.Run("Key API lifecycle", func(t *testing.T) {
		resp, data, err := gatewayRequest(t, f.client, base, created.Key, "GET", "/api/v1/user/keys", "")
		require.NoError(t, err)
		assert.Equal(t, 200, resp.StatusCode)
		assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
		assert.False(t, bytes.Contains(data, []byte(created.Key)))
		assert.NotContains(t, string(data), "key_hash")
		resp, _, err = gatewayRequest(t, f.client, base, second.Key, "DELETE", fmt.Sprintf("/api/v1/user/keys/%d", created.ID), "")
		require.NoError(t, err)
		assert.Equal(t, 204, resp.StatusCode)
		post(processChat, 403, "gaigate_key_revoked")
		resp, _, err = gatewayRequest(t, f.client, base, second.Key, "DELETE", fmt.Sprintf("/api/v1/user/keys/%d", created.ID), "")
		require.NoError(t, err)
		assert.Equal(t, 204, resp.StatusCode)
	})
	// Restart only the owned gateway after all requests have ended. Each startup
	// reruns the original seed; startGateway re-applies only the test primary URL.
	gateway.stop(t)
	gateway, base = f.startGateway(t, 2*time.Second, 500*time.Millisecond)
	t.Run("Overall before headers in a real process", func(t *testing.T) {
		upstream.mode.Store(probeHeaders)
		start := time.Now()
		resp, data, err := gatewayRequest(t, f.client, base, second.Key, "POST", "/v1/chat/completions", probeChat)
		require.NoError(t, err)
		processEnvelope(t, resp, data, 502, "gaigate_upstream_timeout")
		assert.True(t, waitUpstreamObservation(t, upstream.ended, probeHeaders).canceled)
		assert.GreaterOrEqual(t, time.Since(start), 400*time.Millisecond)
		assert.Less(t, time.Since(start), 1500*time.Millisecond)
		_ = summaryFor(t, gateway.logs, resp)
	})
	gateway.stop(t)
	gateway, base = f.startGateway(t, time.Second, 3*time.Second)
	t.Run("graceful lifecycle with an admitted stream", func(t *testing.T) {
		configureOwnedMock(t, f, "normal", `{"chunk_interval":"150ms"}`)
		req, err := http.NewRequest("POST", base+"/v1/chat/completions", strings.NewReader(processChat))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+second.Key)
		resp, err := f.client.Do(req)
		require.NoError(t, err)
		reader := bufio.NewReader(resp.Body)
		first, err := reader.ReadString('\n')
		require.NoError(t, err)
		if runtime.GOOS == "windows" {
			rest, err := io.ReadAll(reader)
			require.NoError(t, err)
			_ = resp.Body.Close()
			c, u, d := frameCounts(t, append([]byte(first), rest...))
			assert.Equal(t, 8, c)
			assert.Equal(t, 1, u)
			assert.Equal(t, 1, d)
			gateway.stop(t)
			t.Log("Windows active-stream Unix signal behavior not claimed; Linux race validates SIGTERM")
		} else {
			stopDone := make(chan struct{})
			go func() { gateway.stop(t); close(stopDone) }()
			rest, err := io.ReadAll(reader)
			require.NoError(t, err)
			_ = resp.Body.Close()
			<-stopDone
			c, u, d := frameCounts(t, append([]byte(first), rest...))
			assert.Equal(t, 8, c)
			assert.Equal(t, 1, u)
			assert.Equal(t, 1, d)
			_ = summaryFor(t, gateway.logs, resp)
			t.Log("SIGTERM preserved admitted stream, completed 8+1+1, then joined gateway with exit 0")
		}
	})
	for _, p := range f.gateways {
		select {
		case <-p.done:
		default:
			t.Error("owned gateway not joined")
		}
		for _, secret := range []string{created.Key, second.Key, base64.StdEncoding.EncodeToString(f.master), "owned-upstream-key"} {
			assert.False(t, strings.Contains(p.logs.String(), secret), "process logs leaked credentials")
		}
	}
	assert.False(t, strings.Contains(f.mock.logs.String(), created.Key))
	assert.False(t, strings.Contains(f.mock.logs.String(), second.Key))
}
