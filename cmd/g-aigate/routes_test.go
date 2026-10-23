package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/access"
	"gaigate/internal/config"
	"gaigate/internal/middleware"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

func init() { gin.SetMode(gin.TestMode) }

type gatewayLogs struct {
	mu     sync.Mutex
	data   bytes.Buffer
	notify chan struct{}
}

func newGatewayLogs() *gatewayLogs { return &gatewayLogs{notify: make(chan struct{}, 1024)} }
func (l *gatewayLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	n, err := l.data.Write(p)
	l.mu.Unlock()
	select {
	case l.notify <- struct{}{}:
	default:
	}
	return n, err
}
func (l *gatewayLogs) String() string { l.mu.Lock(); defer l.mu.Unlock(); return l.data.String() }
func (l *gatewayLogs) entries(t *testing.T, id string) []map[string]any {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		var entries []map[string]any
		done := false
		for _, line := range strings.Split(strings.TrimSpace(l.String()), "\n") {
			if line == "" {
				continue
			}
			var entry map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &entry))
			if entry["request_id"] == id {
				entries = append(entries, entry)
				if entry["msg"] == "request_done" {
					done = true
				}
			}
		}
		if done {
			return entries
		}
		select {
		case <-l.notify:
		case <-timer.C:
			t.Fatal("request_done was not emitted")
			return nil
		}
	}
}

func gatewayTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	port := 3307
	if value := os.Getenv("GAIGATE_TEST_MYSQL_PORT"); value != "" {
		var err error
		port, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	host, password := os.Getenv("GAIGATE_TEST_MYSQL_HOST"), os.Getenv("GAIGATE_TEST_MYSQL_PASSWORD")
	if host == "" {
		host = "127.0.0.1"
	}
	if password == "" {
		password = "gaigate_dev"
	}
	opts := store.Options{Host: host, Port: port, User: "root", Password: password, Database: "gaigate_test"}
	adminOpts := opts
	adminOpts.Database = ""
	admin, err := sql.Open("mysql", adminOpts.DSN())
	require.NoError(t, err)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, admin.PingContext(ctx), "M2 requires real MySQL")
	_, err = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS gaigate_test")
	require.NoError(t, err)
	_, err = admin.ExecContext(ctx, "CREATE DATABASE gaigate_test")
	require.NoError(t, err)
	require.NoError(t, store.RunMigrations(opts, "../../migrations"))
	db, err := store.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { pool, err := db.DB(); require.NoError(t, err); require.NoError(t, pool.Close()) })
	return db
}

func gatewayUser(t *testing.T, db *gorm.DB, name string) string {
	t.Helper()
	now := time.Now().UTC()
	group := store.Group{Name: name, CreatedAt: now}
	require.NoError(t, db.Create(&group).Error)
	user := store.User{Username: name, PasswordHash: "private-password-hash", Role: store.RoleUser, GroupID: group.ID, Status: store.UserStatusEnabled, CreatedAt: now, UpdatedAt: now}
	require.NoError(t, db.Create(&user).Error)
	raw, prefix, hash, err := access.GenerateKey()
	require.NoError(t, err)
	require.NoError(t, store.NewAPIKeyRepo(db).Create(context.Background(), &store.APIKey{UserID: user.ID, KeyPrefix: prefix, KeyHash: hash}))
	return raw
}

func gatewayChannel(t *testing.T, db *gorm.DB, master []byte, name, model, mapped, url, key string, priority int32) store.Channel {
	t.Helper()
	enc, err := secret.Encrypt([]byte(key), master)
	require.NoError(t, err)
	ch := store.Channel{Name: name, Provider: "openai", BaseURL: url, Priority: priority, Weight: 1, Enabled: true, HealthStatus: "disabled"}
	require.NoError(t, store.NewChannelRepo(db, store.NewConfigCache(0)).Create(context.Background(), &ch, []store.ChannelModel{{Model: model, UpstreamModel: mapped}}, [][]byte{enc}))
	return ch
}

type writerSnapshot struct {
	status, size int
	written      bool
	localChannel int64
}

func assembledGateway(t *testing.T, db *gorm.DB, master []byte, cfg config.ProxyConfig, logs *gatewayLogs) (*httptest.Server, <-chan writerSnapshot) {
	t.Helper()
	logger := newLogger(logs, "info")
	r := newGatewayRouter(logger, &middleware.Probes{})
	stats := make(chan writerSnapshot, 64)
	r.Use(func(c *gin.Context) {
		defer func() {
			stats <- writerSnapshot{c.Writer.Status(), c.Writer.Size(), c.Writer.Written(), middleware.ChannelIDFromContext(c.Request.Context())}
		}()
		c.Next()
	})
	require.NoError(t, registerGatewayRoutes(r, db, master, cfg, 5*time.Second, logger))
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return srv, stats
}

func gatewayRequest(t *testing.T, client *http.Client, base, raw, method, path, body string) (*http.Response, []byte, error) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, strings.NewReader(body))
	require.NoError(t, err)
	if raw != "" {
		req.Header.Set("Authorization", "Bearer "+raw)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	data, readErr := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	return resp, data, readErr
}

func summaryFor(t *testing.T, logs *gatewayLogs, resp *http.Response) map[string]any {
	t.Helper()
	entries := logs.entries(t, resp.Header.Get("X-Request-Id"))
	var summary map[string]any
	count := 0
	for _, entry := range entries {
		if entry["msg"] == "request_done" {
			summary = entry
			count++
		}
	}
	require.Equal(t, 1, count)
	assert.Contains(t, summary, "ts")
	assert.NotContains(t, summary, "time")
	assert.EqualValues(t, resp.StatusCode, summary["status_code"])
	return summary
}

func TestGatewayAssemblySelectionErrorsAndAbort(t *testing.T) {
	db := gatewayTestDB(t)
	master := bytes.Repeat([]byte{7}, 32)
	raw := gatewayUser(t, db, "assembly-owner")
	logs := newGatewayLogs()
	old := slog.Default()
	slog.SetDefault(newLogger(logs, "info"))
	defer slog.SetDefault(old)
	var mode atomic.Int32
	var primaryCalls, backupCalls, tieCalls atomic.Int64
	captured := make(chan map[string]json.RawMessage, 1)
	const normal = `{"id":"fixture","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3},"extension":{"keep":true}}`
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := primaryCalls.Add(1)
		var body map[string]json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		if call == 1 {
			captured <- body
		}
		assert.JSONEq(t, `"upstream-mapped"`, string(body["model"]))
		assert.Equal(t, "Bearer upstream-first-enabled", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		switch mode.Load() {
		case 1:
			w.WriteHeader(503)
			_, _ = io.WriteString(w, "<html>private upstream</html>")
		case 2:
			w.WriteHeader(401)
			_, _ = io.WriteString(w, `{"error":{"message":"private upstream","type":"auth"}}`)
		case 3:
			w.Header().Set("X-Private-Upstream", "must-not-copy")
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"error":{"message":"bad input","type":"invalid_request_error","code":"upstream_code","param":null},"private":"drop"}`)
		case 4:
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"error":{"message":"\u0073`+raw[1:]+`","type":"invalid_request_error"}}`)
		case 5:
			w.WriteHeader(400)
			_, _ = io.WriteString(w, `{"error":{"message":"upstream-first-enabled","type":"invalid_request_error"}}`)
		case 6:
			w.WriteHeader(429)
			_, _ = io.WriteString(w, `{"error":{"message":"rate limited","type":"rate_limit_error"}}`)
		case 7:
			w.Header().Set("Content-Type", "text/event-stream")
			for range 3 {
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
				w.(http.Flusher).Flush()
			}
			panic(http.ErrAbortHandler)
		default:
			if string(body["stream"]) == "true" {
				var options map[string]json.RawMessage
				assert.NoError(t, json.Unmarshal(body["stream_options"], &options))
				assert.Equal(t, "true", string(options["include_usage"]))
				w.Header().Set("Content-Type", "text/event-stream")
				for range 8 {
					_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"normal\"}}]}\n\n")
				}
				_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
			} else {
				_, _ = io.WriteString(w, normal)
			}
		}
	}))
	defer primary.Close()
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { backupCalls.Add(1); w.WriteHeader(500) }))
	defer backup.Close()
	tie := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { tieCalls.Add(1); w.WriteHeader(500) }))
	defer tie.Close()
	fallback := gatewayChannel(t, db, master, "fallback", "alias", "wrong-fallback", backup.URL, "upstream-backup", 5)
	chosen := gatewayChannel(t, db, master, "primary", "alias", "upstream-mapped", primary.URL, "revoked-key", 10)
	_ = gatewayChannel(t, db, master, "tie-later-id", "alias", "wrong-tie", tie.URL, "upstream-tie", 10)
	require.NoError(t, db.Model(&store.ChannelKey{}).Where("channel_id = ?", chosen.ID).Update("status", store.ChannelKeyStatusRevoked).Error)
	enc, err := secret.Encrypt([]byte("upstream-first-enabled"), master)
	require.NoError(t, err)
	keys := store.NewKeyRepo(db)
	require.NoError(t, keys.Add(context.Background(), int64(chosen.ID), enc))
	require.NoError(t, keys.Add(context.Background(), int64(chosen.ID), []byte("bad later ciphertext")))
	// 14.2 replaces the old first-Key assumption. Keep this protocol/error
	// matrix on one active Key; dedicated SWRR tests cover bad selected slots.
	require.NoError(t, db.Model(&store.ChannelKey{}).Where("channel_id = ? AND provider_key_enc = ?", chosen.ID, []byte("bad later ciphertext")).Update("status", 0).Error)
	model := store.ModelAlias{Name: "alias", DefaultChannelID: &fallback.ID, FallbackIDs: fmt.Sprintf("[%d]", fallback.ID)}
	models := store.NewModelRepo(db, store.NewConfigCache(0))
	require.NoError(t, models.Create(context.Background(), &model))
	require.NoError(t, models.Create(context.Background(), &store.ModelAlias{Name: "configured-without-channel"}))
	cfg := config.ProxyConfig{TTFBTimeout: time.Second, OverallTimeout: 3 * time.Second, StreamBuffer: 1}
	gateway, stats := assembledGateway(t, db, master, cfg, logs)
	post := func(body string, status int, code string) (*http.Response, []byte) {
		t.Helper()
		resp, data, readErr := gatewayRequest(t, gateway.Client(), gateway.URL, raw, "POST", "/v1/chat/completions", body)
		require.NoError(t, readErr)
		assert.Equal(t, status, resp.StatusCode)
		if code != "" {
			var envelope struct {
				Error struct {
					Code string `json:"code"`
				}
				RequestID string `json:"request_id"`
			}
			require.NoError(t, json.Unmarshal(data, &envelope))
			assert.Equal(t, code, envelope.Error.Code)
			assert.Equal(t, resp.Header.Get("X-Request-Id"), envelope.RequestID)
		}
		summary := summaryFor(t, logs, resp)
		if primaryCalls.Load() > 0 {
			assert.NotEmpty(t, summary["request_id"])
		}
		snapshot := <-stats
		assert.Equal(t, status, snapshot.status)
		assert.Equal(t, len(data), snapshot.size)
		assert.True(t, snapshot.written)
		assert.Zero(t, snapshot.localChannel, "bootstrap must not replace c.Request to update logs")
		assert.Zero(t, backupCalls.Load())
		assert.Zero(t, tieCalls.Load())
		assert.NotContains(t, string(data), raw)
		return resp, data
	}
	const request = `{"model":"alias","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"tool-1","function":{"arguments":"{}"}}]},{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:fixture"}}]}],"opaque":{"n":9007199254740993},"stream_options":{"include_usage":false,"custom":{"keep":true}}}`
	resp, data := post(request, 200, "")
	assert.Equal(t, normal, string(data))
	assert.EqualValues(t, chosen.ID, summaryFor(t, logs, resp)["channel_id"])
	var original map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(request), &original))
	seen := <-captured
	for _, name := range []string{"messages", "opaque", "stream_options"} {
		assert.JSONEq(t, string(original[name]), string(seen[name]))
	}
	_, data = post(`{"model":"alias","stream":true,"messages":[{"role":"user","content":"hi"}]}`, 200, "")
	assert.Equal(t, 8, strings.Count(string(data), `"delta"`))
	assert.Equal(t, 1, strings.Count(string(data), `"usage"`))
	assert.Equal(t, 1, strings.Count(string(data), "data: [DONE]"))
	post(`{"model":"unknown","messages":[{"role":"user"}]}`, 404, "gaigate_model_not_found")
	post(`{"model":"configured-without-channel","messages":[{"role":"user"}]}`, 503, "gaigate_no_candidate")
	for _, tc := range []struct {
		mode   int32
		status int
		code   string
	}{{1, 502, "gaigate_upstream_error"}, {2, 502, "gaigate_upstream_auth_failed"}, {3, 400, "upstream_code"}, {4, 502, "gaigate_upstream_error"}, {5, 502, "gaigate_upstream_error"}, {6, 502, "gaigate_upstream_error"}} {
		mode.Store(tc.mode)
		resp, data = post(request, tc.status, tc.code)
		assert.Empty(t, resp.Header.Get("X-Private-Upstream"))
		assert.NotContains(t, string(data), "private")
	}
	mode.Store(0)
	before := primaryCalls.Load()
	require.NoError(t, db.Model(&store.ChannelKey{}).Where("channel_id = ?", chosen.ID).Update("status", 0).Error)
	post(request, 503, "gaigate_no_candidate")
	assert.Equal(t, before, primaryCalls.Load(), "no enabled Key must not call any upstream")
	var active store.ChannelKey
	require.NoError(t, db.Where("channel_id = ?", chosen.ID).Order("id ASC").Offset(1).First(&active).Error)
	require.NoError(t, db.Model(&store.ChannelKey{}).Where("id = ?", active.ID).Updates(map[string]any{"status": 1, "provider_key_enc": []byte("corrupt")}).Error)
	post(request, 500, "gaigate_internal")
	assert.Equal(t, before, primaryCalls.Load())
	require.NoError(t, db.Model(&store.ChannelKey{}).Where("id = ?", active.ID).Update("provider_key_enc", enc).Error)
	require.NoError(t, db.Where("channel_id = ? AND model = ?", chosen.ID, "alias").Delete(&store.ChannelModel{}).Error)
	post(request, 500, "gaigate_internal")
	assert.Equal(t, before, primaryCalls.Load())
	require.NoError(t, db.Create(&store.ChannelModel{ChannelID: chosen.ID, Model: "alias", UpstreamModel: "upstream-mapped"}).Error)
	mode.Store(7)
	resp, data, readErr := gatewayRequest(t, gateway.Client(), gateway.URL, raw, "POST", "/v1/chat/completions", `{"model":"alias","stream":true,"messages":[{"role":"user"}]}`)
	assert.Equal(t, 200, resp.StatusCode)
	assert.ErrorIs(t, readErr, io.ErrUnexpectedEOF)
	assert.Equal(t, 3, strings.Count(string(data), `"delta"`))
	assert.NotContains(t, string(data), "usage")
	assert.NotContains(t, string(data), "[DONE]")
	assert.NotContains(t, string(data), "gaigate_")
	summary := summaryFor(t, logs, resp)
	assert.EqualValues(t, chosen.ID, summary["channel_id"])
	assert.EqualValues(t, 200, summary["status_code"])
	snapshot := <-stats
	assert.Equal(t, len(data), snapshot.size)
	assert.True(t, snapshot.written)
	assert.Zero(t, snapshot.localChannel)
	assert.Zero(t, backupCalls.Load())
	assert.Zero(t, tieCalls.Load())
	mode.Store(0)
	post(request, 200, "")
	resp, _, readErr = gatewayRequest(t, gateway.Client(), gateway.URL, "", "GET", "/healthz", "")
	require.NoError(t, readErr)
	assert.Equal(t, 200, resp.StatusCode)
	resp, data, readErr = gatewayRequest(t, gateway.Client(), gateway.URL, raw, "GET", "/api/v1/user/keys", "")
	require.NoError(t, readErr)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	assert.NotContains(t, string(data), "key_hash")
	assert.NotContains(t, string(data), raw)
	var views []struct {
		ID int64 `json:"id"`
	}
	require.NoError(t, json.Unmarshal(data, &views))
	require.Len(t, views, 1)
	<-stats
	_ = summaryFor(t, logs, resp)
	resp, _, readErr = gatewayRequest(t, gateway.Client(), gateway.URL, raw, "DELETE", fmt.Sprintf("/api/v1/user/keys/%d", views[0].ID), "")
	require.NoError(t, readErr)
	assert.Equal(t, 204, resp.StatusCode)
	<-stats
	_ = summaryFor(t, logs, resp)
	post(request, 403, "gaigate_key_revoked")
	// A real repository failure through the assembled gateway stays internal.
	rawSQL := gatewayUser(t, db, "sql-failure-owner")
	before = primaryCalls.Load()
	require.NoError(t, db.Exec("DROP TABLE channel_keys").Error)
	resp, data, readErr = gatewayRequest(t, gateway.Client(), gateway.URL, rawSQL, "POST", "/v1/chat/completions", request)
	require.NoError(t, readErr)
	assert.Equal(t, 500, resp.StatusCode)
	assert.Contains(t, string(data), "gaigate_internal")
	assert.Equal(t, before, primaryCalls.Load())
	assert.Zero(t, backupCalls.Load())
	assert.Zero(t, tieCalls.Load())
	<-stats
	_ = summaryFor(t, logs, resp)
	assert.NotContains(t, logs.String(), raw)
	assert.NotContains(t, logs.String(), rawSQL)
	assert.NotContains(t, logs.String(), "upstream-first-enabled")
	assert.NotContains(t, logs.String(), "private-password-hash")
	assert.NotContains(t, logs.String(), "bad later ciphertext")
}

func TestGatewayAssemblyRejectsInvalidConfigurationBeforeRoutes(t *testing.T) {
	for _, tc := range []struct {
		cfg config.ProxyConfig
		key []byte
	}{
		{config.ProxyConfig{TTFBTimeout: time.Second, OverallTimeout: time.Second}, bytes.Repeat([]byte{1}, 32)},
		{config.ProxyConfig{TTFBTimeout: time.Second, OverallTimeout: time.Second, StreamBuffer: 1}, nil},
		{config.ProxyConfig{OverallTimeout: time.Second, StreamBuffer: 1}, bytes.Repeat([]byte{1}, 32)},
	} {
		logs := newGatewayLogs()
		logger := newLogger(logs, "info")
		r := newGatewayRouter(logger, &middleware.Probes{})
		require.Error(t, registerGatewayRoutes(r, nil, tc.key, tc.cfg, 5*time.Second, logger))
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/v1/models", nil))
		assert.Equal(t, 404, w.Code)
		w = httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
		assert.Equal(t, 200, w.Code)
	}
}

func TestGatewayOpenerBudgetsAndClientCancellation(t *testing.T) {
	db := gatewayTestDB(t)
	master := bytes.Repeat([]byte{3}, 32)
	raw := gatewayUser(t, db, "budget-owner")
	logs := newGatewayLogs()
	old := slog.Default()
	slog.SetDefault(newLogger(logs, "info"))
	defer slog.SetDefault(old)
	var phase atomic.Int32
	entered := make(chan struct{}, 8)
	canceled := make(chan struct{}, 8)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Consume the POST body before waiting: net/http can then observe peer
		// closure while this header-delay fixture waits on the request context.
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error("cannot consume upstream fixture request")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		stage := phase.Load()
		if stage >= 1 {
			w.WriteHeader(200)
			if stage >= 2 {
				_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"first\"}}]}\n\n")
			}
			w.(http.Flusher).Flush()
		}
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
			canceled <- struct{}{}
		case <-time.After(3 * time.Second):
			t.Error("upstream did not receive budget/client cancellation")
		}
	}))
	defer upstream.Close()
	ch := gatewayChannel(t, db, master, "budget", "alias", "mapped", upstream.URL, "budget-upstream-key", 10)
	require.NoError(t, store.NewModelRepo(db, store.NewConfigCache(0)).Create(context.Background(), &store.ModelAlias{Name: "alias"}))
	for _, tc := range []struct {
		name          string
		phase         int32
		ttfb, overall time.Duration
		kind          string
		committed     bool
	}{
		{"response headers", 0, 80 * time.Millisecond, 600 * time.Millisecond, "ttfb_timeout", false},
		{"first frame", 1, 80 * time.Millisecond, 600 * time.Millisecond, "ttfb_timeout", false},
		{"Overall before headers", 0, time.Second, 100 * time.Millisecond, "overall_timeout", false},
		{"Overall after frame", 2, 80 * time.Millisecond, 250 * time.Millisecond, "overall_timeout", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			phase.Store(tc.phase)
			gateway, stats := assembledGateway(t, db, master, config.ProxyConfig{TTFBTimeout: tc.ttfb, OverallTimeout: tc.overall, StreamBuffer: 1}, logs)
			type result struct {
				resp *http.Response
				data []byte
				err  error
			}
			done := make(chan result, 1)
			requestStarted := time.Now()
			go func() {
				resp, data, err := gatewayRequest(t, gateway.Client(), gateway.URL, raw, "POST", "/v1/chat/completions", `{"model":"alias","stream":true,"messages":[{"role":"user"}]}`)
				done <- result{resp, data, err}
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("opener did not enter upstream")
			}
			start := time.Now()
			var got result
			select {
			case got = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("gateway did not enforce configured budget")
			}
			assert.Less(t, time.Since(start), 2*time.Second)
			elapsed := time.Since(requestStarted)
			assert.GreaterOrEqual(t, elapsed, min(tc.ttfb, tc.overall)/2)
			assert.Less(t, elapsed, 2*time.Second)
			t.Logf("configured TTFB=%s Overall=%s; observed=%s", tc.ttfb, tc.overall, elapsed)
			if tc.committed {
				assert.Equal(t, 200, got.resp.StatusCode)
				assert.ErrorIs(t, got.err, io.ErrUnexpectedEOF)
				assert.Equal(t, 1, strings.Count(string(got.data), `"delta"`))
				assert.NotContains(t, string(got.data), "[DONE]")
				assert.NotContains(t, string(got.data), "gaigate_")
			} else {
				require.NoError(t, got.err)
				assert.Equal(t, 502, got.resp.StatusCode)
				assert.Contains(t, string(got.data), "gaigate_upstream_timeout")
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("upstream request not canceled")
			}
			entries := logs.entries(t, got.resp.Header.Get("X-Request-Id"))
			var kind string
			for _, entry := range entries {
				if entry["msg"] == "upstream attempt finished" {
					kind, _ = entry["result_kind"].(string)
				}
			}
			assert.Equal(t, tc.kind, kind)
			summary := summaryFor(t, logs, got.resp)
			assert.EqualValues(t, ch.ID, summary["channel_id"])
			snapshot := <-stats
			assert.Equal(t, len(got.data), snapshot.size)
			assert.True(t, snapshot.written)
		})
	}
	phase.Store(2)
	gateway, stats := assembledGateway(t, db, master, config.ProxyConfig{TTFBTimeout: time.Second, OverallTimeout: 3 * time.Second, StreamBuffer: 1}, logs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", gateway.URL+"/v1/chat/completions", strings.NewReader(`{"model":"alias","stream":true,"messages":[{"role":"user"}]}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+raw)
	resp, err := gateway.Client().Do(req)
	require.NoError(t, err)
	_, err = bufio.NewReader(resp.Body).ReadString('\n')
	require.NoError(t, err)
	<-entered
	start := time.Now()
	cancel()
	_, _ = io.Copy(io.Discard, resp.Body)
	require.NoError(t, resp.Body.Close())
	select {
	case <-canceled:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("client cancellation did not reach upstream")
	}
	select {
	case <-stats:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("gateway handler did not exit after cancellation")
	}
	cancelElapsed := time.Since(start)
	assert.Less(t, cancelElapsed, 500*time.Millisecond)
	t.Logf("client cancellation observed=%s; target=100ms upper=500ms", cancelElapsed)
	entries := logs.entries(t, resp.Header.Get("X-Request-Id"))
	var kind string
	for _, entry := range entries {
		if entry["msg"] == "upstream attempt finished" {
			kind, _ = entry["result_kind"].(string)
		}
	}
	assert.Equal(t, "client_canceled", kind)
	assert.NotContains(t, logs.String(), raw)
}

func TestGatewayConcurrentRequestsKeepInnerChannelSummaries(t *testing.T) {
	db := gatewayTestDB(t)
	master := bytes.Repeat([]byte{4}, 32)
	rawA := gatewayUser(t, db, "owner-a")
	rawB := gatewayUser(t, db, "owner-b")
	logs := newGatewayLogs()
	old := slog.Default()
	slog.SetDefault(newLogger(logs, "info"))
	defer slog.SetDefault(old)
	entered := make(chan string, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var fields struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
			t.Error(err)
			return
		}
		entered <- fields.Model
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[]}`)
	}))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		upstream.Close()
	}()
	channels := []store.Channel{gatewayChannel(t, db, master, "a", "model-a", "up-a", upstream.URL, "channel-a-key", 10), gatewayChannel(t, db, master, "b", "model-b", "up-b", upstream.URL, "channel-b-key", 10)}
	models := store.NewModelRepo(db, store.NewConfigCache(0))
	for _, name := range []string{"model-a", "model-b"} {
		require.NoError(t, models.Create(context.Background(), &store.ModelAlias{Name: name}))
	}
	gateway, stats := assembledGateway(t, db, master, config.ProxyConfig{TTFBTimeout: time.Second, OverallTimeout: 3 * time.Second, StreamBuffer: 1}, logs)
	type result struct {
		index int
		resp  *http.Response
		data  []byte
		err   error
	}
	done := make(chan result, 2)
	for i, raw := range []string{rawA, rawB} {
		go func(index int, key string) {
			resp, data, err := gatewayRequest(t, gateway.Client(), gateway.URL, key, "POST", "/v1/chat/completions", fmt.Sprintf(`{"model":"model-%s","messages":[{"role":"user"}]}`, []string{"a", "b"}[index]))
			done <- result{index, resp, data, err}
		}(i, raw)
	}
	seen := map[string]bool{}
	for range 2 {
		select {
		case model := <-entered:
			seen[model] = true
		case <-time.After(3 * time.Second):
			t.Fatal("both requests did not reach upstream together")
		}
	}
	assert.Equal(t, map[string]bool{"up-a": true, "up-b": true}, seen)
	releaseOnce.Do(func() { close(release) })
	ids := map[string]bool{}
	for range 2 {
		got := <-done
		require.NoError(t, got.err)
		assert.Equal(t, 200, got.resp.StatusCode)
		summary := summaryFor(t, logs, got.resp)
		assert.EqualValues(t, channels[got.index].ID, summary["channel_id"])
		assert.EqualValues(t, got.index+1, summary["user_id"])
		assert.Equal(t, []string{"model-a", "model-b"}[got.index], summary["model"])
		id := got.resp.Header.Get("X-Request-Id")
		assert.False(t, ids[id])
		ids[id] = true
		snapshot := <-stats
		assert.Zero(t, snapshot.localChannel)
		assert.Equal(t, len(`{"choices":[]}`), snapshot.size)
		assert.True(t, snapshot.written)
	}
	assert.NotContains(t, logs.String(), rawA)
	assert.NotContains(t, logs.String(), rawB)
	assert.NotContains(t, logs.String(), "channel-a-key")
	assert.NotContains(t, logs.String(), "channel-b-key")
}
