package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/config"
	"gaigate/internal/middleware"
	"gaigate/internal/store"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestGatewayRealHealthAuthSignalsFilteringAndRedisFailure(t *testing.T) {
	db := gatewayTestDB(t)
	master := bytes.Repeat([]byte{4}, 32)
	admin := gatewayUser(t, db, "health-admin")
	require.NoError(t, db.Model(&store.User{}).Where("username = ?", "health-admin").Update("role", store.RoleAdmin).Error)
	ordinary := gatewayUser(t, db, "health-user")
	var status atomic.Int32
	status.Store(200)
	var primaryCalls, backupCalls, probes atomic.Int32
	upstream := func(counter *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var fields map[string]json.RawMessage
			require.NoError(t, json.NewDecoder(r.Body).Decode(&fields))
			if isFixtureHealthProbe(fields) {
				probes.Add(1)
			} else {
				counter.Add(1)
			}
			w.Header().Set("Content-Type", "application/json")
			code := int(status.Load())
			if counter == &backupCalls {
				code = 200
			}
			w.WriteHeader(code)
			if code == 200 {
				_, _ = io.WriteString(w, `{"choices":[]}`)
			} else {
				_, _ = io.WriteString(w, `{"error":{"message":"owned error","type":"owned_error"}}`)
			}
		}))
	}
	primaryServer, backupServer := upstream(&primaryCalls), upstream(&backupCalls)
	t.Cleanup(primaryServer.Close)
	t.Cleanup(backupServer.Close)
	primary := gatewayChannel(t, db, master, "health-primary", "alias", "up-primary", primaryServer.URL, "owned-health-primary", 10)
	backup := gatewayChannel(t, db, master, "health-backup", "alias", "up-backup", backupServer.URL, "owned-health-backup", 5)
	require.NoError(t, store.NewModelRepo(db, store.NewConfigCache(0)).Create(context.Background(), &store.ModelAlias{Name: "alias"}))
	mr := miniredis.RunT(t)
	client := store.NewRedis(store.RedisOptions{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	state := store.NewVersionedHealthState(client)
	logs := newGatewayLogs()
	logger := newLogger(logs, "info")
	router := newGatewayRouter(logger, &middleware.Probes{})
	options := testHealthConfig()
	options.FailureThreshold = 3
	options.DisableDuration = time.Hour
	health, err := registerGatewayRoutes(router, db, master, config.ProxyConfig{TTFBTimeout: time.Second, OverallTimeout: 2 * time.Second, StreamBuffer: 1}, options, state, logger)
	require.NoError(t, err)
	require.NoError(t, health.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, health.Close(context.Background())) })
	waitHealthRound(t, logs)
	require.Equal(t, int32(2), probes.Load(), "actual startup probes must be enabled")
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	path := fmt.Sprintf("/api/v1/admin/channels/%d/health", primary.ID)
	request := func(identity, method, path, body string, want int) []byte {
		resp, data, readErr := gatewayRequest(t, server.Client(), server.URL, identity, method, path, body)
		require.NoError(t, readErr)
		require.Equal(t, want, resp.StatusCode)
		if method == http.MethodGet {
			require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
		}
		return data
	}
	request("", http.MethodGet, path, "", 401)
	request(ordinary, http.MethodGet, path, "", 403)
	request(admin, http.MethodGet, "/api/v1/admin/channels/99999/health", "", 404)
	data := request(admin, http.MethodGet, path, "", 200)
	var initial map[string]any
	require.NoError(t, json.Unmarshal(data, &initial))
	require.Len(t, initial, 7)
	require.Equal(t, true, initial["available"])
	require.Positive(t, initial["last_probe_at"])
	const body = `{"model":"alias","messages":[{"role":"user","content":"hi"}]}`
	// Preparation errors are not attempts and cannot cause channel/Key retry.
	keys, err := store.NewKeyRepo(db).ListByChannel(context.Background(), int64(primary.ID))
	require.NoError(t, err)
	require.NoError(t, db.Model(&store.ChannelKey{}).Where("id = ?", keys[0].ID).Update("provider_key_enc", []byte("owned-corrupt-key")).Error)
	before, err := state.LoadVersioned(context.Background(), int64(primary.ID))
	require.NoError(t, err)
	request(ordinary, http.MethodPost, "/v1/chat/completions", body, 500)
	after, err := state.LoadVersioned(context.Background(), int64(primary.ID))
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Zero(t, primaryCalls.Load())
	require.Zero(t, backupCalls.Load())
	require.NoError(t, db.Model(&store.ChannelKey{}).Where("id = ?", keys[0].ID).Update("provider_key_enc", keys[0].ProviderKeyEnc).Error)
	status.Store(400)
	request(ordinary, http.MethodPost, "/v1/chat/completions", body, 400)
	after, err = state.LoadVersioned(context.Background(), int64(primary.ID))
	require.NoError(t, err)
	require.Equal(t, before, after, "request HTTP400 must be ignored")
	status.Store(503)
	for count := int32(1); count <= 3; count++ {
		request(ordinary, http.MethodPost, "/v1/chat/completions", body, 502)
		record, err := state.LoadVersioned(context.Background(), int64(primary.ID))
		require.NoError(t, err)
		require.Equal(t, count, record.Snapshot.ConsecutiveFailures)
		require.Equal(t, before.Version+uint64(count), record.Version, "exactly one signal per real attempt")
		require.Equal(t, before.Snapshot.LastProbeAt, record.Snapshot.LastProbeAt)
	}
	data = request(admin, http.MethodGet, path, "", 200)
	var disabled map[string]any
	require.NoError(t, json.Unmarshal(data, &disabled))
	require.Equal(t, false, disabled["available"])
	primaryBefore, backupBefore := primaryCalls.Load(), backupCalls.Load()
	request(ordinary, http.MethodPost, "/v1/chat/completions", body, 200)
	require.Equal(t, primaryBefore, primaryCalls.Load())
	require.Equal(t, backupBefore+1, backupCalls.Load(), "disabled high priority channel is pre-filtered")
	backupRecord, err := state.LoadVersioned(context.Background(), int64(backup.ID))
	require.NoError(t, err)
	blocked := backupRecord.Snapshot
	blocked.DisabledUntil = 1
	blocked.ConsecutiveFailures = 3
	committed, err := state.CompareAndSwap(context.Background(), int64(backup.ID), backupRecord, blocked)
	require.NoError(t, err)
	require.True(t, committed)
	backupBefore = backupCalls.Load()
	request(ordinary, http.MethodPost, "/v1/chat/completions", body, 503)
	require.Equal(t, primaryBefore, primaryCalls.Load())
	require.Equal(t, backupBefore, backupCalls.Load(), "no healthy candidates means zero upstream calls, including an expired cooldown")
	mr.Close()
	data = request(admin, http.MethodGet, path, "", 503)
	require.Contains(t, string(data), "gaigate_health_unavailable")
	require.NotContains(t, string(data), "consecutive_failures", "Redis failure must not return a fake zero snapshot")
	for _, sensitive := range []string{admin, ordinary, "owned-health-primary", "owned-health-backup", "owned-corrupt-key"} {
		require.NotContains(t, logs.String(), sensitive)
	}
}
