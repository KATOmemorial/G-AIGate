package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/channel"
	"gaigate/internal/store"
)

const adminChannelsPath = "/api/v1/admin/channels"

func TestChannelAdminProcessAcceptance(t *testing.T) {
	f := newProcessFixture(t)
	gateway, base := f.startGatewayWithProbeTimeout(t, time.Second, 3*time.Second, 180*time.Millisecond)
	admin := f.issueKey(t) // The actual CLI signs the existing seeded admin identity.
	ordinary := gatewayUser(t, f.db, "channel-ordinary")
	ctx := context.Background()
	var principal store.User
	require.NoError(t, f.db.Where("username = ?", "admin").First(&principal).Error)
	require.Equal(t, store.RoleAdmin, principal.Role)

	request := func(key, method, path, body string, expected int) []byte {
		resp, data, err := gatewayRequest(t, f.client, base, key, method, path, body)
		require.NoError(t, err)
		require.Equal(t, expected, resp.StatusCode, "unexpected status without exposing response credentials")
		if strings.HasPrefix(path, adminChannelsPath) {
			assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
		}
		for _, secret := range []string{admin.Key, ordinary, "owned-channel-private-a", "owned-channel-private-b"} {
			assert.False(t, strings.Contains(string(data), secret), "response leaked credential")
		}
		return data
	}
	create := func(name, url, model, key string) string {
		body, err := json.Marshal(map[string]any{"name": name, "provider": "openai", "base_url": url,
			"priority": 100, "models": []map[string]string{{"model": "gpt-4o-demo", "upstream_model": model}}, "keys": []string{key}})
		require.NoError(t, err)
		return string(body)
	}

	t.Run("every route requires real Bearer admin", func(t *testing.T) {
		routes := []struct{ method, path, body string }{
			{"GET", "", ""}, {"GET", "/99999", ""}, {"POST", "", "{}"}, {"PUT", "/99999", "{}"}, {"DELETE", "/99999", ""},
			{"POST", "/99999/keys", "{}"}, {"PUT", "/99999/keys/1", "{}"}, {"DELETE", "/99999/keys/1", ""}, {"POST", "/99999/test", "{}"},
		}
		before := int64(0)
		require.NoError(t, f.db.Model(&store.Channel{}).Count(&before).Error)
		for _, route := range routes {
			request("", route.method, adminChannelsPath+route.path, route.body, 401)
			data := request(ordinary, route.method, adminChannelsPath+route.path, route.body, 403)
			assert.Contains(t, string(data), "gaigate_forbidden")
		}
		after := int64(0)
		require.NoError(t, f.db.Model(&store.Channel{}).Count(&after).Error)
		assert.Equal(t, before, after)
		spare := f.issueKey(t)
		request(admin.Key, "DELETE", fmt.Sprintf("/api/v1/user/keys/%d", spare.ID), "", 204)
		data := request(spare.Key, "GET", adminChannelsPath, "", 403)
		assert.Contains(t, string(data), "gaigate_key_revoked")
	})
	// Fill the production /v1 configuration cache before any management write.
	data := request(ordinary, "POST", "/v1/chat/completions", processChat, 200)
	c, u, d := frameCounts(t, data)
	assert.Equal(t, [3]int{8, 1, 1}, [3]int{c, u, d})
	var created channel.ChannelDetail
	data = request(admin.Key, "POST", adminChannelsPath, create("owned-admin-channel", f.mockURL, "mapped-admin-one", "owned-channel-private-a"), 201)
	require.NoError(t, json.Unmarshal(data, &created))
	require.Positive(t, created.ID)
	require.Len(t, created.Keys, 1)
	assert.True(t, created.Enabled)
	assert.Equal(t, int32(1), created.Weight)
	path := fmt.Sprintf("%s/%d", adminChannelsPath, created.ID)
	keyPath := fmt.Sprintf("%s/keys/%d", path, created.Keys[0].ID)

	t.Run("admin create evicts actual v1 cache and mapping is used", func(t *testing.T) {
		resp, data, err := gatewayRequest(t, f.client, base, ordinary, "POST", "/v1/chat/completions", processChat)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		c, u, d := frameCounts(t, data)
		assert.Equal(t, [3]int{8, 1, 1}, [3]int{c, u, d})
		entry := summaryFor(t, gateway.logs, resp)
		assert.Equal(t, float64(created.ID), entry["channel_id"], "production v1 must immediately select newly created higher priority channel")
		assert.Contains(t, string(data), "mapped-admin-one")
		data = request(admin.Key, "GET", path, "", 200)
		var detail channel.ChannelDetail
		require.NoError(t, json.Unmarshal(data, &detail))
		assert.Equal(t, created.ID, detail.ID)
		data = request(admin.Key, "GET", adminChannelsPath, "", 200)
		var list struct {
			Data []channel.ChannelView `json:"data"`
		}
		require.NoError(t, json.Unmarshal(data, &list))
		assert.Len(t, list.Data, 3)
	})

	keys := store.NewKeyRepo(f.db)
	beforeKeys, err := keys.ListByChannel(ctx, created.ID)
	require.NoError(t, err)
	// A second owned mock gives a direct HTTP proof of BaseURL invalidation.
	otherAddr := processPort(t)
	otherMock := startOwnedProcess(t, f.root, f.mockBinary, append(cleanProcessEnvironment(), "MOCK_ADDR="+otherAddr))
	otherURL := "http://" + otherAddr
	waitProcessHealth(t, otherMock, otherURL)
	update := func(enabled bool, url, mapped string) string {
		body, err := json.Marshal(map[string]any{"name": "owned-admin-updated", "provider": "openai", "base_url": url,
			"priority": 100, "weight": 4, "enabled": enabled, "models": []map[string]string{{"model": "gpt-4o-demo", "upstream_model": mapped}}})
		require.NoError(t, err)
		return string(body)
	}
	request(admin.Key, "PUT", path, update(true, otherURL, "mapped-admin-two"), 200)
	t.Run("admin complete PUT invalidates v1 URL and mapping immediately", func(t *testing.T) {
		resp, data, err := gatewayRequest(t, f.client, base, ordinary, "POST", "/v1/chat/completions", processChat)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		assert.Contains(t, string(data), "mapped-admin-two")
		assert.Equal(t, float64(created.ID), summaryFor(t, gateway.logs, resp)["channel_id"])
		waitProcessLog(t, otherMock.logs, 0, func(e map[string]any) bool { return e["model"] == "mapped-admin-two" })
		after, err := keys.ListByChannel(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, beforeKeys, after)
	})

	configure := func(key, scenario, params string) {
		var fields map[string]any
		if params == "" {
			params = "{}"
		}
		require.NoError(t, json.Unmarshal([]byte(params), &fields))
		body, err := json.Marshal(map[string]any{"key": key, "scenario": scenario, "params": fields})
		require.NoError(t, err)
		resp, _, err := gatewayRequest(t, f.client, otherURL, "", "PUT", "/admin/scenario", string(body))
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
	}
	t.Run("probe outcomes use own mock and never change health or weights", func(t *testing.T) {
		var before store.Channel
		require.NoError(t, f.db.Where("id = ?", created.ID).First(&before).Error)
		beforeKey, err := keys.ListByChannel(ctx, created.ID)
		require.NoError(t, err)
		beforeHealth := f.healthRecord(t, created.ID)
		for _, scenario := range []string{"normal", "downtime", "rate_limited", "slow"} {
			configure("owned-channel-private-a", scenario, "")
			data := request(admin.Key, "POST", path+"/test", `{"model":"gpt-4o-demo"}`, 200)
			var result channel.ProbeResult
			require.NoError(t, json.Unmarshal(data, &result))
			assert.Equal(t, scenario == "normal", result.OK)
			if scenario == "slow" {
				assert.Equal(t, "timeout", result.Reason)
				assert.GreaterOrEqual(t, result.LatencyMS, int64(140))
				assert.Less(t, result.LatencyMS, int64(1200))
			}
		}
		configure("owned-channel-private-a", "downtime", `{"downtime_mode":"refuse"}`)
		data := request(admin.Key, "POST", path+"/test", `{"model":"gpt-4o-demo"}`, 200)
		assert.Contains(t, string(data), `"ok":false`)
		configure("owned-channel-private-a", "normal", "")
		var after store.Channel
		require.NoError(t, f.db.Where("id = ?", created.ID).First(&after).Error)
		// Display updates from earlier business attempts are asynchronous; the
		// manual test must leave Redis and every management column unchanged.
		after.HealthStatus, after.LastCheckedAt = before.HealthStatus, before.LastCheckedAt
		assert.Equal(t, before, after)
		afterKey, err := keys.ListByChannel(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, beforeKey, afterKey)
		assert.Equal(t, beforeHealth, f.healthRecord(t, created.ID), "manual tests do not feed health")
		request(admin.Key, "POST", path+"/test", `{"model":"not-mapped"}`, 400)
	})
	t.Run("key child lifecycle preserves ciphertext and owner", func(t *testing.T) {
		data := request(admin.Key, "POST", path+"/keys", `{"key":"owned-channel-private-b"}`, 201)
		var added channel.KeyView
		require.NoError(t, json.Unmarshal(data, &added))
		assert.Equal(t, int32(1), added.Weight)
		assert.Equal(t, int8(1), added.Status)
		configure("owned-channel-private-a", "downtime", "")
		configure("owned-channel-private-b", "normal", "")
		request(admin.Key, "PUT", keyPath, `{"weight":7,"status":0}`, 200)
		data = request(ordinary, "POST", "/v1/chat/completions", processChat, 200)
		c, u, d := frameCounts(t, data)
		assert.Equal(t, [3]int{8, 1, 1}, [3]int{c, u, d})
		all, err := keys.ListByChannel(ctx, created.ID)
		require.NoError(t, err)
		assert.Equal(t, beforeKeys[0].ProviderKeyEnc, all[0].ProviderKeyEnc)
		assert.Equal(t, beforeKeys[0].CreatedAt, all[0].CreatedAt)
		request(admin.Key, "DELETE", fmt.Sprintf("%s/1/keys/%d", adminChannelsPath, added.ID), "", 404)
		request(admin.Key, "DELETE", fmt.Sprintf("%s/keys/%d", path, added.ID), "", 204)
		request(admin.Key, "POST", path+"/test", `{"model":"gpt-4o-demo"}`, 503)
		request(ordinary, "POST", "/v1/chat/completions", processChat, 503)
		request(admin.Key, "PUT", keyPath, `{"weight":1,"status":1}`, 200)
		configure("owned-channel-private-a", "normal", "")
	})
	t.Run("disabled diagnostic and fallback are configuration changes", func(t *testing.T) {
		request(admin.Key, "PUT", path, update(false, otherURL, "mapped-admin-two"), 200)
		data := request(admin.Key, "POST", path+"/test", `{"model":"gpt-4o-demo"}`, 200)
		assert.Contains(t, string(data), `"ok":true`)
		resp, _, err := gatewayRequest(t, f.client, base, ordinary, "POST", "/v1/chat/completions", processChat)
		require.NoError(t, err)
		assert.NotEqual(t, float64(created.ID), summaryFor(t, gateway.logs, resp)["channel_id"])
		request(admin.Key, "PUT", path, update(true, otherURL, "mapped-admin-two"), 200)
		resp, _, err = gatewayRequest(t, f.client, base, ordinary, "POST", "/v1/chat/completions", processChat)
		require.NoError(t, err)
		assert.Equal(t, float64(created.ID), summaryFor(t, gateway.logs, resp)["channel_id"])
		request(admin.Key, "DELETE", path, "", 204)
		resp, _, err = gatewayRequest(t, f.client, base, ordinary, "POST", "/v1/chat/completions", processChat)
		require.NoError(t, err)
		assert.NotEqual(t, float64(created.ID), summaryFor(t, gateway.logs, resp)["channel_id"])
		request(admin.Key, "GET", path, "", 404)
	})
	// Close and join before reading process logs, including asynchronous mock summaries.
	otherMock.stop(t)
	gateway.stop(t)
	f.mock.stop(t)
	for _, p := range []*ownedProcess{gateway, f.mock, otherMock} {
		for _, secret := range []string{admin.Key, ordinary, "owned-channel-private-a", "owned-channel-private-b", base64.StdEncoding.EncodeToString(f.master), string(beforeKeys[0].ProviderKeyEnc)} {
			assert.False(t, strings.Contains(p.logs.String(), secret), "owned process log credential leak")
		}
	}
	t.Log("real CLI admin -> management create/update/keys/probe -> v1 cache and mapped upstream closed loop passed")
}
