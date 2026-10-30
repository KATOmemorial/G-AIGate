package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/channel"
	"gaigate/internal/store"
)

func TestChannelPickerProcessSWRRAcceptance(t *testing.T) {
	f := newProcessFixture(t)
	gateway, base := f.startGatewayWithProbeTimeout(t, time.Second, 3*time.Second, 250*time.Millisecond)
	admin := f.issueKey(t)
	ordinary := gatewayUser(t, f.db, "swrr-ordinary")
	const keyA, keyB, keyBackup = "owned-swrr-private-a", "owned-swrr-private-b", "owned-swrr-private-backup"
	const mappedMain, mappedBackup = "mapped-swrr-main", "mapped-swrr-backup"
	secrets := []string{admin.Key, ordinary, keyA, keyB, keyBackup, base64.StdEncoding.EncodeToString(f.master)}
	request := func(t *testing.T, identity, method, path string, body any, status int) (*http.Response, []byte) {
		t.Helper()
		var encoded []byte
		if body != nil {
			var err error
			encoded, err = json.Marshal(body)
			require.NoError(t, err)
		}
		resp, data, err := gatewayRequest(t, f.client, base, identity, method, path, string(encoded))
		require.NoError(t, err)
		require.Equal(t, status, resp.StatusCode, "unexpected status without exposing credentials")
		if strings.HasPrefix(path, adminChannelsPath) {
			assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
		}
		for _, raw := range secrets {
			assert.False(t, strings.Contains(string(data), raw), "response credential leak")
		}
		return resp, data
	}
	configure := func(t *testing.T, key, scenario string, chunks int) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"key": key, "scenario": scenario,
			"params": map[string]any{"chunks": chunks, "chunk_interval": "0s"}})
		require.NoError(t, err)
		resp, _, err := gatewayRequest(t, f.client, f.mockURL, "", "PUT", "/admin/scenario", string(body))
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
	}
	configure(t, keyA, "normal", 4)
	configure(t, keyB, "normal", 6)
	configure(t, keyBackup, "normal", 7)
	// Fill the real /v1 cache before admin creates a higher priority channel.
	resp, data, err := gatewayRequest(t, f.client, base, ordinary, "POST", "/v1/chat/completions", processChat)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	c, u, d := frameCounts(t, data)
	assert.Equal(t, [3]int{8, 1, 1}, [3]int{c, u, d})
	create := func(t *testing.T, name, mapped string, priority int, keys []string) channel.ChannelDetail {
		t.Helper()
		_, data := request(t, admin.Key, "POST", adminChannelsPath, map[string]any{
			"name": name, "provider": "openai", "base_url": f.mockURL, "priority": priority,
			"models": []map[string]string{{"model": "gpt-4o-demo", "upstream_model": mapped}}, "keys": keys}, 201)
		var detail channel.ChannelDetail
		require.NoError(t, json.Unmarshal(data, &detail))
		return detail
	}
	_ = create(t, "owned-swrr-backup", mappedBackup, 100, []string{keyBackup})
	created := create(t, "owned-swrr-main", mappedMain, 200, []string{keyA, keyB})
	require.Len(t, created.Keys, 2)
	require.Less(t, created.Keys[0].ID, created.Keys[1].ID)
	path := fmt.Sprintf("%s/%d", adminChannelsPath, created.ID)
	keyPath := func(index int) string { return fmt.Sprintf("%s/keys/%d", path, created.Keys[index].ID) }
	setKey := func(t *testing.T, index, weight, status int) {
		t.Helper()
		request(t, admin.Key, "PUT", keyPath(index), map[string]int{"weight": weight, "status": status}, 200)
	}
	expectedMainCalls := 0
	chatBody := map[string]any{"model": "gpt-4o-demo", "stream": true, "messages": []map[string]string{{"role": "user", "content": "hi"}}}
	pick := func(t *testing.T, expected string) {
		t.Helper()
		resp, data := request(t, ordinary, "POST", "/v1/chat/completions", chatBody, 200)
		expectedMainCalls++
		c, u, d := frameCounts(t, data)
		assert.Equal(t, map[string]int{"A": 4, "B": 6}[expected], c, "the chosen Key determines its own mock response")
		assert.Equal(t, 1, u)
		assert.Equal(t, 1, d)
		assert.Contains(t, string(data), mappedMain)
		assert.Equal(t, float64(created.ID), summaryFor(t, gateway.logs, resp)["channel_id"])
	}
	failure := func(t *testing.T, status int, code string, upstreamCalled bool) {
		t.Helper()
		resp, data := request(t, ordinary, "POST", "/v1/chat/completions", chatBody, status)
		processEnvelope(t, resp, data, status, code)
		assert.Equal(t, float64(created.ID), summaryFor(t, gateway.logs, resp)["channel_id"])
		if upstreamCalled {
			expectedMainCalls++
		}
	}
	t.Run("three to one and manual probes preserve position", func(t *testing.T) {
		setKey(t, 0, 3, 1)
		pick(t, "A")
		beforeHealth := f.healthRecord(t, created.ID)
		for range 2 {
			_, data := request(t, admin.Key, "POST", path+"/test", map[string]string{"model": "gpt-4o-demo"}, 200)
			expectedMainCalls++
			var result channel.ProbeResult
			require.NoError(t, json.Unmarshal(data, &result))
			assert.True(t, result.OK)
		}
		assert.Equal(t, beforeHealth, f.healthRecord(t, created.ID), "manual diagnostics do not feed health")
		for _, expected := range []string{"A", "B", "A"} {
			pick(t, expected)
		}
	})
	t.Run("committed weight and status changes are immediate", func(t *testing.T) {
		setKey(t, 0, 1, 1)
		setKey(t, 1, 3, 1)
		// Equal currents on the second position choose the smaller ID (A).
		for _, expected := range []string{"B", "A", "B", "B"} {
			pick(t, expected)
		}
		setKey(t, 1, 3, 0)
		pick(t, "A")
		setKey(t, 1, 3, 1)
		pick(t, "B")
	})
	t.Run("no Key and corrupt selected slot never use backup", func(t *testing.T) {
		setKey(t, 0, 1, 0)
		setKey(t, 1, 1, 0)
		failure(t, 503, "gaigate_no_candidate", false)
		request(t, admin.Key, "POST", path+"/test", map[string]string{"model": "gpt-4o-demo"}, 503)
		setKey(t, 0, 1, 1)
		setKey(t, 1, 1, 1)
		rows, err := store.NewKeyRepo(f.db).ListByChannel(context.Background(), created.ID)
		require.NoError(t, err)
		require.Len(t, rows, 2)
		secrets = append(secrets, string(rows[0].ProviderKeyEnc), string(rows[1].ProviderKeyEnc))
		require.NoError(t, f.db.Model(&store.ChannelKey{}).Where("channel_id = ? AND id = ?", created.ID, created.Keys[0].ID).
			Update("provider_key_enc", []byte("owned-swrr-corrupt-ciphertext")).Error)
		failure(t, 500, "gaigate_internal", false)
		pick(t, "B") // corrupt A consumed exactly one slot without retry
		require.NoError(t, f.db.Model(&store.ChannelKey{}).Where("channel_id = ? AND id = ?", created.ID, created.Keys[0].ID).
			Update("provider_key_enc", rows[0].ProviderKeyEnc).Error)
		pick(t, "A")
	})
	t.Run("upstream failure consumes one slot without retry", func(t *testing.T) {
		configure(t, keyA, "downtime", 4)
		pick(t, "B")
		failure(t, 502, "gaigate_upstream_error", true)
		pick(t, "B")
		configure(t, keyA, "normal", 4)
		pick(t, "A")
	})
	for _, key := range f.redis.Keys() {
		assert.True(t, strings.HasPrefix(key, "gaigate:health:"), "SWRR must not create distributed state")
	}
	// Join every process before counting complete upstream logs or scanning secrets.
	gateway.stop(t)
	f.mock.stop(t)
	assert.Equal(t, 1, strings.Count(gateway.logs.String(), `"msg":"health_probe_round_finished"`), "new channels were created after the completed initial round; exact SWRR counts must not include another background round")
	actualMain, actualBackup := 0, 0
	for _, entry := range processLogEntries(t, f.mock.logs) {
		if entry["msg"] != "request_done" {
			continue
		}
		switch entry["model"] {
		case mappedMain:
			actualMain++
		case mappedBackup:
			actualBackup++
		}
	}
	assert.Equal(t, expectedMainCalls, actualMain, "each business/probe call makes exactly its planned single upstream attempt")
	assert.Zero(t, actualBackup, "eligible backup channel is never tried")
	for _, process := range []*ownedProcess{gateway, f.mock} {
		for _, raw := range secrets {
			assert.False(t, strings.Contains(process.logs.String(), raw), "process log credential leak")
		}
	}
	t.Logf("real admin SWRR 3:1=A,A,B,A; 1:3=B,A,B,B; status/ciphertext updates immediate; planned upstream calls=%d actual=%d backup=%d", expectedMainCalls, actualMain, actualBackup)
}
