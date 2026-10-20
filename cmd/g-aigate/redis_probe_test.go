package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

// 审查②：复用 main 的生产客户端与检查函数，验证 readyz 的 2s 预算及故障恢复。
func TestRedisReadyProbeTimeoutAndRecovery(t *testing.T) {
	mr := miniredis.RunT(t)
	client := store.NewRedis(store.RedisOptions{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	probes := &middleware.Probes{}
	probes.Register("redis", redisReadinessCheck(client))
	router := gin.New()
	router.GET("/readyz", probes.ReadyHandler())
	request := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return w
	}
	assert.Equal(t, http.StatusOK, request().Code)
	received := make(chan string, 1)
	mr.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		select {
		case received <- cmd:
		default:
		}
		return true
	})
	start := time.Now()
	w := request()
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 1900*time.Millisecond)
	assert.Less(t, elapsed, 2500*time.Millisecond, "readyz 的 2s 预算须约束 Redis 网络读取")
	assertRedisProbeUnavailable(t, w)
	select {
	case cmd := <-received:
		assert.Equal(t, "PING", strings.ToUpper(cmd))
	default:
		t.Fatal("Redis 未收到 PING，未命中连接已建立但无响应的场景")
	}
	mr.Server().SetPreHook(nil)
	w = request()
	assert.Equal(t, http.StatusOK, w.Code, "Redis 恢复后同一客户端的 readyz 应恢复")
	assert.JSONEq(t, `{"status":"ok"}`, w.Body.String())
	t.Logf("unresponsive readyz elapsed=%s", elapsed)
}

func TestRedisReadyProbeHonorsShorterRequestDeadline(t *testing.T) {
	mr := miniredis.RunT(t)
	mr.Server().SetPreHook(func(_ *server.Peer, _ string, _ ...string) bool { return true })
	client := store.NewRedis(store.RedisOptions{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	probes := &middleware.Probes{}
	probes.Register("redis", redisReadinessCheck(client))
	router := gin.New()
	router.GET("/readyz", probes.ReadyHandler())
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	start := time.Now()
	router.ServeHTTP(w, req)
	elapsed := time.Since(start)
	assert.GreaterOrEqual(t, elapsed, 80*time.Millisecond)
	assert.Less(t, elapsed, 600*time.Millisecond, "检查不得把更早的请求 deadline 延长到 2s")
	assertRedisProbeUnavailable(t, w)
	t.Logf("shorter request deadline elapsed=%s", elapsed)
}

func assertRedisProbeUnavailable(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	var body struct {
		Status string   `json:"status"`
		Failed []string `json:"failed"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "unavailable", body.Status)
	require.Len(t, body.Failed, 1)
	assert.True(t, strings.HasPrefix(body.Failed[0], "redis:"), "失败项须明确为 redis：%v", body.Failed)
}
