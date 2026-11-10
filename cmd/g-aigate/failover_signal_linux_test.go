//go:build redis_integration

package main

import (
	"context"
	"fmt"
	"gaigate/internal/store"
	"github.com/stretchr/testify/require"
	"syscall"
	"testing"
	"time"
)

func TestFailoverRedisLinuxSIGTERMDuringFallback(t *testing.T) {
	f := newFailoverFixture(t)
	p, base := f.start(t, 2*time.Second, 4*time.Second, 300*time.Millisecond, 3)
	f.mode(t, 0, "429")
	f.mode(t, 1, "hold")
	done := make(chan failoverResponse, 1)
	go func() { done <- f.chat(context.Background(), base, f.user, "owned-b5", true) }()
	failoverWait(t, func() bool { return f.stats(t, 1).Active == 1 })
	require.Equal(t, 1, f.stats(t, 0).Business)
	require.Equal(t, 1, f.stats(t, 1).Business)
	pending, err := f.redis.ZCard(context.Background(), fmt.Sprintf("gaigate:breaker:pending:{%d}", f.channels[1].ID)).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, pending)
	health := store.NewVersionedHealthState(f.redis)
	before, err := health.LoadVersioned(context.Background(), int64(f.channels[1].ID))
	require.NoError(t, err)
	start := time.Now()
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback SIGTERM did not join")
	}
	require.NoError(t, p.err)
	require.Error(t, (<-done).Err)
	require.Less(t, time.Since(start), 2500*time.Millisecond)
	failoverWait(t, func() bool { return f.stats(t, 1).Active == 0 })
	pending, err = f.redis.ZCard(context.Background(), fmt.Sprintf("gaigate:breaker:pending:{%d}", f.channels[1].ID)).Result()
	require.NoError(t, err)
	require.Zero(t, pending, "neutral cleanup must finish while Redis is still open")
	after, err := health.LoadVersioned(context.Background(), int64(f.channels[1].ID))
	require.NoError(t, err)
	require.Equal(t, before, after, "canceled fallback must not invent a health failure")
	indexes := map[string]int{}
	kinds := []any{}
	lastAttempt := -1
	for i, row := range processLogEntries(t, p.logs) {
		msg, _ := row["msg"].(string)
		if msg == "upstream attempt finished" {
			lastAttempt = i
			kinds = append(kinds, row["result_kind"])
		}
		if msg == "request_done" || msg == "http_handlers_stopped" || msg == "health_service_stopped" {
			indexes[msg] = i
		}
		if msg == "dependency_close_finished" {
			require.Equal(t, true, row["ok"])
			name, _ := row["name"].(string)
			indexes[name] = i
		}
	}
	require.Equal(t, []any{"upstream_error", "client_canceled"}, kinds)
	for _, name := range []string{"request_done", "http_handlers_stopped", "health_service_stopped", "health", "usage-repo", "redis", "mysql"} {
		_, ok := indexes[name]
		require.True(t, ok, name)
	}
	require.Greater(t, indexes["request_done"], lastAttempt)
	require.Greater(t, indexes["http_handlers_stopped"], indexes["request_done"])
	require.Greater(t, indexes["health_service_stopped"], indexes["http_handlers_stopped"])
	require.Greater(t, indexes["health"], indexes["health_service_stopped"])
	require.Greater(t, indexes["usage-repo"], indexes["health"])
	require.Greater(t, indexes["redis"], indexes["usage-repo"])
	require.Greater(t, indexes["mysql"], indexes["redis"])
	f.privacy(t)
	t.Logf("SIGTERM during actual 429->backup opener; both attempt feedbacks then whole handler, health, usage, Redis, MySQL joined; exit0 elapsed=%s", time.Since(start))
}
