//go:build redis_integration

package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

func TestHealthProcess_RealRedisLinuxSIGTERMWithActiveHTTPAndProbe(t *testing.T) {
	f := newRealHealthFixture(t)
	p, base := f.start(t, 100*time.Millisecond, 2*time.Second, 300*time.Millisecond)
	f.setup(t)
	probe, user := f.arm(true), f.arm(false)
	requestDone := make(chan error, 1)
	go func() {
		req, err := http.NewRequest("POST", base+"/v1/chat/completions", strings.NewReader(`{"model":"gpt-4o-demo","messages":[{"role":"user","content":"inflight shutdown"}]}`))
		if err != nil {
			requestDone <- err
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+f.user)
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err == nil {
			_, err = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()
	awaitHealthBarrier(t, user.entered)
	awaitHealthBarrier(t, probe.entered)
	before, err := store.NewVersionedHealthState(f.redis).LoadVersioned(context.Background(), int64(f.channel.ID))
	require.NoError(t, err)
	for _, gate := range []*healthGate{user, probe} {
		select {
		case <-gate.done:
			t.Fatal("HTTP/probe barrier ended before SIGTERM")
		default:
		}
	}
	started := time.Now()
	require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("actual Linux SIGTERM failed to join")
	}
	require.NoError(t, p.err)
	awaitHealthBarrier(t, user.done)
	awaitHealthBarrier(t, probe.done)
	require.True(t, user.canceled.Load(), "HTTP shutdown expiry must cancel actual provider HTTP")
	require.True(t, probe.canceled.Load(), "health Close must cancel and join actual provider probe")
	require.Error(t, <-requestDone, "in-flight HTTP transport must be closed")
	elapsed := time.Since(started)
	require.GreaterOrEqual(t, elapsed, 280*time.Millisecond)
	require.Less(t, elapsed, 2*time.Second, "HTTP budget plus independent dependency budget, no probe timeout wait")
	entries := processLogEntries(t, p.logs)
	httpStopped, stopped, healthClosed, redisClosed, mysqlClosed := -1, -1, -1, -1, -1
	var stopTime time.Time
	for i, entry := range entries {
		if entry["msg"] == "http_handlers_stopped" {
			httpStopped = i
			require.Equal(t, true, entry["within_budget"])
		}
		if entry["msg"] == "health_service_stopped" {
			stopped = i
			stopTime, err = time.Parse(time.RFC3339Nano, entry["ts"].(string))
			require.NoError(t, err)
		}
		if entry["msg"] == "dependency_close_finished" {
			require.Equal(t, true, entry["ok"], "dependent close must finish inside independent budget")
			switch entry["name"] {
			case "health":
				healthClosed = i
			case "redis":
				redisClosed = i
			case "mysql":
				mysqlClosed = i
			}
		}
		if entry["msg"] == "依赖关闭失败" {
			t.Error("dependency phase failed")
		}
	}
	require.GreaterOrEqual(t, httpStopped, 0)
	require.Greater(t, stopped, httpStopped)
	require.Greater(t, healthClosed, stopped)
	require.Greater(t, redisClosed, healthClosed)
	require.Greater(t, mysqlClosed, redisClosed)
	// The canceled probe never becomes a false health signal; both actual
	// transports and child process have joined before checking final Redis.
	after, err := store.NewVersionedHealthState(f.redis).LoadVersioned(context.Background(), int64(f.channel.ID))
	require.NoError(t, err)
	require.Equal(t, before, after)
	for _, entry := range entries[stopped+1:] {
		msg, _ := entry["msg"].(string)
		require.False(t, strings.HasPrefix(msg, "health_"), "no late health activity after owners joined")
	}
	t.Logf("actual SIGTERM PID=%d: simultaneous provider HTTP + probe entered barriers, both canceled/joined; HTTP budget=300ms elapsed=%s, health stopped=%s before Redis/MySQL close; exit=0, no late signal/worker log", p.cmd.Process.Pid, elapsed, stopTime.Format(time.RFC3339Nano))
}
