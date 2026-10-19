package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestLogger 输出到内存 buffer 的 JSON 日志器，供断言关闭日志。
func newTestLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, nil))
}

// startTestServer 起一个真实 http.Server（随机端口），返回 server 与 base URL。
func startTestServer(t *testing.T, h http.Handler) (*http.Server, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }() // 测试内 goroutine：模拟服务主循环
	t.Cleanup(func() { _ = srv.Close() })
	return srv, "http://" + ln.Addr().String()
}

// 优雅关闭核心语义：Shutdown 等待在途请求完成后，依赖按登记逆序关闭。
func TestShutdownWaitsInFlightAndClosesDepsReverse(t *testing.T) {
	release := make(chan struct{})
	handlerDone := make(chan struct{})
	srv, base := startTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		w.WriteHeader(http.StatusOK)
		close(handlerDone)
	}))

	respDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(base)
		if err == nil {
			_ = resp.Body.Close()
		}
		respDone <- err
	}()
	time.Sleep(100 * time.Millisecond) // 确保请求已进入 handler（在途）

	var mu sync.Mutex
	var order []string
	mk := func(name string) dependency {
		return dependency{name: name, close: func(ctx context.Context) error {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			return nil
		}}
	}
	// 登记顺序：按装配顺序（对应未来 mysql → redis → meter-drain 的装配序）
	deps := []dependency{mk("mysql"), mk("redis"), mk("meter-drain")}

	var buf bytes.Buffer
	shutdownDone := make(chan struct{})
	go func() {
		shutdown(2*time.Second, srv, deps, newTestLogger(&buf))
		close(shutdownDone)
	}()

	// shutdown 应阻塞等待在途请求，而非立即关闭依赖
	select {
	case <-shutdownDone:
		t.Fatal("在途请求未完成时 shutdown 不应返回")
	case <-time.After(150 * time.Millisecond):
	}

	// 10.2 §7 风险②：Shutdown 等待期间新连接立即被拒（非 bug，
	// listener 已关闭），须有断言覆盖避免误判
	_, err := http.Get(base)
	require.Error(t, err, "Shutdown 等待期间新连接应立即失败")

	// 放行在途请求 → shutdown 完成，请求正常收到响应（无截断）
	close(release)
	<-handlerDone
	require.NoError(t, <-respDone)
	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("放行后 shutdown 未在期限内完成")
	}

	mu.Lock()
	defer mu.Unlock()
	// 逆序：meter-drain → redis → mysql（10.2 §3：落账先 drain 再关 DB）
	assert.Equal(t, []string{"meter-drain", "redis", "mysql"}, order)
}

// 超时截断：在途请求超过上限 → Shutdown 报错但依赖仍逆序关闭。
func TestShutdownTimeoutStillClosesDeps(t *testing.T) {
	srv, base := startTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))

	respDone := make(chan error, 1)
	go func() {
		resp, err := http.Get(base)
		if err == nil {
			_ = resp.Body.Close()
		}
		respDone <- err
	}()
	time.Sleep(50 * time.Millisecond)

	var order []string
	deps := []dependency{
		{name: "a", close: func(ctx context.Context) error { order = append(order, "a"); return nil }},
		{name: "b", close: func(ctx context.Context) error { order = append(order, "b"); return nil }},
	}

	var buf bytes.Buffer
	shutdown(50*time.Millisecond, srv, deps, newTestLogger(&buf)) // 须在超时后返回

	// 依赖仍在 Shutdown 截断后按逆序关闭
	assert.Equal(t, []string{"b", "a"}, order)
	// 超时被记录为 Error 日志
	assert.Contains(t, buf.String(), "优雅关闭超时/出错")
}

// 单个依赖关闭失败：记录 Error 但不阻断其余依赖的关闭。
func TestShutdownDepCloseFailureIsolated(t *testing.T) {
	srv, _ := startTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var order []string
	deps := []dependency{
		{name: "mysql", close: func(ctx context.Context) error {
			order = append(order, "mysql")
			return errors.New("mysql: connection reset")
		}},
		{name: "meter-drain", close: func(ctx context.Context) error {
			order = append(order, "meter-drain")
			return nil
		}},
	}

	var buf bytes.Buffer
	shutdown(time.Second, srv, deps, newTestLogger(&buf))

	// 失败不阻断逆序链
	assert.Equal(t, []string{"meter-drain", "mysql"}, order)
	assert.Contains(t, buf.String(), "mysql: connection reset")

	// 完成日志为 JSON 且含退出消息
	var lastLine map[string]any
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.NotEmpty(t, lines)
	require.NoError(t, json.Unmarshal([]byte(lines[len(lines)-1]), &lastLine))
	assert.Equal(t, "g-aigate 已退出", lastLine["msg"])
}

// 评审阻断项①：newLogger 输出的时间键为 ts（00 §8.3），非 slog 默认 time。
func TestNewLoggerTsKey(t *testing.T) {
	var buf bytes.Buffer
	logger := newLogger(&buf, "info")
	logger.Info("hello", "k", "v")

	var m map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &m))
	assert.Contains(t, m, "ts")
	assert.NotContains(t, m, "time")
	assert.Equal(t, "hello", m["msg"])
	assert.Equal(t, "INFO", m["level"])
}

// 11.3 §3 / 10.2 §3.2 / 台账 #16：装配序 mysql → redis → usage-repo，
// 逆序关闭 drain(usage-repo) → redis → mysql（redis 夹在中间，非追加末尾）。
// 顺序反了则队列中未落库记录随 DB 关闭而丢失——这是关闭序列的兑现点。
func TestBuildDepsCloseOrderDrainRedisMySQL(t *testing.T) {
	srv, _ := startTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	var mu sync.Mutex
	var closed []string
	rec := func(name string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			closed = append(closed, name)
			mu.Unlock()
			return nil
		}
	}
	deps := buildDeps(rec("mysql"), rec("redis"), rec("usage-repo"))

	// 登记序（装配序）：mysql 在前、redis 居中、usage-repo 在后
	assert.Equal(t, []string{"mysql", "redis", "usage-repo"},
		[]string{deps[0].name, deps[1].name, deps[2].name})

	var buf bytes.Buffer
	shutdown(time.Second, srv, deps, newTestLogger(&buf))

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{"usage-repo", "redis", "mysql"}, closed,
		"关闭逆序：先 drain 落账，再关 redis，最后关 mysql")
}
