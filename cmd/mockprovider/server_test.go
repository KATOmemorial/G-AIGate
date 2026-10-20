package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"gaigate/internal/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockAddr(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", "127.0.0.1:18080"}, {":18080", ":18080"},
		{"127.0.0.1:18086", "127.0.0.1:18086"}, {"localhost:1", "localhost:1"},
		{"[::1]:65535", "[::1]:65535"},
	} {
		got, err := mockAddr(tc.input)
		require.NoError(t, err)
		assert.Equal(t, tc.want, got)
	}
	for _, input := range []string{
		" ", "127.0.0.1", "18080", "http://127.0.0.1:18080", "::1:18080",
		":", ":0", ":-1", ":65536", ":999999999999999999999", ":http", ":+80", " :18080", ":18080\n",
	} {
		t.Run(input, func(t *testing.T) {
			_, err := mockAddr(input)
			require.ErrorContains(t, err, "MOCK_ADDR")
		})
	}
}

func TestRunReportsInvalidConfigAndOccupiedAddress(t *testing.T) {
	logger := mock.NewLogger(io.Discard)
	require.ErrorContains(t, run(t.Context(), ":65536", logger), "MOCK_ADDR")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	require.ErrorContains(t, run(t.Context(), listener.Addr().String(), logger), "listen on MOCK_ADDR")
}

func TestHealthzAndRouterLogs(t *testing.T) {
	var logs bytes.Buffer
	s := httptest.NewServer(newHandler(mock.NewEngine(), mock.NewLogger(&logs)))
	defer s.Close()
	resp, err := s.Client().Get(s.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, 200, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, `{"status":"ok"}`, string(body))
	assert.True(t, strings.HasPrefix(resp.Header.Get("X-Request-Id"), "req_"))
	s.Close() // No concurrent log writes remain before inspecting the buffer.
	var record map[string]any
	require.NoError(t, json.Unmarshal(logs.Bytes(), &record))
	assert.Contains(t, record, "ts")
	assert.NotContains(t, record, "time")
	assert.Equal(t, "completed", record["outcome"])
	assert.Equal(t, "", record["model"])
	assert.Equal(t, "", record["scenario"])
	assert.Equal(t, float64(0), record["chunks_written"])
	assert.Equal(t, resp.Header.Get("X-Request-Id"), record["request_id"])
}

type closeObservedListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (listener *closeObservedListener) Close() error {
	err := listener.Listener.Close()
	listener.once.Do(func() { close(listener.closed) })
	return err
}

func testListener(t *testing.T) *closeObservedListener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	return &closeObservedListener{Listener: listener, closed: make(chan struct{})}
}

func waitSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

func TestShutdownWaitsGracefullyAndStopsAccepting(t *testing.T) {
	listener := testListener(t)
	entered, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(exited)
		close(entered)
		select {
		case <-release:
			w.Write([]byte("finished"))
		case <-r.Context().Done():
			t.Error("signal canceled an in-flight request before the graceful budget expired")
		}
	})
	go func() { done <- serve(ctx, listener, handler, mock.NewLogger(io.Discard)) }()
	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	clientDone := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + listener.Addr().String())
		if err == nil {
			defer resp.Body.Close()
			var body []byte
			body, err = io.ReadAll(resp.Body)
			if string(body) != "finished" {
				err = errors.New("graceful response was truncated")
			}
		}
		clientDone <- err
	}()
	waitSignal(t, entered, "handler never entered")
	cancel()
	waitSignal(t, listener.closed, "Shutdown did not stop accepting")
	connection, err := net.DialTimeout("tcp", listener.Addr().String(), 200*time.Millisecond)
	if connection != nil {
		connection.Close()
	}
	require.Error(t, err)
	select {
	case err := <-done:
		t.Fatalf("shutdown returned before handler exit: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-clientDone)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("shutdown did not finish after handler release")
	}
	waitSignal(t, exited, "handler exit was not observed")
}

type observedWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *observedWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

// §5⑬: real 3s budget, not a shortened test-only constant. The production chat
// is in an hour-long first-byte wait; Close + cancellation must end it, close
// the client's connection, and finish the handler before serve returns.
func TestShutdownThreeSecondBudgetForcesCloseAndWaitsHandlers(t *testing.T) {
	require.Equal(t, 3*time.Second, shutdownBudget)
	listener := testListener(t)
	e := mock.NewEngine()
	p := mock.DefaultScenarioParams()
	p.TTFBDelay = time.Hour
	require.NoError(t, e.Set("sk-close-test", mock.Slow, p))
	var logs bytes.Buffer // Inspected only after serve and its handlers have exited.
	logger := mock.NewLogger(&logs)
	router := newHandler(e, logger)
	waiting, exited := make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(exited)
		requestCtx := &observedWaitContext{Context: r.Context(), waiting: waiting}
		router.ServeHTTP(w, r.WithContext(requestCtx))
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, listener, handler, logger) }()
	client := &http.Client{Timeout: 6 * time.Second}
	defer client.CloseIdleConnections()
	clientDone := make(chan error, 1)
	go func() {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+listener.Addr().String()+"/v1/chat/completions", strings.NewReader(`{"model":"close-model","messages":[{}]}`))
		if err == nil {
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", "Bearer sk-close-test")
			resp, callErr := client.Do(request)
			err = callErr
			if resp != nil {
				resp.Body.Close()
				err = errors.New("slow response unexpectedly returned headers")
			}
		}
		clientDone <- err
	}()
	waitSignal(t, waiting, "slow handler did not reach its wait")
	started := time.Now()
	cancel()
	waitSignal(t, listener.closed, "Shutdown did not close listener")
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(4 * time.Second):
		t.Fatal("forced shutdown exceeded the 3s budget plus cleanup margin")
	}
	elapsed := time.Since(started)
	assert.GreaterOrEqual(t, elapsed, 2900*time.Millisecond)
	assert.Less(t, elapsed, 4*time.Second)
	select {
	case <-exited:
	default:
		t.Fatal("serve returned without waiting for handler exit")
	}
	require.Error(t, <-clientDone)
	assert.Contains(t, logs.String(), "mockprovider_shutdown_forced")
	assert.Contains(t, logs.String(), `"outcome":"canceled"`)
	assert.Contains(t, logs.String(), `"chunks_written":0`)
	assert.NotContains(t, logs.String(), "sk-close-test")
	t.Logf("forced close and handler exit after %s", elapsed)
}

func TestHandlerTrackerRejectsWorkAfterStop(t *testing.T) {
	called := false
	tracker := &handlerTracker{next: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true })}
	tracker.stopAndWait()
	response := httptest.NewRecorder()
	tracker.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, 503, response.Code)
	assert.False(t, called)
}
