//go:build redis_integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// This HTTP service exists only in an explicitly owned test-binary child. It
// does not change the shared mock or expose any gateway administration route.
type failoverMockStats struct {
	Mode     string         `json:"mode"`
	Business int            `json:"business"`
	Probes   int            `json:"probes"`
	Active   int            `json:"active"`
	Sent     int            `json:"sent"`
	Canceled int            `json:"canceled"`
	Models   map[string]int `json:"models"`
}
type failoverMock struct {
	mu         sync.Mutex
	stats      failoverMockStats
	gate       chan struct{}
	token, key string
}

func (m *failoverMock) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"status":"ok"}`)
		return
	}
	if r.URL.Path == "/state" || r.URL.Path == "/control" {
		if r.Header.Get("Authorization") != "Bearer "+m.token {
			w.WriteHeader(401)
			return
		}
		m.mu.Lock()
		defer m.mu.Unlock()
		if r.URL.Path == "/control" {
			var c struct {
				Mode    string `json:"mode"`
				Release bool   `json:"release"`
			}
			if r.Method != "POST" || json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&c) != nil {
				w.WriteHeader(400)
				return
			}
			if c.Release {
				if m.gate != nil {
					close(m.gate)
					m.gate = nil
				}
				m.stats.Mode = "normal"
			} else {
				switch c.Mode {
				case "normal", "429", "downtime", "ttfb", "interrupted", "bulk", "hold":
				default:
					w.WriteHeader(400)
					return
				}
				if m.gate != nil {
					close(m.gate)
					m.gate = nil
				}
				m.stats.Mode = c.Mode
				if c.Mode == "hold" || c.Mode == "ttfb" {
					m.gate = make(chan struct{})
				}
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.stats)
		return
	}
	if r.URL.Path != "/v1/chat/completions" || r.Method != "POST" {
		w.WriteHeader(404)
		return
	}
	if r.Header.Get("Authorization") != "Bearer "+m.key {
		w.WriteHeader(401)
		return
	}
	var fields map[string]json.RawMessage
	if json.NewDecoder(io.LimitReader(r.Body, 10*1024*1024)).Decode(&fields) != nil {
		w.WriteHeader(400)
		return
	}
	if isFixtureHealthProbe(fields) {
		m.mu.Lock()
		m.stats.Probes++
		m.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
		return
	}
	var model string
	_ = json.Unmarshal(fields["model"], &model)
	m.mu.Lock()
	m.stats.Business++
	m.stats.Active++
	m.stats.Models[model]++
	mode, gate := m.stats.Mode, m.gate
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.stats.Active--
		if r.Context().Err() != nil {
			m.stats.Canceled++
		}
		m.mu.Unlock()
	}()
	if mode == "hold" || mode == "ttfb" {
		select {
		case <-gate:
		case <-r.Context().Done():
			return
		}
	}
	switch mode {
	case "429":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"error":{"message":"owned rate limit","type":"rate_limit"}}`)
		return
	case "downtime":
		w.WriteHeader(503)
		return
	}
	stream := string(fields["stream"]) == "true"
	if !stream {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"model": model, "choices": []any{}, "usage": map[string]int{"prompt_tokens": 2, "completion_tokens": 3}})
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	count := 2
	if mode == "interrupted" {
		count = 3
	}
	if mode == "bulk" {
		count = 4096
	}
	content := "owned"
	if mode == "bulk" {
		content = strings.Repeat("x", 64*1024)
	}
	for i := 0; i < count; i++ {
		if r.Context().Err() != nil {
			return
		}
		b, _ := json.Marshal(map[string]any{"model": model, "choices": []any{map[string]any{"delta": map[string]string{"content": content}}}})
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		m.mu.Lock()
		m.stats.Sent++
		m.mu.Unlock()
	}
	if mode == "interrupted" {
		panic(http.ErrAbortHandler)
	}
	_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
}

func TestFailoverMockProcessHelper(t *testing.T) {
	if os.Getenv("GAIGATE_FAILOVER_MOCK_HELPER") == "" {
		return
	}
	require.Equal(t, "owned-v1", os.Getenv("GAIGATE_FAILOVER_MOCK_HELPER"))
	addr, token, key := os.Getenv("GAIGATE_FAILOVER_MOCK_ADDR"), os.Getenv("GAIGATE_FAILOVER_CONTROL_TOKEN"), os.Getenv("GAIGATE_FAILOVER_UPSTREAM_KEY")
	require.True(t, strings.HasPrefix(addr, "127.0.0.1:"))
	require.GreaterOrEqual(t, len(token), 32)
	require.NotEmpty(t, key)
	mock := &failoverMock{stats: failoverMockStats{Mode: "normal", Models: map[string]int{}}, token: token, key: key}
	server := newHTTPServer(addr, mock)
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	select {
	case <-ctx.Done():
		require.NoError(t, server.Close())
		server.handlers.wait()
		require.ErrorIs(t, <-done, http.ErrServerClosed)
	case err := <-done:
		require.NoError(t, err)
	}
}
