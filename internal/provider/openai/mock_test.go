package openai_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"gaigate/internal/mock"
	"gaigate/internal/provider"
	"gaigate/internal/provider/openai"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

const mockTestKey = "sk-provider-132-test"

type watchedEngine struct {
	mock.Engine
	resolved chan struct{}
	once     sync.Once
}

func (e *watchedEngine) Resolve(key, override string) (mock.Scenario, mock.ScenarioParams, error) {
	s, p, err := e.Engine.Resolve(key, override)
	e.once.Do(func() { close(e.resolved) })
	return s, p, err
}

type synchronizedLog struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (b *synchronizedLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.Write(p)
}
func (b *synchronizedLog) text() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.data.String()
}

func TestRepositoryMockScenarios(t *testing.T) {
	for _, scenario := range []string{"normal", "slow", "downtime", "refuse", "rate_limited", "interrupted"} {
		t.Run(scenario, func(t *testing.T) {
			engine := &watchedEngine{Engine: mock.NewEngine(), resolved: make(chan struct{})}
			var log synchronizedLog
			r := gin.New()
			r.Use(mock.RequestLog(mock.NewLogger(&log))) // No recover: preserve real HTTP aborts.
			mock.RegisterChatRoutes(r, engine)
			mock.RegisterControlRoutes(r, engine)
			exited := make(chan struct{}, 4)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
				if rq.URL.Path == "/v1/chat/completions" {
					defer func() { exited <- struct{}{} }()
				}
				r.ServeHTTP(w, rq)
			}))
			defer s.Close()
			params := map[string]any{"chunk_interval": "0s"}
			selected := scenario
			if scenario == "slow" {
				params["ttfb_delay"] = "2s"
			}
			if scenario == "refuse" {
				selected = "downtime"
				params["downtime_mode"] = "refuse"
			}
			controlBody, err := json.Marshal(map[string]any{"key": mockTestKey, "scenario": selected, "params": params})
			require.NoError(t, err)
			control, err := http.NewRequestWithContext(t.Context(), http.MethodPut, s.URL+"/admin/scenario", bytes.NewReader(controlBody))
			require.NoError(t, err)
			control.Header.Set("Content-Type", "application/json")
			response, err := s.Client().Do(control)
			require.NoError(t, err)
			_, err = io.Copy(io.Discard, response.Body)
			require.NoError(t, err)
			require.NoError(t, response.Body.Close())
			require.Equal(t, 200, response.StatusCode)
			p, err := openai.New(s.Client(), 2)
			require.NoError(t, err)
			ep := &provider.Endpoint{BaseURL: s.URL, APIKey: mockTestKey, Model: "upstream/model-v2"}
			req := &provider.ChatRequest{Model: "alias", Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"private-request-marker"}`)}}
			if scenario == "slow" {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				var group errgroup.Group
				group.Go(func() error {
					stream, err := p.ChatStream(ctx, ep, req)
					if stream != nil {
						_ = stream.Close()
					}
					return err
				})
				select {
				case <-engine.resolved:
				case <-time.After(3 * time.Second):
					t.Fatal("mock did not resolve slow snapshot")
				}
				started := time.Now()
				cancel()
				require.ErrorIs(t, group.Wait(), context.Canceled)
				select {
				case <-exited:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("slow handler did not exit")
				}
				require.Less(t, time.Since(started), 500*time.Millisecond)
				require.Contains(t, log.text(), `"outcome":"canceled"`)
			} else {
				stream, err := p.ChatStream(t.Context(), ep, req)
				if scenario == "downtime" || scenario == "refuse" || scenario == "rate_limited" {
					require.Nil(t, stream)
					var upstream *provider.UpstreamError
					require.ErrorAs(t, err, &upstream)
					require.True(t, upstream.Retryable)
					want := 503
					if scenario == "rate_limited" {
						want = 429
					}
					if scenario == "refuse" {
						want = 0
					}
					require.Equal(t, want, upstream.StatusCode)
				} else {
					require.NoError(t, err)
					defer func() { _ = stream.Close() }()
					var events []provider.StreamEvent
					for e := range stream.Events() {
						events = append(events, e)
					}
					require.NoError(t, stream.Close())
					chunks := 8
					if scenario == "interrupted" {
						chunks = 3
						require.Len(t, events, 4)
					} else {
						require.Len(t, events, 10)
					}
					for i := range chunks {
						require.Equal(t, provider.Chunk, events[i].Kind)
						var payload struct {
							Model   string `json:"model"`
							Choices []struct {
								Delta struct {
									Content string `json:"content"`
								} `json:"delta"`
							} `json:"choices"`
						}
						require.NoError(t, json.Unmarshal(events[i].Raw, &payload))
						require.Equal(t, ep.Model, payload.Model)
						require.Len(t, payload.Choices, 1)
						require.Equal(t, fmt.Sprintf("mock-%d ", i+1), payload.Choices[0].Delta.Content)
					}
					if scenario == "interrupted" {
						require.Equal(t, provider.Error, events[3].Kind)
						require.ErrorIs(t, events[3].Err, provider.ErrStreamInterrupted)
						require.ErrorIs(t, events[3].Err, io.ErrUnexpectedEOF)
						for _, e := range events {
							require.NotEqual(t, provider.Done, e.Kind)
							require.NotEqual(t, provider.UsageDone, e.Kind)
						}
					} else {
						require.Equal(t, provider.UsageDone, events[8].Kind)
						require.Equal(t, &provider.Usage{InputTokens: 12, OutputTokens: 34}, events[8].Usage)
						require.Equal(t, provider.Done, events[9].Kind)
						require.Equal(t, "[DONE]", string(events[9].Raw))
						chat, usage, err := p.Chat(t.Context(), ep, req)
						require.NoError(t, err)
						require.Equal(t, &provider.Usage{InputTokens: 12, OutputTokens: 34}, usage)
						require.Contains(t, string(chat.Body), "mock-1 mock-2 mock-3 mock-4 mock-5 mock-6 mock-7 mock-8 ")
					}
				}
			}
			require.NotContains(t, log.text(), mockTestKey)
			require.NotContains(t, log.text(), "private-request-marker")
		})
	}
}
