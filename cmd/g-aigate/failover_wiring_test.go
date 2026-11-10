package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/config"
	"gaigate/internal/store"
	"github.com/stretchr/testify/require"
)

type failoverFaultWriter struct {
	http.ResponseWriter
	mode string
}

func (w *failoverFaultWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *failoverFaultWriter) Write(p []byte) (int, error) {
	if w.mode == "short" && len(p) > 0 {
		n, err := w.ResponseWriter.Write(p[:len(p)-1])
		return n, err
	}
	return w.ResponseWriter.Write(p)
}
func (w *failoverFaultWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if w.mode == "flush" {
		return io.ErrClosedPipe
	}
	return err
}

func TestGatewayFormalFailoverUsesOneTCPWriter(t *testing.T) {
	cases := []struct {
		mode   string
		stream bool
	}{{"429", false}, {"429", true}, {"ttfb", false}, {"ttfb", true}, {"interrupted", true}, {"short", false}, {"short", true}, {"flush", true}, {"cancel", true}}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/stream=%v", tc.mode, tc.stream), func(t *testing.T) {
			db := gatewayTestDB(t)
			master := bytes.Repeat([]byte{23}, 32)
			key := gatewayUser(t, db, "failover-wiring")
			logs := newGatewayLogs()
			var primaryCalls, backupCalls atomic.Int32
			primaryEntered := make(chan struct{}, 1)
			primaryDone := make(chan struct{}, 1)
			handlerDone := make(chan struct{})
			success := func(w http.ResponseWriter, stream bool) {
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"complete\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\ndata: [DONE]\n\n")
				} else {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3},"complete":true}`)
				}
			}
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var fields map[string]json.RawMessage
				require.NoError(t, json.NewDecoder(r.Body).Decode(&fields))
				if respondToFixtureHealthProbe(w, fields) {
					return
				}
				primaryCalls.Add(1)
				primaryEntered <- struct{}{}
				defer func() { primaryDone <- struct{}{} }()
				require.Equal(t, "Bearer wiring-primary-secret", r.Header.Get("Authorization"))
				require.JSONEq(t, `"mapped-primary"`, string(fields["model"]))
				switch tc.mode {
				case "429":
					w.Header().Set("X-Failed-Attempt", "must-not-copy")
					w.WriteHeader(429)
					_, _ = io.WriteString(w, `{"error":{"message":"private-body","type":"rate_limit"}}`)
				case "ttfb", "cancel":
					<-r.Context().Done()
				case "interrupted":
					w.Header().Set("Content-Type", "text/event-stream")
					for range 3 {
						_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n")
						w.(http.Flusher).Flush()
					}
					panic(http.ErrAbortHandler)
				default:
					success(w, tc.stream)
				}
			}))
			t.Cleanup(primary.Close)
			backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var fields map[string]json.RawMessage
				require.NoError(t, json.NewDecoder(r.Body).Decode(&fields))
				if respondToFixtureHealthProbe(w, fields) {
					return
				}
				backupCalls.Add(1)
				require.Equal(t, "Bearer wiring-backup-secret", r.Header.Get("Authorization"))
				require.JSONEq(t, `"mapped-backup"`, string(fields["model"]))
				success(w, tc.stream)
			}))
			t.Cleanup(backup.Close)
			first := gatewayChannel(t, db, master, "wiring-primary", "alias", "mapped-primary", primary.URL, "wiring-primary-secret", 10)
			second := gatewayChannel(t, db, master, "wiring-backup", "alias", "mapped-backup", backup.URL, "wiring-backup-secret", 5)
			require.NoError(t, store.NewModelRepo(db, store.NewConfigCache(0)).Create(context.Background(), &store.ModelAlias{Name: "alias", Strategy: "fallback_chain", DefaultChannelID: &first.ID, FallbackIDs: fmt.Sprintf("[%d]", second.ID)}))
			cfg := config.ProxyConfig{TTFBTimeout: 80 * time.Millisecond, OverallTimeout: time.Second, StreamBuffer: 1}
			gateway, stats := assembledGateway(t, db, master, cfg, logs, func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(handlerDone)
					if tc.mode == "short" || tc.mode == "flush" {
						w = &failoverFaultWriter{w, tc.mode}
					}
					next.ServeHTTP(w, r)
				})
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, err := http.NewRequestWithContext(ctx, "POST", gateway.URL+"/v1/chat/completions", strings.NewReader(fmt.Sprintf(`{"model":"alias","stream":%t,"messages":[{"role":"user","content":"private-message"}]}`, tc.stream)))
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer "+key)
			req.Header.Set("Content-Type", "application/json")
			type response struct {
				resp *http.Response
				data []byte
				err  error
			}
			done := make(chan response, 1)
			go func() {
				resp, err := gateway.Client().Do(req)
				var data []byte
				if resp != nil {
					data, err = io.ReadAll(resp.Body)
					_ = resp.Body.Close()
				}
				done <- response{resp, data, err}
			}()
			if tc.mode == "cancel" {
				select {
				case <-primaryEntered:
					cancel()
				case <-time.After(3 * time.Second):
					t.Fatal("primary not entered")
				}
			}
			var got response
			select {
			case got = <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("client did not exit")
			}
			select {
			case <-handlerDone:
			case <-time.After(3 * time.Second):
				t.Fatal("handler/feedback did not exit")
			}
			select {
			case <-primaryDone:
			case <-time.After(3 * time.Second):
				t.Fatal("primary transport did not exit")
			}
			require.Equal(t, int32(1), primaryCalls.Load())
			fallback := tc.mode == "429" || tc.mode == "ttfb"
			if fallback {
				require.NoError(t, got.err)
				require.NotNil(t, got.resp)
				require.Equal(t, 200, got.resp.StatusCode)
				require.Equal(t, int32(1), backupCalls.Load())
				require.Empty(t, got.resp.Header.Get("X-Failed-Attempt"))
				require.NotContains(t, string(got.data), "private-body")
				if tc.stream {
					c, u, d := frameCounts(t, got.data)
					require.Equal(t, [3]int{1, 1, 1}, [3]int{c, u, d})
				} else {
					require.JSONEq(t, `{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3},"complete":true}`, string(got.data))
				}
			} else {
				require.Error(t, got.err)
				require.Zero(t, backupCalls.Load())
				require.NotContains(t, string(got.data), "[DONE]")
				require.NotContains(t, string(got.data), "gaigate_internal")
				if tc.mode == "interrupted" {
					require.Equal(t, 3, strings.Count(string(got.data), "data:"))
					require.NotContains(t, string(got.data), "usage")
				}
			}
			snapshot := <-stats
			if fallback {
				require.True(t, snapshot.written)
				require.Equal(t, len(got.data), snapshot.size)
			}
			attempts, requests := 0, 0
			var last map[string]any
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var row map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &row))
				switch row["msg"] {
				case "upstream attempt finished":
					attempts++
					last = row
				case "request_done":
					requests++
					want := first.ID
					if fallback {
						want = second.ID
					}
					require.EqualValues(t, want, row["channel_id"])
				}
			}
			require.Equal(t, 1, requests)
			wantAttempts := 1
			if fallback {
				wantAttempts = 2
			}
			require.Equal(t, wantAttempts, attempts)
			if tc.mode == "short" || tc.mode == "flush" {
				require.Equal(t, "write_error", last["result_kind"])
				require.Equal(t, true, last["wrote_header"])
			}
			for _, secret := range []string{key, "wiring-primary-secret", "wiring-backup-secret", "private-message", "private-body", primary.URL, backup.URL} {
				require.NotContains(t, logs.String(), secret)
			}
		})
	}
}
