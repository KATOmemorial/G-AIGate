package meter_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gaigate/internal/meter"
	"gaigate/internal/provider"
	"gaigate/internal/provider/openai"
	"gaigate/internal/proxy"
	"github.com/stretchr/testify/require"
)

func TestActualProviderProxyCollector(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, mode := range []string{"missing", "zero", "nonzero"} {
			t.Run(mode+map[bool]string{true: "/stream", false: "/nonstream"}[stream], func(t *testing.T) {
				usageJSON := ""
				var wantUsage *provider.Usage
				if mode != "missing" {
					wantUsage = &provider.Usage{}
					usageJSON = `,"usage":{"prompt_tokens":0,"completion_tokens":0}`
					if mode == "nonzero" {
						*wantUsage = provider.Usage{InputTokens: 7, OutputTokens: 3}
						usageJSON = `,"usage":{"prompt_tokens":7,"completion_tokens":3}`
					}
				}
				body := " \n" + `{"choices":[{"message":{"role":"assistant","content":"中🙂abc"}}]` + usageJSON + `} ` + "\r\n"
				frames := []string{`{"choices":[{"delta":{"role":"assistant","content":"中🙂"}}]}`, `{"choices":[{"delta":{"content":"abc"}}]}`}
				if wantUsage != nil {
					frames = append(frames, `{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`, `{"choices":[]`+usageJSON+`}`)
				}
				var sse strings.Builder
				for _, frame := range frames {
					sse.WriteString("data: " + frame + "\n\n")
				}
				sse.WriteString("data: [DONE]\n\n")
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, sse.String())
					} else {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, body)
					}
				}))
				defer up.Close()
				p, err := openai.New(up.Client(), 1)
				require.NoError(t, err)
				req := &provider.ChatRequest{Model: "public", Stream: stream, Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"中🙂"}`)}, Extra: map[string]json.RawMessage{"stream_options": []byte(`{"include_usage":false}`)}}
				before, err := json.Marshal(req)
				require.NoError(t, err)
				ep := &provider.Endpoint{BaseURL: up.URL, Model: "mapped", APIKey: "fixture-key"}
				type outcome struct {
					result                proxy.ForwardResult
					response              *provider.ChatResponse
					measurement           meter.Measurement
					forwardErr, finishErr error
					callbacks             int
					unchanged             bool
				}
				completed := make(chan outcome, 1)
				down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					c, err := meter.NewCollector(req, 4)
					if err != nil {
						completed <- outcome{finishErr: err}
						return
					}
					out := outcome{unchanged: true}
					opts := proxy.ForwardOptions{TTFBTimeout: time.Second, OverallTimeout: 5 * time.Second}
					f := proxy.NewForwarder()
					if stream {
						out.result, out.forwardErr = f.Forward(r.Context(), w, func(ctx context.Context) (provider.Stream, error) { return p.ChatStream(ctx, ep, req) }, opts, func(e provider.StreamEvent) {
							raw := append([]byte(nil), e.Raw...)
							var u provider.Usage
							if e.Usage != nil {
								u = *e.Usage
							}
							c.Observe(e)
							out.callbacks++
							out.unchanged = out.unchanged && bytes.Equal(raw, e.Raw) && (e.Usage == nil || u == *e.Usage)
						})
					} else {
						out.result, out.forwardErr = f.ForwardNonStream(r.Context(), w, func(ctx context.Context) (*provider.ChatResponse, *provider.Usage, error) {
							response, u, err := p.Chat(ctx, ep, req)
							out.response = response
							return response, u, err
						}, opts)
						if out.response != nil {
							out.unchanged = string(out.response.Body) == body
						}
					}
					// This test follows the future consumer contract: only a successful
					// attempt finishes. It does not wire production accounting.
					if out.forwardErr == nil && out.result.Kind == proxy.ResultSuccess {
						out.measurement, out.finishErr = c.Finish(out.result.Usage, out.response)
					}
					completed <- out
				}))
				defer down.Close()
				resp, err := down.Client().Get(down.URL)
				require.NoError(t, err)
				got, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				out := <-completed
				require.NoError(t, out.forwardErr)
				require.NoError(t, out.finishErr)
				require.Equal(t, proxy.ResultSuccess, out.result.Kind)
				require.True(t, out.unchanged)
				require.Equal(t, http.StatusOK, resp.StatusCode)
				if stream {
					require.Equal(t, sse.String(), string(got))
					require.Equal(t, len(frames), out.callbacks)
					require.Equal(t, 2, out.result.Chunks)
				} else {
					require.Equal(t, body, string(got))
					require.Zero(t, out.callbacks)
				}
				if wantUsage == nil {
					require.Equal(t, meter.Measurement{Usage: provider.Usage{InputTokens: 8, OutputTokens: 2}, Estimated: true}, out.measurement)
				} else {
					require.Equal(t, meter.Measurement{Usage: *wantUsage}, out.measurement)
					require.Equal(t, wantUsage, out.result.Usage)
				}
				after, err := json.Marshal(req)
				require.NoError(t, err)
				require.Equal(t, before, after)
				if out.response != nil && out.response.Usage != nil {
					require.NotSame(t, out.response.Usage, out.result.Usage)
				}
			})
		}
	}
}
