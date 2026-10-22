package access

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/provider/openai"
	"gaigate/internal/store"
)

type precheckFunc func(context.Context, *Principal) error

func (f precheckFunc) Precheck(ctx context.Context, p *Principal) error { return f(ctx, p) }

type chatExecutorFunc func(context.Context, http.ResponseWriter, *Principal, *provider.ChatRequest) error

func (f chatExecutorFunc) Execute(ctx context.Context, w http.ResponseWriter, p *Principal, req *provider.ChatRequest) error {
	return f(ctx, w, p, req)
}

type modelListerFunc func(context.Context) ([]store.ModelAlias, error)

func (f modelListerFunc) List(ctx context.Context) ([]store.ModelAlias, error) { return f(ctx) }

const validChatJSON = `{"model":"alias","messages":[{"role":"user","content":"hello"}]}`

// Real authentication, with isolated repositories. This is an injectable
// handler fixture, not the bootstrap/cmd gateway assembly delivered in M2.
func v1TestRouter(precheck Prechecker, executor ChatExecutor, models ModelLister) *gin.Engine {
	_, key, user := authFixture()
	auth := NewAuthenticator(keyLookupStub{find: func(context.Context, string) ([]store.APIKey, error) {
		return []store.APIKey{key}, nil
	}}, userLookupStub{get: func(context.Context, int64) (*store.User, error) { return &user, nil }})
	r := gin.New()
	r.Use(middleware.RequestID())
	RegisterV1Routes(r, AuthMiddleware(auth), precheck, executor, models)
	return r
}

func noPrecheck(context.Context, *Principal) error         { return nil }
func noModels(context.Context) ([]store.ModelAlias, error) { return nil, nil }
func emptyChat(_ context.Context, w http.ResponseWriter, _ *Principal, _ *provider.ChatRequest) error {
	w.WriteHeader(204)
	return nil
}

func v1Request(method, path, body string) *http.Request {
	raw, _, _ := authFixture()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", "Bearer "+raw)
	return req
}

func TestV1RequiredDependencies(t *testing.T) {
	auth := func(c *gin.Context) { c.Next() }
	for _, tc := range []struct {
		name   string
		auth   gin.HandlerFunc
		pre    Prechecker
		exec   ChatExecutor
		models ModelLister
	}{
		{"auth", nil, precheckFunc(noPrecheck), chatExecutorFunc(emptyChat), modelListerFunc(noModels)},
		{"precheck", auth, nil, chatExecutorFunc(emptyChat), modelListerFunc(noModels)},
		{"executor", auth, precheckFunc(noPrecheck), nil, modelListerFunc(noModels)},
		{"models", auth, precheckFunc(noPrecheck), chatExecutorFunc(emptyChat), nil},
		{"typed nil", auth, precheckFunc(nil), chatExecutorFunc(emptyChat), modelListerFunc(noModels)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Panics(t, func() { RegisterV1Routes(gin.New(), tc.auth, tc.pre, tc.exec, tc.models) })
		})
	}
	// An injected middleware that forgets identity must fail closed.
	r := gin.New()
	RegisterV1Routes(r, auth, precheckFunc(noPrecheck), chatExecutorFunc(emptyChat), modelListerFunc(noModels))
	for _, path := range []string{"/v1/models", "/v1/chat/completions"} {
		method := "GET"
		if path == "/v1/chat/completions" {
			method = "POST"
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, v1Request(method, path, validChatJSON))
		assert.Equal(t, 401, w.Code)
	}
}

func TestV1InvalidInputHasZeroBusinessCalls(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"empty", ""}, {"null", "null"}, {"array", "[]"}, {"number", "1"}, {"string", `"x"`},
		{"truncated", `{"model":"x"`}, {"trailing object", validChatJSON + `{}`}, {"trailing garbage", validChatJSON + `x`},
		{"missing model", `{"messages":[{"role":"user"}]}`}, {"empty model", `{"model":"","messages":[{"role":"user"}]}`},
		{"null model", `{"model":null,"messages":[{"role":"user"}]}`}, {"number model", `{"model":1,"messages":[{"role":"user"}]}`},
		{"no messages", `{"model":"x"}`}, {"null messages", `{"model":"x","messages":null}`},
		{"empty messages", `{"model":"x","messages":[]}`}, {"object messages", `{"model":"x","messages":{}}`},
		{"null message", `{"model":"x","messages":[null]}`}, {"scalar message", `{"model":"x","messages":[1]}`},
		{"array message", `{"model":"x","messages":[[]]}`}, {"no role", `{"model":"x","messages":[{}]}`},
		{"empty role", `{"model":"x","messages":[{"role":""}]}`}, {"null role", `{"model":"x","messages":[{"role":null}]}`},
		{"number role", `{"model":"x","messages":[{"role":1}]}`},
		{"null stream", `{"model":"x","messages":[{"role":"user"}],"stream":null}`},
		{"number stream", `{"model":"x","messages":[{"role":"user"}],"stream":1}`},
		{"string stream", `{"model":"x","messages":[{"role":"user"}],"stream":"true"}`},
		{"array options", `{"model":"x","messages":[{"role":"user"}],"stream_options":[]}`},
		{"bool options", `{"model":"x","messages":[{"role":"user"}],"stream_options":true}`},
		{"null include usage", `{"model":"x","messages":[{"role":"user"}],"stream_options":{"include_usage":null}}`},
		{"number include usage", `{"model":"x","messages":[{"role":"user"}],"stream_options":{"include_usage":1}}`},
		{"string include usage", `{"model":"x","messages":[{"role":"user"}],"stream_options":{"include_usage":"true"}}`},
		{"duplicate model", `{"model":"x","model":"y","messages":[{"role":"user"}]}`},
		{"escaped duplicate", `{"model":"x","\u006dodel":"y","messages":[{"role":"user"}]}`},
		{"duplicate unknown", `{"model":"x","messages":[{"role":"user"}],"x":1,"x":2}`},
		{"invalid utf8", "{\"model\":\"\xff\",\"messages\":[{\"role\":\"user\"}]}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			r := v1TestRouter(precheckFunc(func(context.Context, *Principal) error { calls++; return nil }),
				chatExecutorFunc(func(context.Context, http.ResponseWriter, *Principal, *provider.ChatRequest) error {
					calls++
					return nil
				}), modelListerFunc(noModels))
			w := &responseCounter{ResponseRecorder: httptest.NewRecorder()}
			r.ServeHTTP(w, v1Request("POST", "/v1/chat/completions", tc.body))
			assert.Equal(t, 400, w.Code)
			assert.Equal(t, 0, calls)
			assert.Equal(t, 1, w.headers)
			assert.Equal(t, 1, w.writes)
			checkErrorEnvelope(t, w.ResponseRecorder, "gaigate_invalid_request")
		})
	}
}

type countedBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *countedBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	b.read += n
	return n, err
}
func (b *countedBody) Close() error { b.closed = true; return nil }

func TestV1MediaAndBoundedReading(t *testing.T) {
	for _, media := range []string{"", "text/plain", "application/problem+json", "application/json; broken"} {
		t.Run(media, func(t *testing.T) {
			calls := 0
			r := v1TestRouter(precheckFunc(func(context.Context, *Principal) error { calls++; return nil }), chatExecutorFunc(emptyChat), modelListerFunc(noModels))
			req := v1Request("POST", "/v1/chat/completions", validChatJSON)
			req.Header.Set("Content-Type", media)
			body := &countedBody{Reader: req.Body}
			req.Body = body
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			assert.Equal(t, 415, w.Code)
			checkErrorEnvelope(t, w, "gaigate_unsupported_media_type")
			assert.Zero(t, body.read)
			assert.Zero(t, calls)
		})
	}
	for _, delta := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(delta), func(t *testing.T) {
			pre, exec := 0, 0
			r := v1TestRouter(precheckFunc(func(context.Context, *Principal) error { pre++; return nil }),
				chatExecutorFunc(func(ctx context.Context, w http.ResponseWriter, p *Principal, req *provider.ChatRequest) error {
					exec++
					return emptyChat(ctx, w, p, req)
				}), modelListerFunc(noModels))
			body := validChatJSON + strings.Repeat(" ", chatBodyLimit+delta-len(validChatJSON))
			for _, length := range []int64{int64(len(body)), -1} { // known and chunked length
				pre, exec = 0, 0
				req := v1Request("POST", "/v1/chat/completions", body)
				req.ContentLength = length
				reader := &countedBody{Reader: req.Body}
				req.Body = reader
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				assert.True(t, reader.closed)
				if delta > 0 {
					assert.Equal(t, 413, w.Code)
					checkErrorEnvelope(t, w, "gaigate_request_too_large")
					assert.Zero(t, pre)
					assert.Zero(t, exec)
					assert.Equal(t, chatBodyLimit+1, reader.read)
				} else {
					assert.Equal(t, 204, w.Code)
					assert.Equal(t, 1, pre)
					assert.Equal(t, 1, exec)
				}
			}
		})
	}
	// This larger body proves consumption stops after limit+1, independently of
	// Content-Length. Authentication failures must consume no body at all.
	r := v1TestRouter(precheckFunc(noPrecheck), chatExecutorFunc(emptyChat), modelListerFunc(noModels))
	for _, authorized := range []bool{true, false} {
		req := v1Request("POST", "/v1/chat/completions", "")
		body := &countedBody{Reader: io.LimitReader(zeroReader{}, 2*chatBodyLimit)}
		req.Body, req.ContentLength = body, -1
		if !authorized {
			req.Header.Del("Authorization")
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if authorized {
			assert.Equal(t, 413, w.Code)
			assert.Equal(t, chatBodyLimit+1, body.read)
		} else {
			assert.Equal(t, 401, w.Code)
			assert.Zero(t, body.read)
		}
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestV1OrderContextAndPrecheckFailure(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			var order []string
			check := func(ctx context.Context, p *Principal) {
				assert.Equal(t, "alias", middleware.ModelFromContext(ctx))
				assert.EqualValues(t, 3, p.UserID)
				identity, ok := PrincipalFromContext(ctx)
				require.True(t, ok)
				assert.Equal(t, p, identity)
			}
			r := v1TestRouter(precheckFunc(func(ctx context.Context, p *Principal) error {
				order = append(order, "precheck")
				check(ctx, p)
				if reject {
					return errors.New("private SQL cause")
				}
				return nil
			}), chatExecutorFunc(func(ctx context.Context, w http.ResponseWriter, p *Principal, req *provider.ChatRequest) error {
				order = append(order, "execute")
				check(ctx, p)
				assert.False(t, req.Stream)
				return emptyChat(ctx, w, p, req)
			}), modelListerFunc(noModels))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, v1Request("POST", "/v1/chat/completions", validChatJSON))
			if reject {
				assert.Equal(t, []string{"precheck"}, order)
				assert.Equal(t, 500, w.Code)
				assert.NotContains(t, w.Body.String(), "private SQL cause")
			} else {
				assert.Equal(t, []string{"precheck", "execute"}, order)
				assert.Equal(t, 204, w.Code)
			}
		})
	}
}

func TestV1ModelsEnvelopeAndIsolation(t *testing.T) {
	for _, aliases := range [][]store.ModelAlias{nil, {}, {{Name: "z", Strategy: "private", FallbackIDs: "[3]"}, {Name: "a", DisplayName: "display"}}} {
		business := 0
		r := v1TestRouter(precheckFunc(func(context.Context, *Principal) error { business++; return nil }),
			chatExecutorFunc(func(context.Context, http.ResponseWriter, *Principal, *provider.ChatRequest) error {
				business++
				return nil
			}),
			modelListerFunc(func(ctx context.Context) ([]store.ModelAlias, error) {
				_, ok := PrincipalFromContext(ctx)
				assert.True(t, ok)
				return aliases, nil
			}))
		w := httptest.NewRecorder()
		r.ServeHTTP(w, v1Request("GET", "/v1/models", "not JSON"))
		assert.Equal(t, 200, w.Code)
		if len(aliases) == 0 {
			assert.JSONEq(t, `{"object":"list","data":[]}`, w.Body.String())
		} else {
			assert.JSONEq(t, `{"object":"list","data":[{"id":"a","object":"model","owned_by":"gaigate"},{"id":"z","object":"model","owned_by":"gaigate"}]}`, w.Body.String())
			assert.Equal(t, "z", aliases[0].Name, "sorting must not mutate lister storage")
		}
		assert.Zero(t, business)
		w = httptest.NewRecorder()
		req := v1Request("GET", "/v1/models", "")
		req.Header.Del("Authorization")
		r.ServeHTTP(w, req)
		assert.Equal(t, 401, w.Code)
	}
	r := v1TestRouter(precheckFunc(noPrecheck), chatExecutorFunc(emptyChat), modelListerFunc(func(context.Context) ([]store.ModelAlias, error) { return nil, errors.New("private SQL error") }))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, v1Request("GET", "/v1/models", ""))
	assert.Equal(t, 500, w.Code)
	checkErrorEnvelope(t, w, "gaigate_internal")
}

func TestV1TransparentRequestCapturedByAdapter(t *testing.T) {
	for _, options := range []string{"", `,"stream_options":null`, `,"stream_options":{}`, `,"stream_options":{"include_usage":false,"extension":{"x":1}}`, `,"stream_options":{"include_usage":true}`} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%v", options, stream), func(t *testing.T) {
				captured := make(chan map[string]json.RawMessage, 1)
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var fields map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&fields); err != nil {
						t.Error(err)
						w.WriteHeader(500)
						return
					}
					assert.Equal(t, "Bearer upstream-fixture-key", r.Header.Get("Authorization"))
					assert.Empty(t, r.Header.Get("X-Mock-Scenario"))
					captured <- fields
					w.Header().Set("Content-Type", "application/json")
					if stream {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: [DONE]\n\n")
					} else {
						_, _ = io.WriteString(w, `{"choices":[],"custom":{"unchanged":true}}`)
					}
				}))
				defer upstream.Close()
				adapter, err := openai.New(upstream.Client(), 1)
				require.NoError(t, err)
				input := fmt.Sprintf(`{"model":"alias","stream":%v,"messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"c","function":{"arguments":"{}"}}]},{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:fixture"}}],"extension":{"a":1}}],"unknown":{"large":9007199254740993},"temperature":0.7%s}`, stream, options)
				r := v1TestRouter(precheckFunc(noPrecheck), chatExecutorFunc(func(ctx context.Context, w http.ResponseWriter, p *Principal, req *provider.ChatRequest) error {
					before, err := json.Marshal(req)
					require.NoError(t, err)
					ep := &provider.Endpoint{BaseURL: upstream.URL, APIKey: "upstream-fixture-key", Model: "mapped-upstream"}
					if stream {
						s, err := adapter.ChatStream(ctx, ep, req)
						require.NoError(t, err)
						for event := range s.Events() {
							assert.Equal(t, provider.Done, event.Kind)
						}
						require.NoError(t, s.Close())
						w.WriteHeader(204)
					} else {
						resp, _, err := adapter.Chat(ctx, ep, req)
						require.NoError(t, err)
						w.Header().Set("Content-Type", "application/json")
						_, err = w.Write(resp.Body)
						require.NoError(t, err)
					}
					after, err := json.Marshal(req)
					require.NoError(t, err)
					assert.Equal(t, before, after)
					return nil
				}), modelListerFunc(noModels))
				w := httptest.NewRecorder()
				request := v1Request("POST", "/v1/chat/completions", input)
				request.Header.Set("X-Mock-Scenario", "private")
				r.ServeHTTP(w, request)
				if !stream {
					assert.Equal(t, `{"choices":[],"custom":{"unchanged":true}}`, w.Body.String())
				}
				var got map[string]json.RawMessage
				select {
				case got = <-captured:
				case <-time.After(3 * time.Second):
					t.Fatal("validated request did not reach the upstream fixture")
				}
				var want map[string]json.RawMessage
				require.NoError(t, json.Unmarshal([]byte(input), &want))
				assert.JSONEq(t, `"mapped-upstream"`, string(got["model"]))
				for _, field := range []string{"messages", "unknown", "temperature", "stream"} {
					assert.JSONEq(t, string(want[field]), string(got[field]))
				}
				if stream {
					var opts map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(got["stream_options"], &opts))
					assert.Equal(t, "true", string(opts["include_usage"]))
					if strings.Contains(options, "extension") {
						assert.JSONEq(t, `{"x":1}`, string(opts["extension"]))
					}
				} else if options != "" {
					assert.JSONEq(t, string(want["stream_options"]), string(got["stream_options"]))
				} else {
					_, exists := got["stream_options"]
					assert.False(t, exists)
				}
			})
		}
	}
}

func TestV1ExecutionErrorsAndPrivacy(t *testing.T) {
	raw, _, _ := authFixture()
	safe := `{"error":{"message":"invalid input","type":"invalid_request_error","param":"messages","code":"bad_input","extra":{"a":1}},"private":"drop"}`
	up := func(status int, body string) error {
		return &provider.UpstreamError{StatusCode: status, Body: []byte(body), Cause: errors.New("secret cause " + raw)}
	}
	for _, tc := range []struct {
		name      string
		err       error
		status    int
		code      string
		preserved bool
	}{
		{"model", fmt.Errorf("wrapped: %w", ErrModelNotFound), 404, "gaigate_model_not_found", false},
		{"candidate", provider.ErrNoCandidate, 503, "gaigate_no_candidate", false},
		{"internal", errors.New("secret cause " + raw), 500, "gaigate_internal", false},
		{"401", up(401, safe), 502, "gaigate_upstream_auth_failed", false}, {"403", up(403, safe), 502, "gaigate_upstream_auth_failed", false},
		{"429", up(429, safe), 502, "gaigate_upstream_error", false}, {"503", up(503, safe), 502, "gaigate_upstream_error", false},
		{"redirect", up(302, safe), 502, "gaigate_upstream_error", false}, {"network", up(0, ""), 502, "gaigate_upstream_error", false},
		{"protocol", provider.ErrStreamInterrupted, 502, "gaigate_upstream_error", false},
		{"400", up(400, safe), 400, "", true}, {"404", up(404, safe), 404, "", true}, {"422", up(422, safe), 422, "", true},
		{"html", up(400, "<html>private</html>"), 502, "gaigate_upstream_error", false},
		{"incomplete", up(400, `{"error":`), 502, "gaigate_upstream_error", false},
		{"null", up(400, `{"error":null}`), 502, "gaigate_upstream_error", false},
		{"missing type", up(400, `{"error":{"message":"x"}}`), 502, "gaigate_upstream_error", false},
		{"bad code type", up(400, `{"error":{"message":"x","type":"invalid_request_error","code":{}}}`), 502, "gaigate_upstream_error", false},
		{"bad param type", up(400, `{"error":{"message":"x","type":"invalid_request_error","param":1}}`), 502, "gaigate_upstream_error", false},
		{"trailing", up(400, safe+`{}`), 502, "gaigate_upstream_error", false},
		{"duplicate", up(400, `{"error":{"message":"safe","type":"x"},"error":{"message":"unsafe","type":"x"}}`), 502, "gaigate_upstream_error", false},
		{"truncated flag", &provider.UpstreamError{StatusCode: 400, Body: []byte(safe), BodyTruncated: true}, 502, "gaigate_upstream_error", false},
		{"echo", up(400, strings.ReplaceAll(safe, "invalid input", raw)), 502, "gaigate_upstream_error", false},
		{"escaped echo", up(400, strings.ReplaceAll(safe, "invalid input", `\u0073`+raw[1:])), 502, "gaigate_upstream_error", false},
		{"field name echo", up(400, strings.ReplaceAll(safe, "extra", raw)), 502, "gaigate_upstream_error", false},
		{"other top field echo", up(400, strings.ReplaceAll(safe, "drop", raw)), 502, "gaigate_upstream_error", false},
		{"timeout", &ExecutionError{Cause: context.DeadlineExceeded, TimedOut: true}, 502, "gaigate_upstream_timeout", false},
		{"latched overall with canceled", &ExecutionError{Cause: errors.Join(context.DeadlineExceeded, context.Canceled), TimedOut: true}, 502, "gaigate_upstream_timeout", false},
		{"canceled", &ExecutionError{Cause: context.Canceled}, 200, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			old := slog.Default()
			slog.SetDefault(capturingLogger(&logs))
			defer slog.SetDefault(old)
			r := v1TestRouter(precheckFunc(noPrecheck), chatExecutorFunc(func(context.Context, http.ResponseWriter, *Principal, *provider.ChatRequest) error {
				return fmt.Errorf("outer: %w", tc.err)
			}), modelListerFunc(noModels))
			w := &responseCounter{ResponseRecorder: httptest.NewRecorder()}
			serve := func() { r.ServeHTTP(w, v1Request("POST", "/v1/chat/completions", validChatJSON)) }
			if tc.name == "canceled" {
				assert.PanicsWithValue(t, http.ErrAbortHandler, serve)
			} else {
				serve()
			}
			assert.Equal(t, tc.status, w.Code)
			if tc.code != "" {
				checkErrorEnvelope(t, w.ResponseRecorder, tc.code)
			}
			if tc.preserved {
				var got, original map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
				require.NoError(t, json.Unmarshal([]byte(safe), &original))
				assert.Len(t, got, 2)
				assert.JSONEq(t, string(original["error"]), string(got["error"]))
				assert.JSONEq(t, fmt.Sprintf("%q", w.Header().Get("X-Request-Id")), string(got["request_id"]))
			}
			if tc.name == "canceled" {
				assert.Zero(t, w.headers)
				assert.Zero(t, w.writes)
				assert.Empty(t, w.Body.String())
			} else {
				assert.Equal(t, 1, w.headers)
				assert.Equal(t, 1, w.writes)
			}
			for _, secret := range []string{raw, "secret cause", "private"} {
				assert.NotContains(t, w.Body.String(), secret)
				assert.NotContains(t, logs.String(), secret)
			}
		})
	}
}

func TestV1CommittedErrorAbortsWithoutAppending(t *testing.T) {
	for _, actuallyWrite := range []bool{true, false} {
		for _, reportedCommit := range []bool{true, false} {
			if !actuallyWrite && !reportedCommit {
				continue
			}
			t.Run(fmt.Sprintf("actual=%v/reported=%v", actuallyWrite, reportedCommit), func(t *testing.T) {
				r := v1TestRouter(precheckFunc(noPrecheck), chatExecutorFunc(func(_ context.Context, w http.ResponseWriter, _ *Principal, _ *provider.ChatRequest) error {
					if actuallyWrite {
						_, _ = io.WriteString(w, "data: partial\n\n")
					}
					return &ExecutionError{Cause: context.Canceled, WroteHeader: reportedCommit, TimedOut: true}
				}), modelListerFunc(noModels))
				w := httptest.NewRecorder()
				assert.PanicsWithValue(t, http.ErrAbortHandler, func() { r.ServeHTTP(w, v1Request("POST", "/v1/chat/completions", validChatJSON)) })
				if actuallyWrite {
					assert.Equal(t, "data: partial\n\n", w.Body.String())
				} else {
					assert.Empty(t, w.Body.String())
				}
			})
		}
	}
	cause := errors.New("private credential")
	err := &ExecutionError{Cause: cause}
	assert.ErrorIs(t, fmt.Errorf("wrapped: %w", err), cause)
	assert.NotContains(t, err.Error(), cause.Error())
	// Production Recover transparency and real HTTP interruption are M2/M3;
	// this test intentionally checks only the entry-point panic boundary.
}

func TestV1CanceledContextHonorsClassifiedTimeout(t *testing.T) {
	for _, timedOut := range []bool{false, true} {
		t.Run(fmt.Sprint(timedOut), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := v1TestRouter(precheckFunc(noPrecheck), chatExecutorFunc(func(context.Context, http.ResponseWriter, *Principal, *provider.ChatRequest) error {
				cancel()
				return &ExecutionError{Cause: errors.New("unrelated cleanup error"), TimedOut: timedOut}
			}), modelListerFunc(noModels))
			w := &responseCounter{ResponseRecorder: httptest.NewRecorder()}
			req := v1Request("POST", "/v1/chat/completions", validChatJSON).WithContext(ctx)
			serve := func() { r.ServeHTTP(w, req) }
			if timedOut {
				serve()
				assert.Equal(t, 502, w.Code)
				checkErrorEnvelope(t, w.ResponseRecorder, "gaigate_upstream_timeout")
				assert.Equal(t, 1, w.headers)
				assert.Equal(t, 1, w.writes)
			} else {
				assert.PanicsWithValue(t, http.ErrAbortHandler, serve)
				assert.Zero(t, w.headers)
				assert.Zero(t, w.writes)
			}
		})
	}
}

type failedChatBody struct{}

func (failedChatBody) Read([]byte) (int, error) { return 0, errors.New("private body read cause") }
func (failedChatBody) Close() error             { return nil }

func TestV1BodyReadFailureHasZeroBusinessCalls(t *testing.T) {
	calls := 0
	r := v1TestRouter(precheckFunc(func(context.Context, *Principal) error { calls++; return nil }),
		chatExecutorFunc(func(context.Context, http.ResponseWriter, *Principal, *provider.ChatRequest) error {
			calls++
			return nil
		}), modelListerFunc(noModels))
	req := v1Request("POST", "/v1/chat/completions", "")
	req.Body = failedChatBody{}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, 400, w.Code)
	checkErrorEnvelope(t, w, "gaigate_invalid_request")
	assert.Zero(t, calls)
	assert.NotContains(t, w.Body.String(), "private body read cause")
}
