package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/provider"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func basicRequest() *provider.ChatRequest {
	return &provider.ChatRequest{Model: "alias", Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"hello"}`)}}
}

func newAdapter(t *testing.T, client *http.Client) provider.Provider {
	t.Helper()
	p, err := New(client, 64)
	require.NoError(t, err)
	return p
}

func endpoint(base string) *provider.Endpoint {
	return &provider.Endpoint{BaseURL: base, APIKey: "sk-private-upstream-test", Model: "mapped-model"}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestURLAndHTTPWire(t *testing.T) {
	for _, tc := range []struct{ suffix, path string }{
		{"", "/v1/chat/completions"}, {"/", "/v1/chat/completions"},
		{"/v1", "/v1/chat/completions"}, {"/v1///", "/v1/chat/completions"},
		{"/prefix/", "/prefix/v1/chat/completions"}, {"/prefix/v1/", "/prefix/v1/chat/completions"},
		{"/pre%2Ffix/v1", "/pre%2Ffix/v1/chat/completions"},
	} {
		t.Run(tc.suffix, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, tc.path, r.URL.EscapedPath())
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "Bearer sk-private-upstream-test", r.Header.Get("Authorization"))
				assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
				assert.Empty(t, r.Header.Get("Cookie"))
				for key := range r.Header {
					assert.False(t, strings.HasPrefix(key, "X-"), "unexpected %s", key)
				}
				var body map[string]json.RawMessage
				assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.JSONEq(t, `"mapped-model"`, string(body["model"]))
				assert.JSONEq(t, `false`, string(body["stream"]))
				assert.NotContains(t, body, "stream_options")
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				_, _ = io.WriteString(w, "{}")
			}))
			defer s.Close()
			resp, usage, err := newAdapter(t, s.Client()).Chat(t.Context(), endpoint(s.URL+tc.suffix), basicRequest())
			require.NoError(t, err)
			require.Equal(t, "{}", string(resp.Body))
			require.Nil(t, usage)
		})
	}
}

func TestInvalidURLBeforeNetwork(t *testing.T) {
	var calls atomic.Int64
	p := newAdapter(t, &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected network")
	})})
	for _, base := range []string{"", "/v1", "ftp://host", "http://", "http:///path", "http://user:secret@host", "http://host?q=x", "http://host?", "http://host#x", "http://host#", "http://%", "http://host:bad", "http://host:65536", "http://host:0"} {
		_, _, err := p.Chat(t.Context(), endpoint(base), basicRequest())
		require.Error(t, err, base)
		require.NotContains(t, err.Error(), "secret")
	}
	_, _, err := p.Chat(t.Context(), nil, basicRequest())
	require.Error(t, err)
	_, _, err = p.Chat(t.Context(), endpoint("http://host"), nil)
	require.Error(t, err)
	require.Zero(t, calls.Load())
}

func TestCallerCookiesAreNotSent(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("Cookie"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer s.Close()
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	u, err := url.Parse(s.URL)
	require.NoError(t, err)
	jar.SetCookies(u, []*http.Cookie{{Name: "private", Value: "caller-cookie"}})
	client := s.Client()
	client.Jar = jar
	_, _, err = newAdapter(t, client).Chat(t.Context(), endpoint(s.URL), basicRequest())
	require.NoError(t, err)
	require.Len(t, jar.Cookies(u), 1)
}

func TestRedirectIsNotFollowed(t *testing.T) {
	var targetCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls.Add(1)
	}))
	defer target.Close()
	for _, status := range []int{301, 302, 303, 307, 308} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var attempts atomic.Int64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				http.Redirect(w, r, target.URL, status)
			}))
			defer s.Close()
			_, _, err := newAdapter(t, s.Client()).Chat(t.Context(), endpoint(s.URL), basicRequest())
			var upstream *provider.UpstreamError
			require.ErrorAs(t, err, &upstream)
			require.Equal(t, status, upstream.StatusCode)
			require.False(t, upstream.Retryable)
			require.EqualValues(t, 1, attempts.Load())
			require.Zero(t, targetCalls.Load(), "credentials never reach redirect target")
		})
	}
}

func TestChatResponseValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, media string
		valid             bool
		usage             *provider.Usage
	}{
		{"original", " \n{\"choices\":[], \"usage\":{\"prompt_tokens\":12,\"completion_tokens\":34}}\r\n", "application/json; charset=utf-8", true, &provider.Usage{InputTokens: 12, OutputTokens: 34}},
		{"missing", `{}`, "application/json", true, nil},
		{"null", `{"usage":null}`, "application/json", true, nil},
		{"zero", `{"usage":{"prompt_tokens":0,"completion_tokens":0}}`, "application/json", true, &provider.Usage{}},
		{"max", `{"usage":{"prompt_tokens":9223372036854775807,"completion_tokens":1}}`, "application/json", true, &provider.Usage{InputTokens: 9223372036854775807, OutputTokens: 1}},
		{"overflow", `{"usage":{"prompt_tokens":9223372036854775808,"completion_tokens":1}}`, "application/json", false, nil},
		{"negative", `{"usage":{"prompt_tokens":-1,"completion_tokens":1}}`, "application/json", false, nil},
		{"fraction", `{"usage":{"prompt_tokens":1.0,"completion_tokens":1}}`, "application/json", false, nil},
		{"exponent", `{"usage":{"prompt_tokens":1e0,"completion_tokens":1}}`, "application/json", false, nil},
		{"string", `{"usage":{"prompt_tokens":"1","completion_tokens":1}}`, "application/json", false, nil},
		{"null token", `{"usage":{"prompt_tokens":null,"completion_tokens":1}}`, "application/json", false, nil},
		{"missing token", `{"usage":{"prompt_tokens":1}}`, "application/json", false, nil},
		{"empty usage", `{"usage":{}}`, "application/json", false, nil},
		{"array usage", `{"usage":[]}`, "application/json", false, nil},
		{"boolean usage", `{"usage":true}`, "application/json", false, nil},
		{"empty", ``, "application/json", false, nil},
		{"root null", `null`, "application/json", false, nil},
		{"array", `[]`, "application/json", false, nil},
		{"trailing", `{} {}`, "application/json", false, nil},
		{"malformed", `{"broken"`, "application/json", false, nil},
		{"wrong media", `{}`, "text/event-stream", false, nil},
		{"missing media", `{}`, "", false, nil},
		{"bad media", `{}`, "application/json;broken", false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.media)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer s.Close()
			resp, usage, err := newAdapter(t, s.Client()).Chat(t.Context(), endpoint(s.URL), basicRequest())
			if !tc.valid {
				var upstream *provider.UpstreamError
				require.ErrorAs(t, err, &upstream)
				require.Equal(t, 200, upstream.StatusCode)
				require.False(t, upstream.Retryable)
				require.Nil(t, resp)
				require.Nil(t, usage)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.body, string(resp.Body))
			require.Equal(t, tc.usage, usage)
			if usage != nil {
				require.Same(t, usage, resp.Usage)
			}
		})
	}
}

func TestChatConcurrentReuseOverHTTP(t *testing.T) {
	req := richRequest()
	before, err := json.Marshal(req)
	require.NoError(t, err)
	expected, err := serialize(endpoint(""), req, false)
	require.NoError(t, err)
	var calls atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.JSONEq(t, string(expected), string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{}`)
	}))
	defer s.Close()
	p := newAdapter(t, s.Client())
	ep := endpoint(s.URL)
	var group errgroup.Group
	for range 24 {
		group.Go(func() error { _, _, err := p.Chat(t.Context(), ep, req); return err })
	}
	require.NoError(t, group.Wait())
	require.EqualValues(t, 24, calls.Load())
	after, err := json.Marshal(req)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after))
}

func TestChatContextCancelsHeadersAndBody(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			entered, exited := make(chan struct{}), make(chan struct{})
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(exited)
				_, _ = io.Copy(io.Discard, r.Body)
				if phase == "body" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"partial":`)
					w.(http.Flusher).Flush()
				}
				close(entered)
				<-r.Context().Done()
			}))
			defer s.Close()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := newAdapter(t, s.Client())
			var group errgroup.Group
			group.Go(func() error { _, _, err := p.Chat(ctx, endpoint(s.URL), basicRequest()); return err })
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("handler not entered")
			}
			started := time.Now()
			cancel()
			require.ErrorIs(t, group.Wait(), context.Canceled)
			select {
			case <-exited:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("upstream did not exit")
			}
			require.Less(t, time.Since(started), 500*time.Millisecond)
		})
	}
}
