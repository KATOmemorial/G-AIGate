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

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

func init() { gin.SetMode(gin.TestMode) }

type authenticatorFunc func(context.Context, string) (*Principal, error)

func (f authenticatorFunc) Authenticate(ctx context.Context, key string) (*Principal, error) {
	return f(ctx, key)
}

type responseCounter struct {
	*httptest.ResponseRecorder
	headers, writes int
}

func (w *responseCounter) WriteHeader(status int) {
	w.headers++
	w.ResponseRecorder.WriteHeader(status)
}
func (w *responseCounter) Write(p []byte) (int, error) {
	w.writes++
	return w.ResponseRecorder.Write(p)
}

func capturingLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) == 0 && a.Key == slog.TimeKey {
			a.Key = "ts"
		}
		return a
	}}))
}

func TestAuthMiddlewareRejectsMalformedHeadersWithoutRepositories(t *testing.T) {
	raw, _, _ := authFixture()
	for _, tc := range []struct {
		name   string
		header http.Header
	}{
		{"missing", http.Header{}},
		{"empty", http.Header{"Authorization": {""}}},
		{"no values", http.Header{"Authorization": nil}},
		{"wrong scheme", http.Header{"Authorization": {"Basic " + raw}}},
		{"scheme only", http.Header{"Authorization": {"Bearer"}}},
		{"missing separator", http.Header{"Authorization": {"Bearer" + raw}}},
		{"two keys", http.Header{"Authorization": {"Bearer " + raw + " " + raw}}},
		{"repeated", http.Header{"Authorization": {"Bearer " + raw, "Bearer " + raw}}},
		{"empty extra", http.Header{"Authorization": {"Bearer " + raw, ""}}},
		{"case variant duplicate", http.Header{"Authorization": {"Bearer " + raw}, "authorization": {"Bearer " + raw}}},
		{"comma schemes", http.Header{"Authorization": {"Bearer " + raw + ", Bearer " + raw}}},
		{"comma key", http.Header{"Authorization": {"Bearer " + raw[:45] + ","}}},
		{"short key", http.Header{"Authorization": {"Bearer " + raw[:45]}}},
		{"long key", http.Header{"Authorization": {"Bearer " + raw + "A"}}},
		{"wrong prefix", http.Header{"Authorization": {"Bearer xx-" + raw[3:]}}},
		{"invalid alphabet", http.Header{"Authorization": {"Bearer " + raw[:45] + "+"}}},
		{"padded key", http.Header{"Authorization": {"Bearer " + raw[:45] + "="}}},
		{"invalid base64 bits", http.Header{"Authorization": {"Bearer " + raw[:45] + "B"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			a := NewAuthenticator(keyLookupStub{find: func(context.Context, string) ([]store.APIKey, error) { calls++; return nil, nil }},
				userLookupStub{get: func(context.Context, int64) (*store.User, error) { calls++; return nil, nil }})
			authCalls, handlerCalls := 0, 0
			r := gin.New()
			r.Use(middleware.RequestID(), AuthMiddleware(authenticatorFunc(func(ctx context.Context, raw string) (*Principal, error) {
				authCalls++
				return a.Authenticate(ctx, raw)
			})))
			r.GET("/protected", func(c *gin.Context) { handlerCalls++; c.Status(204) })
			req := httptest.NewRequest("GET", "/protected", nil)
			req.Header = tc.header
			w := &responseCounter{ResponseRecorder: httptest.NewRecorder()}
			r.ServeHTTP(w, req)
			assert.Equal(t, 401, w.Code)
			assert.Zero(t, calls)
			assert.Zero(t, authCalls, "adapter must reject before calling even an injected authenticator")
			assert.Zero(t, handlerCalls)
			assert.Equal(t, 1, w.headers)
			assert.Equal(t, 1, w.writes)
			checkErrorEnvelope(t, w.ResponseRecorder, "gaigate_unauthorized")
		})
	}
}

func checkErrorEnvelope(t *testing.T, w *httptest.ResponseRecorder, code string) {
	t.Helper()
	var body struct {
		Error     struct{ Message, Type, Code string } `json:"error"`
		RequestID string                               `json:"request_id"`
	}
	decoder := json.NewDecoder(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, decoder.Decode(&body))
	assert.Equal(t, "gaigate_error", body.Error.Type)
	assert.Equal(t, code, body.Error.Code)
	assert.NotEmpty(t, body.Error.Message)
	assert.NotEmpty(t, body.RequestID)
	assert.Equal(t, w.Header().Get("X-Request-Id"), body.RequestID)
	var extra any
	assert.ErrorIs(t, decoder.Decode(&extra), io.EOF, "exactly one JSON envelope")
}

func TestAuthMiddlewarePipelineAndPrivacy(t *testing.T) {
	raw, key, user := authFixture()
	user.PasswordHash = "private-password-fingerprint"
	sensitive := []string{raw, key.KeyHash, user.PasswordHash, "plain-password", "provider-secret", "private-dsn"}
	failure := fmt.Errorf("SQL failed with %s", strings.Join(sensitive, " "))
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"success", nil, 200, ""},
		{"unauthorized", fmt.Errorf("wrapped: %w", ErrUnauthorized), 401, "gaigate_unauthorized"},
		{"revoked", fmt.Errorf("wrapped: %w", ErrKeyRevoked), 403, "gaigate_key_revoked"},
		{"expired", ErrKeyExpired, 403, "gaigate_key_expired"},
		{"disabled", ErrUserDisabled, 403, "gaigate_user_disabled"},
		{"internal", failure, 500, "gaigate_internal"},
		{"orphaned", fmt.Errorf("missing owner: %w", store.ErrNotFound), 500, "gaigate_internal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			logger := capturingLogger(&logs)
			previous := slog.Default()
			slog.SetDefault(logger)
			defer slog.SetDefault(previous)
			expected := Principal{UserID: 3, APIKeyID: 2, GroupID: 4, Username: user.Username, Role: user.Role}
			calls, handlerCalls := 0, 0
			type marker struct{}
			requestContext := context.WithValue(context.Background(), marker{}, "context-preserved")
			r := gin.New()
			r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())
			group := r.Group("/protected", AuthMiddleware(authenticatorFunc(func(ctx context.Context, value string) (*Principal, error) {
				calls++
				assert.Equal(t, raw, value)
				assert.Equal(t, "context-preserved", ctx.Value(marker{}))
				_, ok := middleware.RequestIDFromContext(ctx)
				assert.True(t, ok)
				return &expected, tc.err
			})))
			group.GET("", func(c *gin.Context) {
				handlerCalls++
				p, ok := PrincipalFromContext(c.Request.Context())
				require.True(t, ok)
				assert.Equal(t, expected, *p)
				assert.Equal(t, p.UserID, middleware.UserIDFromContext(c.Request.Context()))
				assert.Equal(t, "context-preserved", c.Request.Context().Value(marker{}))
				c.JSON(200, gin.H{"ok": true})
			})
			req := httptest.NewRequest("GET", "/protected", nil).WithContext(requestContext)
			req.Header.Set("Authorization", " \t bEaReR\t"+raw+" \t")
			w := &responseCounter{ResponseRecorder: httptest.NewRecorder()}
			r.ServeHTTP(w, req)
			assert.Equal(t, 1, calls)
			assert.Equal(t, tc.status, w.Code)
			assert.Equal(t, 1, w.headers)
			assert.Equal(t, 1, w.writes)
			if tc.err != nil {
				assert.Zero(t, handlerCalls)
				checkErrorEnvelope(t, w.ResponseRecorder, tc.code)
			} else {
				assert.Equal(t, 1, handlerCalls)
			}
			var entries []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
				var entry map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &entry))
				assert.Equal(t, w.Header().Get("X-Request-Id"), entry["request_id"])
				if entry["msg"] == "request_done" {
					entries = append(entries, entry)
				}
			}
			require.Len(t, entries, 1)
			entry := entries[0]
			assert.Contains(t, entry, "ts")
			assert.NotContains(t, entry, "time")
			assert.EqualValues(t, tc.status, entry["status_code"])
			assert.Equal(t, fmt.Sprintf("%dxx", tc.status/100), entry["status_class"])
			if tc.err == nil {
				assert.EqualValues(t, expected.UserID, entry["user_id"])
			} else {
				assert.EqualValues(t, 0, entry["user_id"])
			}
			for _, secret := range sensitive {
				assert.NotContains(t, logs.String(), secret)
				assert.NotContains(t, w.Body.String(), secret)
			}
		})
	}
}

func TestAuthMiddlewareInvalidAuthenticatorResult(t *testing.T) {
	raw, _, _ := authFixture()
	for _, a := range []Authenticator{nil,
		authenticatorFunc(func(context.Context, string) (*Principal, error) { return nil, nil }),
		authenticatorFunc(func(context.Context, string) (*Principal, error) { return &Principal{}, nil }),
	} {
		r := gin.New()
		r.Use(middleware.RequestID(), AuthMiddleware(a))
		r.GET("/protected", func(c *gin.Context) { t.Error("invalid identity reached handler") })
		req := httptest.NewRequest("GET", "/protected", nil)
		req.Header.Set("Authorization", "Bearer "+raw)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		assert.Equal(t, 500, w.Code)
		checkErrorEnvelope(t, w, "gaigate_internal")
	}
}

func TestAuthenticatorCancellationCause(t *testing.T) {
	raw, _, _ := authFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	a := NewAuthenticator(keyLookupStub{find: func(got context.Context, _ string) ([]store.APIKey, error) { return nil, got.Err() }}, nil)
	p, err := a.Authenticate(ctx, raw)
	assert.Nil(t, p)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, errors.Is(err, ErrUnauthorized))
}
