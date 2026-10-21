package access

import (
	"bytes"
	"context"
	"encoding/json"
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
	"gaigate/internal/store"
)

func keyTestRouter(svc KeyService) (*gin.Engine, string) {
	raw, key, user := authFixture()
	auth := NewAuthenticator(keyLookupStub{find: func(context.Context, string) ([]store.APIKey, error) {
		return []store.APIKey{key}, nil
	}}, userLookupStub{get: func(context.Context, int64) (*store.User, error) { return &user, nil }})
	r := gin.New()
	r.Use(middleware.RequestID())
	RegisterUserKeyRoutes(r, AuthMiddleware(auth), svc)
	return r, raw
}

func keyHTTPRequest(r http.Handler, method, path, raw, mediaType string, body io.Reader) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	if raw != "" {
		req.Header.Set("Authorization", "Bearer "+raw)
	}
	if mediaType != "" {
		req.Header.Set("Content-Type", mediaType)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestUserKeyHTTPStrictInputZeroBusinessCalls(t *testing.T) {
	object := `{"name":"  原样  "}`
	for _, tc := range []struct {
		name, body, media string
		status            int
		wantName          string
	}{
		{"default", `{}`, "application/json", 201, ""},
		{"spaces retained", object, "application/json; charset=utf-8", 201, "  原样  "},
		{"ascii 128", `{"name":"` + strings.Repeat("a", 128) + `"}`, "application/json", 201, strings.Repeat("a", 128)},
		{"unicode 128", `{"name":"` + strings.Repeat("😀", 128) + `"}`, "application/json", 201, strings.Repeat("😀", 128)},
		{"exact limit", `{}` + strings.Repeat(" ", userKeyBodyLimit-2), "application/json", 201, ""},
		{"multibyte exact limit", object + strings.Repeat(" ", userKeyBodyLimit-len(object)), "application/json", 201, "  原样  "},
		{"limit plus one", `{}` + strings.Repeat(" ", userKeyBodyLimit-1), "application/json", 413, ""},
		{"multibyte plus one", object + strings.Repeat(" ", userKeyBodyLimit-len(object)+1), "application/json", 413, ""},
		{"invalid oversize", strings.Repeat("x", userKeyBodyLimit+1), "application/json", 413, ""},
		{"empty", "", "application/json", 400, ""},
		{"whitespace", "  ", "application/json", 400, ""},
		{"null", "null", "application/json", 400, ""},
		{"array", "[]", "application/json", 400, ""},
		{"scalar", `"name"`, "application/json", 400, ""},
		{"name null", `{"name": null}`, "application/json", 400, ""},
		{"number", `{"name":12}`, "application/json", 400, ""},
		{"bool", `{"name":false}`, "application/json", 400, ""},
		{"name array", `{"name":[]}`, "application/json", 400, ""},
		{"name object", `{"name":{}}`, "application/json", 400, ""},
		{"unknown", `{"extra":1}`, "application/json", 400, ""},
		{"owner injection", `{"user_id":99}`, "application/json", 400, ""},
		{"expired injection", `{"expired_at":null}`, "application/json", 400, ""},
		{"case mismatch", `{"Name":"x"}`, "application/json", 400, ""},
		{"duplicate", `{"name":"a","name":"b"}`, "application/json", 400, ""},
		{"escaped duplicate", `{"name":"a","na\u006de":"b"}`, "application/json", 400, ""},
		{"trailing object", `{} {}`, "application/json", 400, ""},
		{"trailing null", `{} null`, "application/json", 400, ""},
		{"trailing garbage", `{} x`, "application/json", 400, ""},
		{"truncated", `{"name":"x"`, "application/json", 400, ""},
		{"trailing comma", `{"name":"x",}`, "application/json", 400, ""},
		{"ascii 129", `{"name":"` + strings.Repeat("a", 129) + `"}`, "application/json", 400, ""},
		{"unicode 129", `{"name":"` + strings.Repeat("钥", 129) + `"}`, "application/json", 400, ""},
		{"invalid utf8", "{\"name\":\"" + string([]byte{0xff}) + "\"}", "application/json", 400, ""},
		{"no media", `{}`, "", 415, ""},
		{"plain text", `{}`, "text/plain", 415, ""},
		{"json suffix", `{}`, "application/problem+json", 415, ""},
		{"bad parameter", `{}`, "application/json; charset", 415, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, writes := 0, 0
			svc := NewKeyService(managedKeysStub{create: func(ctx context.Context, key *store.APIKey) error {
				writes++
				assert.Equal(t, uint64(3), key.UserID)
				assert.Equal(t, tc.wantName, key.Name)
				key.ID, key.CreatedAt = 20, time.Now().UTC()
				return nil
			}}, userLookupStub{get: func(ctx context.Context, id int64) (*store.User, error) {
				reads++
				return &store.User{ID: uint64(id), Status: store.UserStatusEnabled}, nil
			}})
			r, raw := keyTestRouter(svc)
			w := keyHTTPRequest(r, "POST", "/api/v1/user/keys", raw, tc.media, strings.NewReader(tc.body))
			assert.Equal(t, tc.status, w.Code)
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			if tc.status == 201 {
				assert.Equal(t, 1, writes)
				assert.Equal(t, 1, reads)
				assertJSONFields(t, w.Body.Bytes(), "id", "name", "key", "key_prefix", "created_at")
			} else {
				assert.Zero(t, writes)
				assert.Zero(t, reads, "input failure must not enter service")
				code := map[int]string{400: "gaigate_invalid_request", 413: "gaigate_request_too_large", 415: "gaigate_unsupported_media_type"}[tc.status]
				checkErrorEnvelope(t, w, code)
			}
		})
	}
}

func TestUserKeyHTTPIdentityBeforeInputAndNoStore(t *testing.T) {
	for _, method := range []string{"POST", "GET", "DELETE"} {
		path := "/api/v1/user/keys"
		if method == "DELETE" {
			path += "/1"
		}
		t.Run(method, func(t *testing.T) {
			// Nil repositories prove no business read or write is possible.
			r, _ := keyTestRouter(NewKeyService(nil, nil))
			w := keyHTTPRequest(r, method, path+"?user_id=3", "", "text/plain", strings.NewReader(strings.Repeat("x", userKeyBodyLimit+1)))
			assert.Equal(t, 401, w.Code)
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			checkErrorEnvelope(t, w, "gaigate_unauthorized")
			// An injected identity adapter that fails to populate standard context
			// must still be rejected, even if it puts an owner in Gin.
			r = gin.New()
			r.Use(middleware.RequestID())
			RegisterUserKeyRoutes(r, func(c *gin.Context) { c.Set("user_id", 3); c.Next() }, NewKeyService(nil, nil))
			w = keyHTTPRequest(r, method, path, "", "application/json", strings.NewReader(`{}`))
			assert.Equal(t, 401, w.Code)
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			checkErrorEnvelope(t, w, "gaigate_unauthorized")
		})
	}
	assert.Panics(t, func() { RegisterUserKeyRoutes(gin.New(), nil, NewKeyService(nil, nil)) })
}

func TestUserKeyHTTPDeleteIDValidationAndNoOwnerParameters(t *testing.T) {
	for _, id := range []string{"0", "-1", "+1", "1.5", "abc", "9223372036854775808"} {
		t.Run(id, func(t *testing.T) {
			r, raw := keyTestRouter(NewKeyService(managedKeysStub{}, nil))
			w := keyHTTPRequest(r, "DELETE", "/api/v1/user/keys/"+id, raw, "", nil)
			assert.Equal(t, 400, w.Code)
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			checkErrorEnvelope(t, w, "gaigate_invalid_request")
		})
	}
	calls := 0
	svc := NewKeyService(managedKeysStub{revoke: func(ctx context.Context, owner, id int64) error {
		calls++
		assert.Equal(t, int64(3), owner)
		assert.Equal(t, int64(9223372036854775807), id)
		return nil
	}}, enabledKeyOwner())
	r, raw := keyTestRouter(svc)
	req := httptest.NewRequest("DELETE", "/api/v1/user/keys/9223372036854775807?user_id=99", strings.NewReader(`{"user_id":99}`))
	req.Header.Set("Authorization", "Bearer "+raw)
	req.Header.Set("X-User-ID", "99")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, 204, w.Code)
	assert.Empty(t, w.Body.String())
	assert.Equal(t, 1, calls)
}

func TestUserKeyHTTPErrorsAndLogsArePrivate(t *testing.T) {
	raw, key, user := authFixture()
	secrets := []string{raw, key.KeyHash, "private-password", "private-dsn"}
	failure := fmt.Errorf("SQL: %s", strings.Join(secrets, " "))
	for _, method := range []string{"POST", "GET", "DELETE"} {
		for _, stage := range []string{"repository", "owner missing", "owner disabled", "identity revoked", "identity expired", "identity internal"} {
			t.Run(method+"/"+stage, func(t *testing.T) {
				var logs bytes.Buffer
				logger := capturingLogger(&logs)
				previous := slog.Default()
				slog.SetDefault(logger)
				defer slog.SetDefault(previous)
				calls := 0
				svc := NewKeyService(managedKeysStub{
					create: func(context.Context, *store.APIKey) error { calls++; return failure },
					list:   func(context.Context, int64) ([]store.APIKey, error) { calls++; return nil, failure },
					revoke: func(context.Context, int64, int64) error { calls++; return failure },
				}, userLookupStub{get: func(context.Context, int64) (*store.User, error) {
					if stage == "owner missing" {
						return nil, store.ErrNotFound
					}
					u := user
					if stage == "owner disabled" {
						u.Status = store.UserStatusDisabled
					}
					return &u, nil
				}})
				r := gin.New()
				r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())
				identity := AuthMiddleware(authenticatorFunc(func(context.Context, string) (*Principal, error) {
					switch stage {
					case "identity revoked":
						return nil, ErrKeyRevoked
					case "identity expired":
						return nil, ErrKeyExpired
					case "identity internal":
						return nil, failure
					}
					return &Principal{UserID: 3, APIKeyID: 2, GroupID: 4}, nil
				}))
				RegisterUserKeyRoutes(r, identity, svc)
				path := "/api/v1/user/keys"
				if method == "DELETE" {
					path += "/1"
				}
				w := keyHTTPRequest(r, method, path, raw, "application/json", strings.NewReader(`{}`))
				status, code := 500, "gaigate_internal"
				switch stage {
				case "owner disabled":
					status, code = 403, "gaigate_user_disabled"
				case "identity revoked":
					status, code = 403, "gaigate_key_revoked"
				case "identity expired":
					status, code = 403, "gaigate_key_expired"
				}
				assert.Equal(t, status, w.Code)
				assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
				checkErrorEnvelope(t, w, code)
				if stage != "repository" {
					assert.Zero(t, calls)
				}
				for _, secret := range secrets {
					assert.NotContains(t, w.Body.String(), secret)
					assert.NotContains(t, logs.String(), secret)
				}
			})
		}
	}
}

func TestUserKeyHTTPEmptyList(t *testing.T) {
	svc := NewKeyService(managedKeysStub{list: func(context.Context, int64) ([]store.APIKey, error) { return nil, nil }}, enabledKeyOwner())
	r, raw := keyTestRouter(svc)
	w := keyHTTPRequest(r, "GET", "/api/v1/user/keys?user_id=99", raw, "", nil)
	assert.Equal(t, 200, w.Code)
	assert.Equal(t, "[]", w.Body.String())
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	var views []KeyView
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &views))
	assert.NotNil(t, views)
}

type countedKeyBody struct {
	remaining int
	read      int
}

func (b *countedKeyBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	n := min(len(p), b.remaining)
	for i := range p[:n] {
		p[i] = ' '
	}
	b.remaining -= n
	b.read += n
	return n, nil
}

func TestUserKeyHTTPBodyReadIsBounded(t *testing.T) {
	r, raw := keyTestRouter(NewKeyService(nil, nil))
	body := &countedKeyBody{remaining: 10 * userKeyBodyLimit}
	w := keyHTTPRequest(r, "POST", "/api/v1/user/keys", raw, "application/json", body)
	assert.Equal(t, 413, w.Code)
	assert.Equal(t, userKeyBodyLimit+1, body.read, "reader must stop after the single excess byte")
}

func TestUserKeyHTTPEntropyFailureHasNoPartialCredential(t *testing.T) {
	writes := 0
	svc := NewKeyService(managedKeysStub{create: func(context.Context, *store.APIKey) error {
		writes++
		return nil
	}}, enabledKeyOwner()).(*keyService)
	svc.generate = func() (string, string, string, error) { return generateKey(strings.NewReader("short")) }
	r, raw := keyTestRouter(svc)
	w := keyHTTPRequest(r, "POST", "/api/v1/user/keys", raw, "application/json", strings.NewReader(`{}`))
	assert.Equal(t, 500, w.Code)
	checkErrorEnvelope(t, w, "gaigate_internal")
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
	assertJSONFields(t, w.Body.Bytes(), "error", "request_id")
	assert.NotContains(t, w.Body.String(), "sk-")
	assert.Zero(t, writes)
}
