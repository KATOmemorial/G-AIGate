package access

import (
	"bytes"
	"context"
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
	"gaigate/internal/store"
)

func TestV1MySQLIdentityAndModelDiscovery(t *testing.T) {
	db := authTestDB(t)
	user := authTestOwner(t, db)
	keys := store.NewAPIKeyRepo(db)
	raw, key := generatedTestKey(t, keys, user.ID)
	models := store.NewModelRepo(db, store.NewConfigCache(time.Minute))
	for _, name := range []string{"z-configured", "a-configured"} {
		require.NoError(t, models.Create(context.Background(), &store.ModelAlias{Name: name}))
	}
	var logs bytes.Buffer
	logger := capturingLogger(&logs)
	old := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(old)
	pre, exec := 0, 0
	r := gin.New()
	r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())
	RegisterV1Routes(r, AuthMiddleware(NewAuthenticator(keys, store.NewUserRepo(db))),
		precheckFunc(func(ctx context.Context, p *Principal) error {
			pre++
			assert.Equal(t, "alias", middleware.ModelFromContext(ctx))
			assert.EqualValues(t, user.ID, p.UserID)
			return nil
		}), chatExecutorFunc(func(ctx context.Context, w http.ResponseWriter, p *Principal, req *provider.ChatRequest) error {
			exec++
			assert.EqualValues(t, key.ID, p.APIKeyID)
			w.Header().Set("Content-Type", "application/json")
			_, err := io.WriteString(w, `{"choices":[],"extension":"unchanged"}`)
			return err
		}), models)
	request := func(method, path, body, credential string, status int, code string) *httptest.ResponseRecorder {
		t.Helper()
		logs.Reset()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if credential != "" {
			req.Header.Set("Authorization", "Bearer "+credential)
		}
		r.ServeHTTP(w, req)
		assert.Equal(t, status, w.Code)
		if code != "" {
			checkErrorEnvelope(t, w, code)
		}
		assert.Equal(t, 1, strings.Count(logs.String(), `"msg":"request_done"`))
		assert.Contains(t, logs.String(), w.Header().Get("X-Request-Id"))
		for _, secret := range []string{raw, key.KeyHash, user.PasswordHash} {
			assert.NotContains(t, w.Body.String(), secret)
			assert.NotContains(t, logs.String(), secret)
		}
		return w
	}
	for _, path := range []string{"/v1/models", "/v1/chat/completions"} {
		method := "GET"
		if path != "/v1/models" {
			method = "POST"
		}
		request(method, path, validChatJSON, "", 401, "gaigate_unauthorized")
		request(method, path, validChatJSON, "bad-key", 401, "gaigate_unauthorized")
	}
	w := request("GET", "/v1/models", "invalid JSON ignored", raw, 200, "")
	assert.JSONEq(t, `{"object":"list","data":[{"id":"a-configured","object":"model","owned_by":"gaigate"},{"id":"z-configured","object":"model","owned_by":"gaigate"}]}`, w.Body.String())
	assert.Zero(t, pre)
	assert.Zero(t, exec)
	request("POST", "/v1/chat/completions", `{"model":"must-not-be-logged"}`, raw, 400, "gaigate_invalid_request")
	assert.NotContains(t, logs.String(), "must-not-be-logged")
	assert.Zero(t, pre)
	assert.Zero(t, exec)
	w = request("POST", "/v1/chat/completions", validChatJSON, raw, 200, "")
	assert.Equal(t, `{"choices":[],"extension":"unchanged"}`, w.Body.String())
	assert.Contains(t, logs.String(), `"model":"alias"`)
	assert.Contains(t, logs.String(), `"user_id":3`)
	assert.Equal(t, 1, pre)
	assert.Equal(t, 1, exec)

	expired := time.Now().UTC().Add(-time.Hour)
	require.NoError(t, db.Model(&store.APIKey{}).Where("id = ?", key.ID).Update("expired_at", expired).Error)
	request("POST", "/v1/chat/completions", validChatJSON, raw, 403, "gaigate_key_expired")
	require.NoError(t, db.Model(&store.APIKey{}).Where("id = ?", key.ID).Update("expired_at", nil).Error)
	require.NoError(t, db.Model(&store.User{}).Where("id = ?", user.ID).Update("status", store.UserStatusDisabled).Error)
	request("GET", "/v1/models", "", raw, 403, "gaigate_user_disabled")
	require.NoError(t, db.Model(&store.User{}).Where("id = ?", user.ID).Update("status", store.UserStatusEnabled).Error)
	require.NoError(t, keys.RevokeOwned(context.Background(), int64(user.ID), int64(key.ID)))
	request("POST", "/v1/chat/completions", validChatJSON, raw, 403, "gaigate_key_revoked")
	request("GET", "/v1/models", "", raw, 403, "gaigate_key_revoked")
	assert.Equal(t, 1, pre)
	assert.Equal(t, 1, exec)

	raw, key = generatedTestKey(t, keys, user.ID)
	require.NoError(t, db.Exec("DROP TABLE model_aliases").Error)
	request("GET", "/v1/models", "", raw, 500, "gaigate_internal")
	require.NoError(t, db.Exec("DROP TABLE api_keys").Error)
	request("POST", "/v1/chat/completions", validChatJSON, raw, 500, "gaigate_internal")
	assert.Equal(t, 1, pre)
	assert.Equal(t, 1, exec)
}
