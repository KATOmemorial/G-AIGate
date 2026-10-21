package access

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	mysqlerr "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

func TestUserKeysMySQLHTTPLifecycle(t *testing.T) {
	db := authTestDB(t)
	owner := authTestOwner(t, db)
	other := owner
	other.ID, other.Username = 0, "other-owner"
	require.NoError(t, db.Create(&other).Error)
	keys, users := store.NewAPIKeyRepo(db), store.NewUserRepo(db)
	primaryRaw, primary := generatedTestKey(t, keys, owner.ID)
	backupRaw, _ := generatedTestKey(t, keys, owner.ID)
	otherRaw, otherKey := generatedTestKey(t, keys, other.ID)
	_, expiredKey := generatedTestKey(t, keys, owner.ID)
	expiredAt := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	require.NoError(t, db.Model(&store.APIKey{}).Where("id = ?", expiredKey.ID).Update("expired_at", expiredAt).Error)
	auth := NewAuthenticator(keys, users)
	svc := NewKeyService(keys, users)
	var logs bytes.Buffer
	logger := capturingLogger(&logs)
	previous := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(previous)
	r := gin.New()
	r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover())
	RegisterUserKeyRoutes(r, AuthMiddleware(auth), svc)
	server := httptest.NewServer(r)
	defer server.Close()
	client := server.Client()
	// Real loopback HTTP; success bodies stay in memory and never enter logs.
	request := func(method, path, raw, body string, status int, code string) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), method, server.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		if raw != "" {
			req.Header.Set("Authorization", "Bearer "+raw)
		}
		if method == "POST" {
			req.Header.Set("Content-Type", "application/json")
		}
		res, err := client.Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, status, res.StatusCode)
		assert.Equal(t, "no-store", res.Header.Get("Cache-Control"))
		if code != "" {
			w := httptest.NewRecorder()
			w.Header().Set("X-Request-Id", res.Header.Get("X-Request-Id"))
			_, err = w.Body.Write(data)
			require.NoError(t, err)
			checkErrorEnvelope(t, w, code)
		}
		if status == 204 {
			assert.Empty(t, data)
		}
		if status != 201 {
			for _, secret := range []string{primaryRaw, backupRaw, otherRaw, primary.KeyHash, otherKey.KeyHash, owner.PasswordHash} {
				assert.NotContains(t, string(data), secret)
			}
		}
		return data
	}
	path := "/api/v1/user/keys"
	request("POST", path, "", `{}`, 401, "gaigate_unauthorized")
	createdBody := request("POST", path, primaryRaw, `{"name":"  工作😀  "}`, 201, "")
	assertJSONFields(t, createdBody, "id", "name", "key", "key_prefix", "created_at")
	var created CreatedKey
	require.NoError(t, json.Unmarshal(createdBody, &created))
	assert.Positive(t, created.ID)
	assert.Equal(t, "  工作😀  ", created.Name)
	assert.True(t, validRawKey(created.Key))
	assert.Equal(t, time.UTC, created.CreatedAt.Location())
	var stored store.APIKey
	require.NoError(t, db.First(&stored, created.ID).Error)
	digest := sha256.Sum256([]byte(created.Key))
	assert.True(t, stored.KeyHash == hex.EncodeToString(digest[:]))
	assert.Equal(t, created.Key[:8], stored.KeyPrefix)
	assert.Equal(t, owner.ID, stored.UserID)
	assert.Nil(t, stored.ExpiredAt)
	assert.WithinDuration(t, stored.CreatedAt, created.CreatedAt, time.Millisecond)
	p, err := auth.Authenticate(context.Background(), created.Key)
	require.NoError(t, err)
	assert.Equal(t, int64(owner.ID), p.UserID)
	assert.Equal(t, created.ID, p.APIKeyID)

	listBody := request("GET", path, primaryRaw, "", 200, "")
	var views []KeyView
	require.NoError(t, json.Unmarshal(listBody, &views))
	require.Len(t, views, 4)
	var last int64
	foundExpired := false
	for _, view := range views {
		assert.Greater(t, view.ID, last)
		last = view.ID
		assert.NotEqual(t, int64(otherKey.ID), view.ID)
		data, err := json.Marshal(view)
		require.NoError(t, err)
		assertJSONFields(t, data, "id", "name", "key_prefix", "status", "created_at", "expired_at")
		assert.Equal(t, time.UTC, view.CreatedAt.Location())
		if view.ID == int64(expiredKey.ID) {
			foundExpired = true
			require.NotNil(t, view.ExpiredAt)
			assert.True(t, expiredAt.Equal(*view.ExpiredAt))
		} else {
			assert.Nil(t, view.ExpiredAt)
		}
	}
	assert.True(t, foundExpired)
	assert.NotContains(t, string(listBody), created.Key)
	assert.NotContains(t, string(listBody), stored.KeyHash)
	foreign := request("DELETE", fmt.Sprintf("%s/%d", path, otherKey.ID), primaryRaw, "", 404, "gaigate_not_found")
	missing := request("DELETE", path+"/99999999", primaryRaw, "", 404, "gaigate_not_found")
	var foreignEnvelope, missingEnvelope map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(foreign, &foreignEnvelope))
	require.NoError(t, json.Unmarshal(missing, &missingEnvelope))
	assert.JSONEq(t, string(foreignEnvelope["error"]), string(missingEnvelope["error"]))
	_, err = auth.Authenticate(context.Background(), otherRaw)
	require.NoError(t, err, "foreign revocation must leave B usable")
	request("DELETE", fmt.Sprintf("%s/%d", path, created.ID), backupRaw, "", 204, "")
	request("DELETE", fmt.Sprintf("%s/%d", path, created.ID), backupRaw, "", 204, "")
	_, err = auth.Authenticate(context.Background(), created.Key)
	assert.ErrorIs(t, err, ErrKeyRevoked)
	request("GET", path, created.Key, "", 403, "gaigate_key_revoked")
	request("DELETE", fmt.Sprintf("%s/%d", path, primary.ID), primaryRaw, "", 204, "")
	request("GET", path, primaryRaw, "", 403, "gaigate_key_revoked")
	listBody = request("GET", path, backupRaw, "", 200, "")
	require.NoError(t, json.Unmarshal(listBody, &views))
	require.Len(t, views, 4, "revoked and expired rows remain listed")
	for _, view := range views {
		if view.ID == created.ID || view.ID == int64(primary.ID) {
			assert.Equal(t, store.APIKeyStatusRevoked, view.Status)
		}
	}
	for _, secret := range []string{created.Key, stored.KeyHash, primaryRaw, backupRaw, otherRaw, owner.PasswordHash} {
		assert.NotContains(t, logs.String(), secret)
	}
}

func TestUserKeysMySQLInvalidRequestsDoNotWrite(t *testing.T) {
	db := authTestDB(t)
	user := authTestOwner(t, db)
	keys, users := store.NewAPIKeyRepo(db), store.NewUserRepo(db)
	raw, _ := generatedTestKey(t, keys, user.ID)
	var before, after []store.APIKey
	require.NoError(t, db.Order("id").Find(&before).Error)
	var writes atomic.Int64
	observe := func(*gorm.DB) { writes.Add(1) }
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("134:observe_create", observe))
	require.NoError(t, db.Callback().Update().Before("gorm:update").Register("134:observe_update", observe))
	require.NoError(t, db.Callback().Delete().Before("gorm:delete").Register("134:observe_delete", observe))
	r := gin.New()
	r.Use(middleware.RequestID())
	RegisterUserKeyRoutes(r, AuthMiddleware(NewAuthenticator(keys, users)), NewKeyService(keys, users))
	for _, tc := range []struct{ method, suffix, body, credential string }{
		{"POST", "", `{}`, ""},
		{"POST", "", `{"user_id":9}`, raw},
		{"POST", "", `{"name":null}`, raw},
		{"POST", "", `{"name":"a","name":"b"}`, raw},
		{"POST", "", `{"name":"` + strings.Repeat("钥", 129) + `"}`, raw},
		{"POST", "", `{}` + strings.Repeat(" ", userKeyBodyLimit), raw},
		{"DELETE", "/-1", "", raw},
	} {
		w := keyHTTPRequest(r, tc.method, "/api/v1/user/keys"+tc.suffix, tc.credential, "application/json", strings.NewReader(tc.body))
		assert.GreaterOrEqual(t, w.Code, 400)
	}
	assert.Zero(t, writes.Load(), "GORM write callbacks must never be entered")
	require.NoError(t, db.Order("id").Find(&after).Error)
	assert.Equal(t, before, after, "entire metadata, digest and status must remain unchanged")
}

func TestUserKeysMySQLServiceFaultsAndCancellation(t *testing.T) {
	db := authTestDB(t)
	user := authTestOwner(t, db)
	keys, users := store.NewAPIKeyRepo(db), store.NewUserRepo(db)
	_, key := generatedTestKey(t, keys, user.ID)
	svc := NewKeyService(keys, users)
	ctx := WithPrincipal(context.Background(), &Principal{UserID: int64(user.ID), APIKeyID: 0})
	pool, err := db.DB()
	require.NoError(t, err)
	lock, err := pool.Conn(context.Background())
	require.NoError(t, err)
	defer lock.Close()
	_, err = lock.ExecContext(context.Background(), "LOCK TABLES api_keys WRITE")
	require.NoError(t, err)
	func() {
		defer func() { _, err := lock.ExecContext(context.Background(), "UNLOCK TABLES"); require.NoError(t, err) }()
		for _, operation := range []string{"create", "list", "revoke"} {
			t.Run(operation, func(t *testing.T) {
				bounded, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				defer cancel()
				start := time.Now()
				var err error
				switch operation {
				case "create":
					var created *CreatedKey
					created, err = svc.Create(bounded, "canceled")
					assert.Nil(t, created)
				case "list":
					var views []KeyView
					views, err = svc.List(bounded)
					assert.Nil(t, views)
				case "revoke":
					err = svc.Revoke(bounded, int64(key.ID))
				}
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
				assert.Less(t, time.Since(start), 3*time.Second)
			})
		}
	}()
	rows, err := keys.ListByUser(ctx, int64(user.ID))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, store.APIKeyStatusActive, rows[0].Status)
	// Recheck mutable user state even for the trusted local Principal.
	require.NoError(t, db.Model(&store.User{}).Where("id = ?", user.ID).Update("status", store.UserStatusDisabled).Error)
	created, err := svc.Create(ctx, "disabled")
	assert.Nil(t, created)
	assert.ErrorIs(t, err, ErrUserDisabled)
	assert.ErrorIs(t, svc.Revoke(ctx, int64(key.ID)), ErrUserDisabled)
	require.NoError(t, db.Model(&store.User{}).Where("id = ?", user.ID).Update("status", store.UserStatusEnabled).Error)
	require.NoError(t, db.Exec("DROP TABLE api_keys").Error)
	created, err = svc.Create(ctx, "failure")
	assert.Nil(t, created)
	var sqlErr *mysqlerr.MySQLError
	require.ErrorAs(t, err, &sqlErr)
	assert.Equal(t, uint16(1146), sqlErr.Number)
	assert.NotErrorIs(t, err, ErrUnauthorized)
}
