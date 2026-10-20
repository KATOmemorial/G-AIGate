package access

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

// Follow the store fixture's GAIGATE_TEST_MYSQL_* convention, with a fixed
// database name. These tests fail (never skip) if the required SQL is unavailable.
func authTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	port := 3307
	if value := os.Getenv("GAIGATE_TEST_MYSQL_PORT"); value != "" {
		var err error
		port, err = strconv.Atoi(value)
		require.NoError(t, err)
	}
	host, password := os.Getenv("GAIGATE_TEST_MYSQL_HOST"), os.Getenv("GAIGATE_TEST_MYSQL_PASSWORD")
	if host == "" {
		host = "127.0.0.1"
	}
	if password == "" {
		password = "gaigate_dev"
	} // Existing store fixture development default.
	opts := store.Options{Host: host, Port: port, User: "root", Password: password, Database: "gaigate_test"}
	// Connect without selecting a database so the fixture also works before first creation.
	adminOpts := opts
	adminOpts.Database = ""
	admin, err := sql.Open("mysql", adminOpts.DSN())
	require.NoError(t, err)
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, admin.PingContext(ctx), "real MySQL is required")
	_, err = admin.ExecContext(ctx, "DROP DATABASE IF EXISTS gaigate_test")
	require.NoError(t, err)
	_, err = admin.ExecContext(ctx, "CREATE DATABASE gaigate_test")
	require.NoError(t, err)
	require.NoError(t, store.RunMigrations(opts, "../../migrations"))
	db, err := store.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn, err := db.DB()
		if err == nil {
			require.NoError(t, conn.Close())
		}
	})
	return db
}

func authTestOwner(t *testing.T, db *gorm.DB) store.User {
	t.Helper()
	_, _, user := authFixture()
	user.PasswordHash = "private-owner-password-hash"
	user.CreatedAt, user.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	require.NoError(t, db.Create(&store.Group{ID: user.GroupID, Name: "auth-test", CreatedAt: time.Now().UTC()}).Error)
	require.NoError(t, db.Create(&user).Error)
	return user
}

func generatedTestKey(t *testing.T, repo store.APIKeyRepo, owner uint64) (string, store.APIKey) {
	t.Helper()
	raw, prefix, hash, err := GenerateKey()
	require.NoError(t, err)
	key := store.APIKey{UserID: owner, KeyPrefix: prefix, KeyHash: hash}
	require.NoError(t, repo.Create(context.Background(), &key))
	return raw, key
}

func TestAuthMySQLLifecycleAndPipeline(t *testing.T) {
	db := authTestDB(t)
	user := authTestOwner(t, db)
	keys, users := store.NewAPIKeyRepo(db), store.NewUserRepo(db)
	raw, prefix, hash, err := GenerateKey()
	require.NoError(t, err)
	entropy, err := base64.RawURLEncoding.DecodeString(raw[3:])
	require.NoError(t, err)
	entropy[len(entropy)-1] ^= 1
	collidingRaw := "sk-" + base64.RawURLEncoding.EncodeToString(entropy)
	require.Equal(t, prefix, collidingRaw[:8])
	digest := sha256.Sum256([]byte(collidingRaw))
	first := store.APIKey{UserID: user.ID, KeyPrefix: prefix, KeyHash: hex.EncodeToString(digest[:])}
	require.NoError(t, keys.Create(context.Background(), &first))
	require.NoError(t, keys.RevokeOwned(context.Background(), int64(user.ID), int64(first.ID)))
	key := store.APIKey{UserID: user.ID, KeyPrefix: prefix, KeyHash: hash}
	require.NoError(t, keys.Create(context.Background(), &key))
	assert.Less(t, first.ID, key.ID)
	a := NewAuthenticator(keys, users)
	p, err := a.Authenticate(context.Background(), raw)
	require.NoError(t, err, "must match a later candidate even though the first is revoked")
	assert.Equal(t, &Principal{UserID: int64(user.ID), APIKeyID: int64(key.ID), GroupID: int64(user.GroupID), Username: user.Username, Role: user.Role}, p)

	var logs bytes.Buffer
	logger := capturingLogger(&logs)
	previous := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(previous)
	r := gin.New()
	r.Use(middleware.RequestID(), middleware.AccessLog(logger), middleware.Recover(), AuthMiddleware(a))
	r.GET("/protected", func(c *gin.Context) {
		got, ok := PrincipalFromContext(c.Request.Context())
		require.True(t, ok)
		assert.Equal(t, p, got)
		c.JSON(200, gin.H{"ok": true})
	})
	request := func(status int, code string) {
		t.Helper()
		logs.Reset()
		w := &responseCounter{ResponseRecorder: httptest.NewRecorder()}
		req := httptest.NewRequest("GET", "/protected", nil)
		req.Header.Set("Authorization", "Bearer "+raw)
		r.ServeHTTP(w, req)
		assert.Equal(t, status, w.Code)
		assert.Equal(t, 1, w.headers)
		assert.Equal(t, 1, w.writes)
		if code != "" {
			checkErrorEnvelope(t, w.ResponseRecorder, code)
		}
		var summary map[string]any
		for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
			var entry map[string]any
			require.NoError(t, json.Unmarshal([]byte(line), &entry))
			if entry["msg"] == "request_done" {
				require.Nil(t, summary)
				summary = entry
			}
		}
		require.NotNil(t, summary)
		assert.Equal(t, w.Header().Get("X-Request-Id"), summary["request_id"])
		assert.EqualValues(t, status, summary["status_code"])
		if status == 200 {
			assert.EqualValues(t, user.ID, summary["user_id"])
		} else {
			assert.EqualValues(t, 0, summary["user_id"])
		}
		for _, secret := range []string{raw, hash, key.KeyHash, collidingRaw, first.KeyHash, user.PasswordHash} {
			assert.NotContains(t, logs.String(), secret)
			assert.NotContains(t, w.Body.String(), secret)
		}
	}
	request(200, "")
	// A shape-valid credential with this prefix but an unmatched digest is 401.
	entropy[len(entropy)-2] ^= 1
	unmatched := "sk-" + base64.RawURLEncoding.EncodeToString(entropy)
	_, err = a.Authenticate(context.Background(), unmatched)
	assert.ErrorIs(t, err, ErrUnauthorized)
	originalRaw := raw
	raw = unmatched
	request(401, "gaigate_unauthorized")
	raw = originalRaw
	now := time.Now().UTC().Truncate(time.Millisecond)
	a.(*authenticator).now = func() time.Time { return now }
	for _, tc := range []struct {
		expiry time.Time
		status int
		code   string
	}{
		{now.Add(time.Millisecond), 200, ""},
		{now, 403, "gaigate_key_expired"},
		{now.Add(-time.Millisecond), 403, "gaigate_key_expired"},
	} {
		require.NoError(t, db.Model(&store.APIKey{}).Where("id = ?", key.ID).Update("expired_at", tc.expiry).Error)
		request(tc.status, tc.code)
	}
	require.NoError(t, db.Model(&store.APIKey{}).Where("id = ?", key.ID).Update("expired_at", nil).Error)
	require.NoError(t, db.Model(&store.User{}).Where("id = ?", user.ID).Update("status", store.UserStatusDisabled).Error)
	request(403, "gaigate_user_disabled")
	require.NoError(t, db.Model(&store.User{}).Where("id = ?", user.ID).Update("status", store.UserStatusEnabled).Error)
	request(200, "")
	require.NoError(t, keys.RevokeOwned(context.Background(), int64(user.ID), int64(key.ID)))
	require.NoError(t, keys.RevokeOwned(context.Background(), int64(user.ID), int64(key.ID)))
	request(403, "gaigate_key_revoked")
	assert.ErrorIs(t, keys.RevokeOwned(context.Background(), int64(user.ID)+1, int64(key.ID)), store.ErrNotFound)

	// A different active key with a missing associated user is a data error, not 401.
	raw, key = generatedTestKey(t, keys, user.ID)
	require.NoError(t, db.Delete(&store.User{}, user.ID).Error)
	request(500, "gaigate_internal")
	require.NoError(t, db.Exec("DROP TABLE api_keys").Error)
	request(500, "gaigate_internal")
}

func TestAuthMySQLRevocationKeepsInFlightRequest(t *testing.T) {
	db := authTestDB(t)
	user := authTestOwner(t, db)
	keys := store.NewAPIKeyRepo(db)
	raw, key := generatedTestKey(t, keys, user.ID)
	r := gin.New()
	r.Use(middleware.RequestID(), AuthMiddleware(NewAuthenticator(keys, store.NewUserRepo(db))))
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	r.GET("/protected", func(c *gin.Context) { close(entered); <-release; c.Status(204) })
	request := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/protected", nil)
		req.Header.Set("Authorization", "Bearer "+raw)
		r.ServeHTTP(w, req)
		return w
	}
	var first *httptest.ResponseRecorder
	go func() { defer close(done); first = request() }()
	defer func() { close(release); <-done }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not reach authenticated handler")
	}
	require.NoError(t, keys.RevokeOwned(context.Background(), int64(user.ID), int64(key.ID)))
	fresh := request()
	assert.Equal(t, 403, fresh.Code)
	checkErrorEnvelope(t, fresh, "gaigate_key_revoked")
	// The first request stays blocked in its admitted handler until explicitly released.
	select {
	case <-done:
		t.Fatal("in-flight request unexpectedly ended")
	default:
	}
	t.Cleanup(func() { require.NotNil(t, first); assert.Equal(t, 204, first.Code) })
}

func TestAuthMySQLCancellationReachesBothQueries(t *testing.T) {
	db := authTestDB(t)
	user := authTestOwner(t, db)
	raw, _ := generatedTestKey(t, store.NewAPIKeyRepo(db), user.ID)
	a := NewAuthenticator(store.NewAPIKeyRepo(db), store.NewUserRepo(db))
	pool, err := db.DB()
	require.NoError(t, err)
	for _, table := range []string{"api_keys", "users"} {
		t.Run(table, func(t *testing.T) {
			lock, err := pool.Conn(context.Background())
			require.NoError(t, err)
			defer lock.Close()
			_, err = lock.ExecContext(context.Background(), "LOCK TABLES "+table+" WRITE")
			require.NoError(t, err)
			defer func() { _, err := lock.ExecContext(context.Background(), "UNLOCK TABLES"); require.NoError(t, err) }()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			start := time.Now()
			p, err := a.Authenticate(ctx, raw)
			assert.Nil(t, p)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.False(t, errors.Is(err, ErrUnauthorized))
			assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
			assert.Less(t, time.Since(start), 3*time.Second)
		})
	}
	p, err := a.Authenticate(context.Background(), raw)
	require.NoError(t, err, "queries recover after locks are released")
	assert.Equal(t, int64(user.ID), p.UserID)
}
