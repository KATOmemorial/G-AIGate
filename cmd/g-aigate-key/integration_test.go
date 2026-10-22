package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"gaigate/internal/access"
	"gaigate/internal/config"
	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

// The fixture alone creates the fixed test schema and existing users. The child
// receives an explicit, verified mapping; business GAIGATE_* values are filtered.
func commandTestOptions(t *testing.T) store.Options {
	t.Helper()
	port := 3307
	if p := os.Getenv("GAIGATE_TEST_MYSQL_PORT"); p != "" {
		var err error
		port, err = strconv.Atoi(p)
		require.NoError(t, err)
	}
	host, password := os.Getenv("GAIGATE_TEST_MYSQL_HOST"), os.Getenv("GAIGATE_TEST_MYSQL_PASSWORD")
	if host == "" {
		host = "127.0.0.1"
	}
	if password == "" {
		password = "gaigate_dev"
	}
	return store.Options{Host: host, Port: port, User: "root", Password: password, Database: "gaigate_test"}
}

func commandTestDB(t *testing.T, migrate bool) (*gorm.DB, store.Options) {
	t.Helper()
	opts := commandTestOptions(t)
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
	if migrate {
		require.NoError(t, store.RunMigrations(opts, "../../migrations"))
	}
	db, err := store.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { pool, err := db.DB(); require.NoError(t, err); require.NoError(t, pool.Close()) })
	return db, opts
}

func commandTestUser(t *testing.T, db *gorm.DB) store.User {
	t.Helper()
	now := time.Now().UTC()
	group := store.Group{Name: "command-fixture", CreatedAt: now}
	require.NoError(t, db.Create(&group).Error)
	u := store.User{Username: "existing-user", PasswordHash: "fixture-password-digest", Role: store.RoleUser, GroupID: group.ID, Status: 1, CreatedAt: now, UpdatedAt: now}
	require.NoError(t, db.Create(&u).Error)
	return u
}

func buildCommand(t *testing.T) string {
	t.Helper()
	name := "g-aigate-key"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(t.TempDir(), name)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	args := []string{"build"}
	if commandRaceBuild {
		args = append(args, "-race")
	}
	args = append(args, "-o", path, ".")
	build := exec.CommandContext(ctx, "go", args...)
	data, err := build.CombinedOutput()
	require.NoError(t, err, "command build failed: %s", data)
	return path
}

func commandEnvironment(t *testing.T, opts store.Options, redisAddress string) []string {
	t.Helper()
	require.Equal(t, "gaigate_test", opts.Database)
	env := make([]string, 0)
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if !strings.HasPrefix(strings.ToUpper(name), "GAIGATE_") {
			env = append(env, value)
		}
	}
	values := map[string]string{"GAIGATE_MYSQL_HOST": opts.Host, "GAIGATE_MYSQL_PORT": strconv.Itoa(opts.Port), "GAIGATE_MYSQL_USER": opts.User,
		"GAIGATE_MYSQL_PASSWORD": opts.Password, "GAIGATE_MYSQL_DATABASE": "gaigate_test",
		"GAIGATE_SECRET_MASTER_KEY": base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32)),
		"GAIGATE_REDIS_ADDR":        redisAddress, "GAIGATE_SERVER_ADDR": redisAddress, "GAIGATE_SERVER_METRICS_ADDR": redisAddress}
	for name, value := range values {
		env = append(env, name+"="+value)
	}
	assert.Contains(t, env, "GAIGATE_MYSQL_DATABASE=gaigate_test")
	assert.Contains(t, env, "GAIGATE_MYSQL_PORT="+strconv.Itoa(opts.Port))
	return env
}

func executeCommand(t *testing.T, binary string, env []string, args ...string) (int, []byte, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = env
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	err := cmd.Run()
	if err == nil {
		return 0, out.Bytes(), errOut.String()
	}
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "command did not exit normally")
	require.NoError(t, ctx.Err(), "fixture watchdog must not be responsible for command exit")
	return exitErr.ExitCode(), out.Bytes(), errOut.String()
}

func decodeCreated(t *testing.T, data []byte) access.CreatedKey {
	t.Helper()
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &fields))
	assert.Len(t, fields, 5)
	for _, name := range []string{"id", "name", "key", "key_prefix", "created_at"} {
		assert.Contains(t, fields, name)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	var key access.CreatedKey
	require.NoError(t, decoder.Decode(&key))
	var extra any
	assert.ErrorIs(t, decoder.Decode(&extra), io.EOF)
	assert.Positive(t, key.ID)
	assert.Len(t, key.Key, 46)
	assert.True(t, strings.HasPrefix(key.Key, "sk-"))
	assert.True(t, key.KeyPrefix == key.Key[:8])
	assert.Equal(t, time.UTC, key.CreatedAt.Location())
	return key
}

func TestCommandProcessMySQLAndHTTPLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db, opts := commandTestDB(t, true)
	owner := commandTestUser(t, db)
	binary := buildCommand(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var redisConnections atomic.Int64
	listenerDone := make(chan struct{})
	go func() {
		defer close(listenerDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			redisConnections.Add(1)
			_ = conn.Close()
		}
	}()
	defer func() { _ = listener.Close(); <-listenerDone }()
	env := commandEnvironment(t, opts, listener.Addr().String())
	counts := func() map[string]int64 {
		out := map[string]int64{}
		for _, table := range []string{"users", "groups", "channels", "channel_keys", "channel_models", "model_aliases", "model_prices", "quotas", "bills", "usage_records", "schema_migrations"} {
			var count int64
			require.NoError(t, db.Table(table).Count(&count).Error)
			out[table] = count
		}
		return out
	}
	before := counts()
	code, data, stderr := executeCommand(t, binary, env, "-config", filepath.Join(t.TempDir(), "missing.yaml"), "-user", owner.Username)
	assert.Zero(t, code)
	assert.Empty(t, stderr)
	initial := decodeCreated(t, data)
	assert.Empty(t, initial.Name)
	assert.Equal(t, before, counts(), "command must not seed, migrate or create users")
	assert.Zero(t, redisConnections.Load(), "command must not dial Redis or start HTTP")
	keys, users := store.NewAPIKeyRepo(db), store.NewUserRepo(db)
	auth := access.NewAuthenticator(keys, users)
	principal, err := auth.Authenticate(context.Background(), initial.Key)
	require.NoError(t, err)
	assert.Equal(t, int64(owner.ID), principal.UserID)
	var stored store.APIKey
	require.NoError(t, db.First(&stored, initial.ID).Error)
	digest := sha256.Sum256([]byte(initial.Key))
	assert.True(t, stored.KeyHash == hex.EncodeToString(digest[:]))
	svc := access.NewKeyService(keys, users)
	backup, err := svc.Create(access.WithPrincipal(context.Background(), principal), "backup")
	require.NoError(t, err)
	r := gin.New()
	r.Use(middleware.RequestID())
	access.RegisterUserKeyRoutes(r, access.AuthMiddleware(auth), svc)
	server := httptest.NewServer(r)
	defer server.Close()
	request := func(method, path, raw, body string, status int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(context.Background(), method, server.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+raw)
		req.Header.Set("Content-Type", "application/json")
		res, err := server.Client().Do(req)
		require.NoError(t, err)
		defer res.Body.Close()
		result, err := io.ReadAll(res.Body)
		require.NoError(t, err)
		assert.Equal(t, status, res.StatusCode)
		assert.Equal(t, "no-store", res.Header.Get("Cache-Control"))
		if status == 204 {
			assert.Empty(t, result)
		}
		if status != 201 {
			assert.NotContains(t, string(result), initial.Key)
			assert.NotContains(t, string(result), stored.KeyHash)
			assert.NotContains(t, string(result), owner.PasswordHash)
		}
		return result
	}
	path := "/api/v1/user/keys"
	created := decodeCreated(t, request("POST", path, initial.Key, `{"name":"由命令凭据创建"}`, 201))
	_, err = auth.Authenticate(context.Background(), created.Key)
	require.NoError(t, err)
	var views []access.KeyView
	require.NoError(t, json.Unmarshal(request("GET", path, initial.Key, "", 200), &views))
	assert.Len(t, views, 3)
	request("DELETE", fmt.Sprintf("%s/%d", path, created.ID), initial.Key, "", 204)
	request("DELETE", fmt.Sprintf("%s/%d", path, created.ID), backup.Key, "", 204)
	_, err = auth.Authenticate(context.Background(), created.Key)
	assert.ErrorIs(t, err, access.ErrKeyRevoked)
	request("GET", path, created.Key, "", 403)
	request("DELETE", fmt.Sprintf("%s/%d", path, initial.ID), initial.Key, "", 204)
	request("GET", path, initial.Key, "", 403)
	request("GET", path, backup.Key, "", 200)
	assert.Equal(t, before, counts())
	assert.Zero(t, redisConnections.Load())

	// Negative child-process cases must neither return raw nor add a Key.
	var countBefore int64
	require.NoError(t, db.Model(&store.APIKey{}).Count(&countBefore).Error)
	disabled := owner
	disabled.ID = 0
	disabled.Username = "disabled-user"
	disabled.Status = 0
	require.NoError(t, db.Create(&disabled).Error)
	for _, args := range [][]string{{}, {"-user", "missing-user"}, {"-user", disabled.Username}, {"-user", owner.Username, "-name", strings.Repeat("钥", 129)}} {
		code, out, errText := executeCommand(t, binary, env, args...)
		assert.NotZero(t, code)
		assert.Empty(t, out)
		assert.NotEmpty(t, errText)
		assert.NotContains(t, errText, opts.Password)
		assert.NotContains(t, errText, initial.Key)
	}
	var countAfter int64
	require.NoError(t, db.Model(&store.APIKey{}).Count(&countAfter).Error)
	assert.Equal(t, countBefore, countAfter)
	for _, override := range []string{"GAIGATE_SECRET_MASTER_KEY=invalid-secret", "GAIGATE_PROXY_TTFB_TIMEOUT=0s", "GAIGATE_MYSQL_PORT=1"} {
		name, _, _ := strings.Cut(override, "=")
		var changed []string
		for _, entry := range env {
			if !strings.HasPrefix(entry, name+"=") {
				changed = append(changed, entry)
			}
		}
		changed = append(changed, override)
		code, out, errText := executeCommand(t, binary, changed, "-user", owner.Username)
		assert.NotZero(t, code)
		assert.Empty(t, out)
		assert.NotContains(t, errText, "invalid-secret")
		assert.NotContains(t, errText, opts.Password)
	}
}

func TestCommandProcessNeverMigratesMissingSchema(t *testing.T) {
	db, opts := commandTestDB(t, false)
	binary := buildCommand(t)
	env := commandEnvironment(t, opts, "127.0.0.1:1")
	code, out, stderr := executeCommand(t, binary, env, "-user", "admin")
	assert.NotZero(t, code)
	assert.Empty(t, out)
	assert.NotEmpty(t, stderr)
	var tables []string
	require.NoError(t, db.Raw("SHOW TABLES").Scan(&tables).Error)
	assert.Empty(t, tables, "command must leave an empty schema empty")
}

func TestCommandProcessTenSecondInitializationBudget(t *testing.T) {
	opts, entered, peerClosed := blockedMySQLPeer(t, "handshake")
	binary := buildCommand(t)
	env := commandEnvironment(t, opts, "127.0.0.1:1")
	observerCtx, stopObserver := context.WithCancel(context.Background())
	observed := make(chan struct {
		elapsed time.Duration
		err     error
	}, 1)
	observerDone := make(chan struct{})
	go func() {
		defer close(observerDone)
		select {
		case <-entered:
		case <-observerCtx.Done():
			return
		}
		start := time.Now()
		select {
		case err := <-peerClosed:
			observed <- struct {
				elapsed time.Duration
				err     error
			}{time.Since(start), err}
		case <-observerCtx.Done():
		}
	}()
	defer func() { stopObserver(); <-observerDone }()
	start := time.Now()
	code, out, stderr := executeCommand(t, binary, env, "-user", "existing-user")
	elapsed := time.Since(start)
	assert.NotZero(t, code)
	assert.Empty(t, out)
	assert.NotEmpty(t, stderr)
	assert.GreaterOrEqual(t, elapsed, 9*time.Second)
	assert.Less(t, elapsed, 13*time.Second)
	select {
	case <-entered:
	default:
		t.Fatal("child never reached TCP fixture")
	}
	select {
	case result := <-observed:
		require.NoError(t, result.err)
		assert.GreaterOrEqual(t, result.elapsed, 9*time.Second)
		assert.Less(t, result.elapsed, 12*time.Second)
		t.Logf("process lifetime=%s (includes runtime startup); handshake wait=%s; own 10s budget, watchdog not used", elapsed.Round(time.Millisecond), result.elapsed.Round(time.Millisecond))
	case <-time.After(2 * time.Second):
		t.Fatal("command left handshake connection alive")
	}
}

func TestPrivateInitializerRealVersionTransactionsAndCallerContexts(t *testing.T) {
	fixture, opts := commandTestDB(t, true)
	owner := commandTestUser(t, fixture)
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	db, err := openMySQL(ctx, opts)
	require.NoError(t, err)
	pool, err := db.DB()
	require.NoError(t, err)
	defer pool.Close()
	var actualVersion string
	require.NoError(t, fixture.Raw("SELECT VERSION()").Scan(&actualVersion).Error)
	assert.Equal(t, actualVersion, db.Dialector.(*mysql.Dialector).ServerVersion)
	assert.Same(t, pool, db.ConnPool)
	assert.Same(t, pool, db.Statement.ConnPool)
	var charset string
	require.NoError(t, db.Raw("SELECT @@character_set_connection").Scan(&charset).Error)
	assert.Equal(t, "utf8mb4", charset)
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		u, err := store.NewUserRepo(tx).Get(ctx, int64(owner.ID))
		require.NoError(t, err)
		assert.Equal(t, owner.Username, u.Username)
		return nil
	}))
	child, childCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer childCancel()
	start := time.Now()
	var value int
	err = db.WithContext(child).Raw("SELECT SLEEP(3)").Scan(&value).Error
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)
	require.NoError(t, ctx.Err())
	cause := fmt.Errorf("independent caller cancellation")
	independent, independentCancel := context.WithCancelCause(context.Background())
	independentCancel(cause)
	err = db.WithContext(independent).Raw("SELECT 1").Scan(&value).Error
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, context.Cause(independent), cause)
	require.NoError(t, db.WithContext(ctx).Raw("SELECT 1").Scan(&value).Error)
	assert.Equal(t, 1, value)
	require.NoError(t, pool.Close())
	assert.Zero(t, pool.Stats().OpenConnections)
	assert.Error(t, pool.PingContext(ctx))
}

func TestCommandRealSQLCancellationAndOutputFailure(t *testing.T) {
	db, opts := commandTestDB(t, true)
	owner := commandTestUser(t, db)
	configLoader := func(string) (*config.Config, error) {
		return &config.Config{MySQL: config.MySQLConfig{Host: opts.Host, Port: opts.Port, User: opts.User, Password: opts.Password, Database: "gaigate_test"}}, nil
	}
	for _, stage := range []string{"users", "api_keys", "output"} {
		t.Run(stage, func(t *testing.T) {
			var actualPool *sql.DB
			entered := make(chan struct{}, 1)
			deps := commandDeps{load: configLoader, open: func(ctx context.Context, opts store.Options) (*keyDatabase, error) {
				actual, err := openMySQL(ctx, opts)
				if err != nil {
					return nil, err
				}
				actualPool, err = actual.DB()
				require.NoError(t, err)
				observe := func(tx *gorm.DB) {
					if tx.Statement.Table == stage {
						select {
						case entered <- struct{}{}:
						default:
						}
					}
				}
				require.NoError(t, actual.Callback().Query().Before("gorm:query").Register("134:observe_query", observe))
				require.NoError(t, actual.Callback().Create().Before("gorm:create").Register("134:observe_create", observe))
				users := store.NewUserRepo(actual)
				return &keyDatabase{users: users, service: access.NewKeyService(store.NewAPIKeyRepo(actual), users), close: actualPool.Close}, nil
			}}
			var lock *sql.Conn
			if stage != "output" {
				pool, err := db.DB()
				require.NoError(t, err)
				lock, err = pool.Conn(context.Background())
				require.NoError(t, err)
				_, err = lock.ExecContext(context.Background(), "LOCK TABLES "+stage+" WRITE")
				require.NoError(t, err)
			}
			unlock := func() {
				if lock != nil {
					_, err := lock.ExecContext(context.Background(), "UNLOCK TABLES")
					require.NoError(t, err)
					require.NoError(t, lock.Close())
					lock = nil
				}
			}
			defer unlock()
			parent, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			var out, errOut bytes.Buffer
			var stdout io.Writer = &out
			if stage == "output" {
				stdout = failedOutput{}
			}
			start := time.Now()
			code := run(parent, []string{"-user", owner.Username}, stdout, &errOut, deps)
			assert.Equal(t, 1, code)
			assert.Empty(t, out.String())
			require.NotNil(t, actualPool)
			assert.Zero(t, actualPool.Stats().OpenConnections)
			assert.Error(t, actualPool.PingContext(context.Background()))
			if stage != "output" {
				select {
				case <-entered:
				default:
					t.Fatal("did not enter locked repository phase")
				}
				assert.ErrorIs(t, parent.Err(), context.DeadlineExceeded)
				assert.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond)
				assert.Less(t, time.Since(start), 2*time.Second)
			} else {
				assert.Contains(t, errOut.String(), "可能已创建，请另行查询或吊销")
			}
			unlock()
			var count int64
			require.NoError(t, db.Model(&store.APIKey{}).Count(&count).Error)
			if stage == "output" {
				assert.Equal(t, int64(1), count, "output failure must not claim rollback")
			} else {
				assert.Zero(t, count, "canceled command must not create a Key")
			}
		})
	}
}
