//go:build redis_integration

package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/secret"
	"gaigate/internal/store"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// healthGate holds a real provider HTTP call at the transparent observer. The
// request has crossed the actual child gateway's provider transport; no fake
// tester, fake clock or short sleep is used to infer that it is in flight.
type healthGate struct {
	entered, release, done chan struct{}
	canceled               atomic.Bool
}

func newHealthGate() *healthGate {
	return &healthGate{entered: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
}

type realHealthFixture struct {
	root, dir, binary, mockURL, mockKey, admin, user, redisAddr string
	opts                                                        store.Options
	master                                                      []byte
	db                                                          *gorm.DB
	redis                                                       *redis.Client
	redisLeased                                                 bool
	mock                                                        *ownedProcess
	observer                                                    *httptest.Server
	gateways                                                    []*ownedProcess
	channel                                                     store.Channel
	business, probes                                            atomic.Int64
	probeGate, userGate                                         atomic.Pointer[healthGate]
	gatesMu                                                     sync.Mutex
	gates                                                       []*healthGate
}

func newRealHealthFixture(t *testing.T) *realHealthFixture {
	t.Helper()
	require.Equal(t, "1", os.Getenv("GAIGATE_TEST_REDIS_OWNED"), "process fixture requires explicit owned Redis, not a shared instance")
	f := &realHealthFixture{dir: t.TempDir(), opts: processOptions(t), master: bytes.Repeat([]byte{13}, 32), mockKey: "owned-health-b4", redisAddr: os.Getenv("GAIGATE_TEST_REDIS_ADDR")}
	require.NotEmpty(t, f.redisAddr)
	var err error
	f.root, err = filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	// DB14 belongs exclusively to this serial fixture in the owned Redis. Never
	// FLUSHDB/FLUSHALL, and fail instead of erasing any pre-existing state.
	f.redis = store.NewRedis(store.RedisOptions{Addr: f.redisAddr, DB: 14})
	t.Cleanup(func() {
		for _, p := range f.gateways {
			p.stop(t)
		}
		f.gatesMu.Lock()
		gates := append([]*healthGate(nil), f.gates...)
		f.gatesMu.Unlock()
		for _, gate := range gates {
			select {
			case <-gate.entered:
				awaitHealthBarrier(t, gate.done)
			default:
			}
		}
		if f.observer != nil {
			f.observer.Close()
		}
		if f.mock != nil {
			f.mock.stop(t)
		}
		if f.db != nil {
			pool, e := f.db.DB()
			require.NoError(t, e)
			require.NoError(t, pool.Close())
		}
		if f.redisLeased {
			for _, id := range []uint64{1, 2, f.channel.ID} {
				if id != 0 {
					require.NoError(t, f.redis.Del(context.Background(), fmt.Sprintf("gaigate:health:%d", id)).Err())
				}
			}
			size, e := f.redis.DBSize(context.Background()).Result()
			require.NoError(t, e)
			require.Zero(t, size, "fixture must leave its isolated DB empty")
		}
		require.NoError(t, f.redis.Close())
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := f.redis.Info(ctx, "server").Result()
	require.NoError(t, err)
	require.Contains(t, info, "redis_version:", "actual Redis server, not miniredis")
	size, err := f.redis.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Zero(t, size, "owned fixture DB must start empty")
	f.redisLeased = true
	f.binary = buildProcess(t, f.root, f.dir, "./cmd/g-aigate")
	mockBinary := buildProcess(t, f.root, f.dir, "./cmd/mockprovider")
	recreateProcessDatabase(t, f.opts)
	// Own the seed's frozen URL before any gateway starts. If occupied, fail;
	// never send probes into an unknown service or kill its owner.
	listener, err := net.Listen("tcp", "127.0.0.1:18080")
	require.NoError(t, err, "frozen seed URL must be exclusively leased by this fixture")
	t.Cleanup(func() { _ = listener.Close() })
	mockAddr := processPort(t)
	f.mock = startOwnedProcess(t, f.root, mockBinary, append(cleanProcessEnvironment(), "MOCK_ADDR="+mockAddr))
	waitProcessHealth(t, f.mock, "http://"+mockAddr)
	target, err := url.Parse("http://" + mockAddr)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ModifyResponse = func(r *http.Response) error {
		if r.Request.URL.Path == "/healthz" && r.StatusCode == 200 {
			r.Header.Set("X-Gaigate-Fixture-Owned", "14.3")
			r.Header.Set("X-Gaigate-Fixture-Business-Calls", strconv.FormatInt(f.business.Load(), 10))
			r.Header.Set("X-Gaigate-Fixture-Probe-Calls", strconv.FormatInt(f.probes.Load(), 10))
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, _ error) {
		if r.Context().Err() == nil {
			w.WriteHeader(502)
		}
	}
	f.observer = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" && r.Header.Get("Authorization") == "Bearer "+f.mockKey {
			body, e := io.ReadAll(r.Body)
			if e != nil {
				w.WriteHeader(400)
				return
			}
			_ = r.Body.Close()
			r.Body = io.NopCloser(bytes.NewReader(body))
			var fields map[string]json.RawMessage
			if json.Unmarshal(body, &fields) != nil {
				w.WriteHeader(400)
				return
			}
			gatePtr := &f.userGate
			if isFixtureHealthProbe(fields) {
				f.probes.Add(1)
				gatePtr = &f.probeGate
			} else {
				f.business.Add(1)
			}
			gate := gatePtr.Swap(nil)
			if gate != nil {
				close(gate.entered)
				defer close(gate.done)
				select {
				case <-r.Context().Done():
					gate.canceled.Store(true)
					return
				case <-gate.release:
				}
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	f.observer.Listener = listener
	f.observer.Start()
	f.mockURL = f.observer.URL
	require.Equal(t, "http://127.0.0.1:18080", f.mockURL)
	t.Logf("owned mock PID=%d, frozen loopback URL leased; gateway children race=%t; real Redis isolated DB=14, SQL=gaigate_test", f.mock.cmd.Process.Pid, gatewayProcessRace)
	return f
}

func (f *realHealthFixture) start(t *testing.T, interval, cooldown, shutdown time.Duration) (*ownedProcess, string) {
	t.Helper()
	addr := processPort(t)
	path := filepath.Join(f.dir, fmt.Sprintf("health-%d.yaml", len(f.gateways)))
	source := fmt.Sprintf("server:\n  addr: %q\n  shutdown-timeout: %s\nproxy:\n  ttfb-timeout: 15s\n  overall-timeout: 20s\n  stream-buffer: 1\nchannel:\n  probe-interval: %s\n  disable-duration: %s\n  probe-timeout: 10s\n  failure-threshold: 3\n  probe-concurrency: 2\n  state-timeout: 500ms\n", addr, shutdown, interval, cooldown)
	require.NoError(t, os.WriteFile(path, []byte(source), 0600)) // no credentials
	env := append(cleanProcessEnvironment(), "GAIGATE_MYSQL_HOST="+f.opts.Host, "GAIGATE_MYSQL_PORT="+strconv.Itoa(f.opts.Port), "GAIGATE_MYSQL_USER="+f.opts.User,
		"GAIGATE_MYSQL_PASSWORD="+f.opts.Password, "GAIGATE_MYSQL_DATABASE=gaigate_test", "GAIGATE_SECRET_MASTER_KEY="+base64.StdEncoding.EncodeToString(f.master),
		"GAIGATE_ADMIN_PASSWORD=owned-health-fixture", "GAIGATE_REDIS_ADDR="+f.redisAddr, "GAIGATE_REDIS_DB=14")
	p := startOwnedProcess(t, f.root, f.binary, env, "-config", path)
	f.gateways = append(f.gateways, p)
	base := "http://" + addr
	waitProcessHealth(t, p, base)
	waitHealthRound(t, p.logs)
	if f.db == nil {
		var err error
		f.db, err = store.Open(f.opts)
		require.NoError(t, err)
	}
	var database string
	require.NoError(t, f.db.Raw("SELECT DATABASE()").Scan(&database).Error)
	require.Equal(t, "gaigate_test", database)
	t.Logf("owned gateway PID=%d URL=%s interval=%s cooldown=%s threshold=3 concurrency=2 probe_timeout=10s state_timeout=500ms shutdown=%s", p.cmd.Process.Pid, base, interval, cooldown, shutdown)
	return p, base
}

func (f *realHealthFixture) setup(t *testing.T) {
	t.Helper()
	// All child startup seed operations are now complete. Only owned test rows
	// are changed; the seed implementation and parent environment stay intact.
	require.NoError(t, f.db.Model(&store.Channel{}).Where("name IN ?", []string{"demo-primary", "demo-fallback"}).Update("enabled", false).Error)
	f.user = gatewayUser(t, f.db, "owned-b4-user")
	f.admin = gatewayUser(t, f.db, "owned-b4-admin")
	require.NoError(t, f.db.Model(&store.User{}).Where("username = ?", "owned-b4-admin").Update("role", store.RoleAdmin).Error)
	f.channel = gatewayChannel(t, f.db, f.master, "owned-b4", "gpt-4o-demo", "owned-b4-model", f.mockURL, f.mockKey, 100)
	keys, err := store.NewKeyRepo(f.db).ListByChannel(context.Background(), int64(f.channel.ID))
	require.NoError(t, err)
	require.Len(t, keys, 1)
	plain, err := secret.Decrypt(keys[0].ProviderKeyEnc, f.master)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, []byte(f.mockKey)), "actual encrypted channel Key must identify owned mock scenario")
	require.Equal(t, f.mockURL, f.channel.BaseURL)
	t.Logf("verified owned channel=%d, one encrypted mock Key, separate authenticated admin/user Keys in gaigate_test", f.channel.ID)
}

type b4Health struct {
	ChannelID int64 `json:"channel_id"`
	Enabled   bool  `json:"enabled"`
	Available bool  `json:"available"`
	Failures  int32 `json:"consecutive_failures"`
	Until     int64 `json:"disabled_until"`
	ProbeAt   int64 `json:"last_probe_at"`
	ProbeOK   bool  `json:"last_probe_ok"`
}

func (f *realHealthFixture) health(t *testing.T, base string) b4Health {
	t.Helper()
	resp, data, err := gatewayRequest(t, &http.Client{Timeout: 2 * time.Second}, base, f.admin, "GET", fmt.Sprintf("/api/v1/admin/channels/%d/health", f.channel.ID), "")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	var h b4Health
	require.NoError(t, json.Unmarshal(data, &h))
	require.EqualValues(t, f.channel.ID, h.ChannelID)
	return h
}

func (f *realHealthFixture) scenario(t *testing.T, scenario string) {
	t.Helper()
	data, _ := json.Marshal(map[string]any{"key": f.mockKey, "scenario": scenario, "params": map[string]any{}})
	req, err := http.NewRequest("PUT", f.mockURL+"/admin/scenario", bytes.NewReader(data))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Equal(t, 200, resp.StatusCode)
}

func (f *realHealthFixture) chat(t *testing.T, base string, want int) {
	t.Helper()
	resp, _, err := gatewayRequest(t, &http.Client{Timeout: 3 * time.Second}, base, f.user, "POST", "/v1/chat/completions", `{"model":"gpt-4o-demo","messages":[{"role":"user","content":"owned B4"}]}`)
	require.NoError(t, err)
	require.Equal(t, want, resp.StatusCode)
}

func (f *realHealthFixture) arm(probe bool) *healthGate {
	g := newHealthGate()
	f.gatesMu.Lock()
	f.gates = append(f.gates, g)
	f.gatesMu.Unlock()
	if probe {
		f.probeGate.Store(g)
	} else {
		f.userGate.Store(g)
	}
	return g
}

func awaitHealthBarrier(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(12 * time.Second):
		t.Fatal("controlled real HTTP barrier watchdog")
	}
}

func TestHealthProcess_RealRedisCrossProcessDisabledVisibility(t *testing.T) {
	f := newRealHealthFixture(t)
	a, baseA := f.start(t, time.Hour, time.Hour, time.Second)
	b, baseB := f.start(t, time.Hour, time.Hour, time.Second)
	f.setup(t)
	f.scenario(t, "downtime")
	for range 3 {
		f.chat(t, baseA, 502)
	}
	before := f.business.Load()
	h := f.health(t, baseB)
	require.False(t, h.Available)
	require.EqualValues(t, 3, h.Failures)
	require.Positive(t, h.Until)
	f.chat(t, baseB, 503)
	require.Equal(t, before, f.business.Load(), "second OS process must observe Redis disable without provider traffic")
	r, err := store.NewVersionedHealthState(f.redis).LoadVersioned(context.Background(), int64(f.channel.ID))
	require.NoError(t, err)
	require.Equal(t, uint64(3), r.Version)
	require.NotEqual(t, a.cmd.Process.Pid, b.cmd.Process.Pid)
	t.Logf("distinct gateway PIDs=%d/%d; 3 failures committed version=3; second process health=false/request503, business calls=%d unchanged", a.cmd.Process.Pid, b.cmd.Process.Pid, before)
}

func TestHealthProcess_RealRedisB4ScriptAndExpiredProbeBarrier(t *testing.T) {
	f := newRealHealthFixture(t)
	p, base := f.start(t, 200*time.Millisecond, 2*time.Second, time.Second)
	f.setup(t)
	t.Run("script rejects unowned and preserves transport failure", func(t *testing.T) {
		for _, tc := range []struct {
			name, owner, gateway string
			want                 int
		}{
			{"unowned", "", base, 2},
			{"curl transport failure", "1", "http://" + processPort(t), 0},
		} {
			t.Run(tc.name, func(t *testing.T) {
				temp := t.TempDir()
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, findBash(t), "--login", filepath.ToSlash(filepath.Join(f.root, "test/e2e/health_lifecycle.sh")), tc.gateway, f.mockURL)
				cmd.Dir = f.root
				cmd.Env = append(cleanProcessEnvironment(), "GAIGATE_E2E_OWNED_FIXTURE="+tc.owner, "GAIGATE_E2E_ADMIN_API_KEY="+f.admin, "GAIGATE_E2E_API_KEY="+f.user,
					"GAIGATE_E2E_CHANNEL_ID="+strconv.FormatUint(f.channel.ID, 10), "GAIGATE_E2E_MOCK_KEY="+f.mockKey, "TMPDIR="+filepath.ToSlash(temp))
				output, err := cmd.CombinedOutput()
				require.NoError(t, ctx.Err())
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				if tc.want != 0 {
					require.Equal(t, tc.want, exit.ExitCode())
				} else {
					// Closed loopback ports fail immediately on Linux; Windows
					// can consume the connect timeout. Preserve either curl code.
					require.Contains(t, []int{7, 28}, exit.ExitCode())
					require.Contains(t, string(output), fmt.Sprintf("curl=%d", exit.ExitCode()))
				}
				for _, credential := range []string{f.admin, f.user, f.mockKey} {
					require.NotContains(t, string(output), credential)
					require.NotContains(t, strings.Join(cmd.Args, " "), credential)
				}
				files, e := os.ReadDir(temp)
				require.NoError(t, e)
				require.Empty(t, files)
				t.Logf("script actual exit=%d; no response files/credential diagnostics", exit.ExitCode())
			})
		}
	})
	t.Run("B4 shell lifecycle", func(t *testing.T) {
		temp := t.TempDir()
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, findBash(t), "--login", filepath.ToSlash(filepath.Join(f.root, "test/e2e/health_lifecycle.sh")), base, f.mockURL)
		cmd.Dir = f.root
		cmd.Env = append(cleanProcessEnvironment(), "GAIGATE_E2E_OWNED_FIXTURE=1", "GAIGATE_E2E_ADMIN_API_KEY="+f.admin, "GAIGATE_E2E_API_KEY="+f.user,
			"GAIGATE_E2E_CHANNEL_ID="+strconv.FormatUint(f.channel.ID, 10), "GAIGATE_E2E_MOCK_KEY="+f.mockKey, "TMPDIR="+filepath.ToSlash(temp))
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		err := cmd.Run()
		require.NoError(t, ctx.Err(), "script must exit before outer watchdog")
		t.Logf("B4 script stdout:\n%s", stdout.String())
		require.NoError(t, err, "script stderr: %s", stderr.String())
		require.Empty(t, stderr.String())
		require.Contains(t, stdout.String(), "[PASS] B4")
		files, e := os.ReadDir(temp)
		require.NoError(t, e)
		require.Empty(t, files, "responses cleaned")
		for _, credential := range []string{f.admin, f.user, f.mockKey} {
			require.NotContains(t, stdout.String()+stderr.String(), credential)
		}
	})
	t.Run("expired but actual probe still in flight", func(t *testing.T) {
		// Begin from the real script's recovered state; commit a disabled
		// snapshot with an already elapsed timestamp using a distinct client.
		// The next scheduled actual probe is held before the real mock reply.
		previous := f.arm(true)
		awaitHealthBarrier(t, previous.entered)
		state := store.NewVersionedHealthState(f.redis)
		// An in-flight previous round may conflict; retry only the test setup,
		// bounded by 16, never the production probe's completion generation.
		committed := false
		for range 16 {
			r, err := state.LoadVersioned(context.Background(), int64(f.channel.ID))
			require.NoError(t, err)
			next := r.Snapshot
			next.ConsecutiveFailures = 3
			next.DisabledUntil = time.Now().Unix() - 1
			ok, err := state.CompareAndSwap(context.Background(), int64(f.channel.ID), r, next)
			require.NoError(t, err)
			if ok {
				committed = true
				break
			}
		}
		require.True(t, committed)
		gate := f.arm(true)
		close(previous.release)
		awaitHealthBarrier(t, previous.done)
		awaitHealthBarrier(t, gate.entered)
		h := f.health(t, base)
		require.LessOrEqual(t, h.Until, time.Now().Unix())
		require.Positive(t, h.Until)
		require.False(t, h.Available)
		before := f.business.Load()
		f.chat(t, base, 503)
		require.Equal(t, before, f.business.Load())
		close(gate.release)
		awaitHealthBarrier(t, gate.done)
		require.Eventually(t, func() bool { return f.health(t, base).Available }, 3*time.Second, 20*time.Millisecond)
		f.chat(t, base, 200)
		t.Logf("controlled expired state=%d <= now, actual probe blocked; health=false/request503/zero business, then probe release recovered", h.Until)
	})
	p.stop(t)
	for _, credential := range []string{f.admin, f.user, f.mockKey, base64.StdEncoding.EncodeToString(f.master)} {
		require.False(t, strings.Contains(p.logs.String(), credential), "gateway logs must not contain credentials")
	}
}

func TestHealthProcess_RealRedisRejectsOccupiedDBWithoutCleanupWrites(t *testing.T) {
	const childFlag = "GAIGATE_TEST_HEALTH_NONEMPTY_CHILD"
	if os.Getenv(childFlag) == "1" {
		_ = newRealHealthFixture(t) // must fail before any build, SQL or DEL
		t.Fatal("occupied Redis DB was accepted")
	}
	require.Equal(t, "1", os.Getenv("GAIGATE_TEST_REDIS_OWNED"))
	addr := os.Getenv("GAIGATE_TEST_REDIS_ADDR")
	require.NotEmpty(t, addr)
	client := store.NewRedis(store.RedisOptions{Addr: addr, DB: 14})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	size, err := client.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Zero(t, size)
	const key = "gaigate:health:1"
	require.NoError(t, client.HSet(ctx, key, "other_fixture", "preserve").Err())
	t.Cleanup(func() { require.NoError(t, client.Del(context.Background(), key).Err()) })
	binary, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestHealthProcess_RealRedisRejectsOccupiedDBWithoutCleanupWrites$", "-test.v")
	cmd.Env = append(cleanProcessEnvironment(), childFlag+"=1", "GAIGATE_TEST_REDIS_ADDR="+addr, "GAIGATE_TEST_REDIS_OWNED=1")
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err())
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit)
	require.Equal(t, 1, exit.ExitCode())
	require.Contains(t, string(output), "owned fixture DB must start empty")
	fields, err := client.HGetAll(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"other_fixture": "preserve"}, fields, "rejected fixture must not DEL pre-existing health keys in cleanup")
	t.Log("expected child rejection exit=1 before build/SQL; pre-existing health hash preserved byte-for-byte")
}
