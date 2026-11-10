//go:build redis_integration

package main

import (
	"bytes"
	"context"
	"crypto/rand"
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
	"strconv"
	"strings"
	"testing"
	"time"

	"gaigate/internal/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type failoverFixture struct {
	root, dir, binary, token, user, redisAddr string
	opts                                      store.Options
	master                                    []byte
	db                                        *gorm.DB
	redis                                     *redis.Client
	leased                                    bool
	gateways                                  []*ownedProcess
	mocks                                     [2]*ownedProcess
	urls, keys                                [2]string
	channels                                  [2]store.Channel
	seed, control                             *httptest.Server
	client                                    *http.Client
}

func newFailoverFixture(t *testing.T) *failoverFixture {
	t.Helper()
	require.Equal(t, "1", os.Getenv("GAIGATE_TEST_REDIS_OWNED"))
	f := &failoverFixture{dir: t.TempDir(), opts: processOptions(t), master: bytes.Repeat([]byte{29}, 32), redisAddr: os.Getenv("GAIGATE_TEST_REDIS_ADDR"), client: &http.Client{Timeout: 8 * time.Second}}
	require.NotEmpty(t, f.redisAddr)
	var err error
	f.root, err = filepath.Abs("../..")
	require.NoError(t, err)
	random := make([]byte, 32)
	_, err = rand.Read(random)
	require.NoError(t, err)
	f.token = hex.EncodeToString(random)
	f.redis = store.NewRedis(store.RedisOptions{Addr: f.redisAddr, DB: 13})
	t.Cleanup(func() {
		for _, p := range f.gateways {
			p.stop(t)
		}
		for _, p := range f.mocks {
			if p != nil {
				p.stop(t)
			}
		}
		if f.control != nil {
			f.control.Close()
		}
		if f.seed != nil {
			f.seed.Close()
		}
		f.client.CloseIdleConnections()
		if f.db != nil {
			pool, e := f.db.DB()
			require.NoError(t, e)
			require.NoError(t, pool.Close())
		}
		if f.leased {
			for _, id := range []uint64{1, 2, f.channels[0].ID, f.channels[1].ID} {
				if id > 0 {
					require.NoError(t, f.redis.Del(context.Background(), failoverRedisKeys(id)...).Err())
				}
			}
			size, e := f.redis.DBSize(context.Background()).Result()
			require.NoError(t, e)
			require.Zero(t, size, "owned DB13 must be empty after all process owners join")
		}
		require.NoError(t, f.redis.Close())
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	info, err := f.redis.Info(ctx, "server").Result()
	require.NoError(t, err)
	require.Contains(t, info, "redis_version:")
	size, err := f.redis.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Zero(t, size, "never erase occupied fixture DB")
	f.leased = true
	for i, name := range []string{"primary", "backup"} {
		addr := processPort(t)
		f.urls[i] = "http://" + addr
		f.keys[i] = "owned-b5-upstream-" + name
		env := append(cleanProcessEnvironment(), "GAIGATE_FAILOVER_MOCK_HELPER=owned-v1", "GAIGATE_FAILOVER_MOCK_ADDR="+addr, "GAIGATE_FAILOVER_CONTROL_TOKEN="+f.token, "GAIGATE_FAILOVER_UPSTREAM_KEY="+f.keys[i])
		f.mocks[i] = startOwnedProcess(t, f.root, os.Args[0], env, "-test.run=^TestFailoverMockProcessHelper$", "-test.v")
		waitProcessHealth(t, f.mocks[i], f.urls[i])
	}
	require.NotEqual(t, f.mocks[0].cmd.Process.Pid, f.mocks[1].cmd.Process.Pid)
	listener, err := net.Listen("tcp", "127.0.0.1:18080")
	require.NoError(t, err, "own seed address before any gateway startup")
	f.seed = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var fields map[string]json.RawMessage
		if json.NewDecoder(r.Body).Decode(&fields) != nil || !respondToFixtureHealthProbe(w, fields) {
			w.WriteHeader(400)
		}
	}))
	f.seed.Listener = listener
	f.seed.Start()
	recreateProcessDatabase(t, f.opts)
	require.NoError(t, store.RunMigrations(f.opts, "../../migrations"))
	f.db, err = store.Open(f.opts)
	require.NoError(t, err)
	require.NoError(t, store.Seed(f.db, store.SeedOptions{AdminPassword: "owned-b5-admin", MasterKey: f.master}))
	f.user = gatewayUser(t, f.db, "owned-b5-user")
	for i, name := range []string{"primary", "backup"} {
		f.channels[i] = gatewayChannel(t, f.db, f.master, "owned-b5-"+name, "owned-b5", "mapped-"+name, f.urls[i], f.keys[i], int32(20-i*10))
		require.NoError(t, f.db.Create(&store.ChannelModel{ChannelID: f.channels[i].ID, Model: "owned-b5-other", UpstreamModel: "other-" + name}).Error)
	}
	models := store.NewModelRepo(f.db, store.NewConfigCache(0))
	for _, name := range []string{"owned-b5", "owned-b5-other"} {
		first, second := f.channels[0].ID, f.channels[1].ID
		if name == "owned-b5-other" {
			first, second = second, first
		}
		require.NoError(t, models.Create(context.Background(), &store.ModelAlias{Name: name, Strategy: "fallback_chain", DefaultChannelID: &first, FallbackIDs: fmt.Sprintf("[%d]", second)}))
	}
	f.binary = buildProcess(t, f.root, f.dir, "./cmd/g-aigate")
	f.control = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+f.token {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/mode" {
			var c struct {
				Target  string `json:"target"`
				Mode    string `json:"mode"`
				Release bool   `json:"release"`
			}
			if json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&c) != nil {
				w.WriteHeader(400)
				return
			}
			i := 0
			if c.Target == "backup" {
				i = 1
			} else if c.Target != "primary" {
				w.WriteHeader(400)
				return
			}
			if f.mockControl(i, c.Mode, c.Release) != nil {
				w.WriteHeader(500)
				return
			}
		}
		snapshot, e := f.snapshot()
		if e != nil {
			w.WriteHeader(500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(snapshot)
	}))
	t.Logf("B5 owns mock PIDs=%d/%d, SQL=gaigate_test, real Redis DB13; health threshold=1000 interval=1h; accelerated breaker min=3 open=2s (defaults remain20/30s)", f.mocks[0].cmd.Process.Pid, f.mocks[1].cmd.Process.Pid)
	return f
}
func failoverRedisKeys(id uint64) []string {
	return []string{fmt.Sprintf("gaigate:health:%d", id), fmt.Sprintf("gaigate:breaker:{%d}", id), fmt.Sprintf("gaigate:breaker:events:{%d}", id), fmt.Sprintf("gaigate:breaker:pending:{%d}", id)}
}
func (f *failoverFixture) start(t *testing.T, ttfb, overall, shutdown time.Duration, max int) (*ownedProcess, string) {
	t.Helper()
	addr := processPort(t)
	path := filepath.Join(f.dir, fmt.Sprintf("gateway-%d.yaml", len(f.gateways)))
	config := fmt.Sprintf("server:\n  addr: %q\n  shutdown-timeout: %s\nproxy:\n  ttfb-timeout: %s\n  overall-timeout: %s\n  stream-buffer: 1\nchannel:\n  failure-threshold: 1000\n  probe-interval: 1h\n  probe-timeout: 3s\n  state-timeout: 250ms\nrouter:\n  default-strategy: fallback_chain\n  route-timeout: 2s\n  state-timeout: 250ms\n  max-attempts: %d\n  breaker-window: 10s\n  breaker-min-samples: 3\n  breaker-err-threshold: 0.5\n  breaker-open-duration: 2s\n", addr, shutdown, ttfb, overall, max)
	require.NoError(t, os.WriteFile(path, []byte(config), 0600))
	env := append(cleanProcessEnvironment(), "GAIGATE_MYSQL_HOST="+f.opts.Host, "GAIGATE_MYSQL_PORT="+strconv.Itoa(f.opts.Port), "GAIGATE_MYSQL_USER="+f.opts.User, "GAIGATE_MYSQL_PASSWORD="+f.opts.Password, "GAIGATE_MYSQL_DATABASE=gaigate_test", "GAIGATE_SECRET_MASTER_KEY="+base64.StdEncoding.EncodeToString(f.master), "GAIGATE_ADMIN_PASSWORD=owned-b5-admin", "GAIGATE_REDIS_ADDR="+f.redisAddr, "GAIGATE_REDIS_DB=13")
	p := startOwnedProcess(t, f.root, f.binary, env, "-config", path)
	f.gateways = append(f.gateways, p)
	base := "http://" + addr
	waitProcessHealth(t, p, base)
	waitHealthRound(t, p.logs)
	for i := range f.urls {
		require.Positive(t, f.stats(t, i).Probes, "actual normal initial probes precede every fault")
	}
	return p, base
}
func (f *failoverFixture) mockControl(i int, mode string, release bool) error {
	body, _ := json.Marshal(map[string]any{"mode": mode, "release": release})
	req, err := http.NewRequest("POST", f.urls[i]+"/control", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+f.token)
	resp, err := f.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("fixture control status %d", resp.StatusCode)
	}
	return nil
}
func (f *failoverFixture) mode(t *testing.T, i int, mode string) {
	t.Helper()
	require.NoError(t, f.mockControl(i, mode, false))
}
func (f *failoverFixture) readStats(i int) (failoverMockStats, error) {
	var s failoverMockStats
	req, _ := http.NewRequest("GET", f.urls[i]+"/state", nil)
	req.Header.Set("Authorization", "Bearer "+f.token)
	resp, err := f.client.Do(req)
	if err != nil {
		return s, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return s, fmt.Errorf("state unavailable")
	}
	err = json.NewDecoder(resp.Body).Decode(&s)
	return s, err
}
func (f *failoverFixture) stats(t *testing.T, i int) failoverMockStats {
	t.Helper()
	s, err := f.readStats(i)
	require.NoError(t, err)
	return s
}

type failoverSnapshot struct {
	State    string `json:"state"`
	Eligible bool   `json:"eligible"`
	Primary  int    `json:"primary"`
	Backup   int    `json:"backup"`
	Active   int    `json:"active"`
	Health   bool   `json:"health"`
}

func (f *failoverFixture) snapshot() (failoverSnapshot, error) {
	var out failoverSnapshot
	p, e := f.readStats(0)
	if e != nil {
		return out, e
	}
	b, e := f.readStats(1)
	if e != nil {
		return out, e
	}
	out.Primary, out.Backup, out.Active = p.Business, b.Business, p.Active
	fields, e := f.redis.HGetAll(context.Background(), fmt.Sprintf("gaigate:breaker:{%d}", f.channels[0].ID)).Result()
	if e != nil {
		return out, e
	}
	out.State = fields["state"]
	if out.State == "" {
		out.State = "closed"
	}
	now, e := f.redis.Time(context.Background()).Result()
	if e != nil {
		return out, e
	}
	until, _ := strconv.ParseInt(fields["open_until_ms"], 10, 64)
	out.Eligible = out.State == "open" && now.UnixMilli() >= until
	health, e := store.NewVersionedHealthState(f.redis).LoadVersioned(context.Background(), int64(f.channels[0].ID))
	out.Health = health.Snapshot.DisabledUntil == 0
	return out, e
}
func failoverWait(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("bounded B5 state/entry barrier expired")
		}
	}
}

type failoverResponse struct {
	Status int
	Body   []byte
	ID     string
	Err    error
}

func (f *failoverFixture) chat(ctx context.Context, base, key, model string, stream bool) failoverResponse {
	return f.chatUsing(ctx, f.client, base, key, model, stream)
}
func (f *failoverFixture) chatUsing(ctx context.Context, client *http.Client, base, key, model string, stream bool) failoverResponse {
	body := fmt.Sprintf(`{"model":%q,"stream":%t,"messages":[{"role":"user","content":"private-b5-message"}],"strategy":"cost_first","fallback_ids":[999999]}`, model, stream)
	req, err := http.NewRequestWithContext(ctx, "POST", base+"/v1/chat/completions", strings.NewReader(body))
	if err != nil {
		return failoverResponse{Err: err}
	}
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Route-Strategy", "weighted")
	resp, err := client.Do(req)
	if err != nil {
		return failoverResponse{Err: err}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	return failoverResponse{resp.StatusCode, data, resp.Header.Get("X-Request-Id"), err}
}
func (f *failoverFixture) success(t *testing.T, base string) failoverResponse {
	t.Helper()
	r := f.chat(context.Background(), base, f.user, "owned-b5", false)
	require.NoError(t, r.Err)
	require.Equal(t, 200, r.Status)
	return r
}
func (f *failoverFixture) privacy(t *testing.T, extra ...string) {
	t.Helper()
	secrets := append([]string{f.token, f.user, f.keys[0], f.keys[1], f.urls[0], f.urls[1], "private-b5-message"}, extra...)
	for _, p := range f.gateways {
		for _, s := range secrets {
			require.NotContains(t, p.logs.String(), s)
		}
	}
}

func TestFailoverRedisProcessesSingleProbeAndRecovery(t *testing.T) {
	verifyFailoverLifecycle(t, true)
}
func TestFailoverRedisConcurrentHalfOpenBound(t *testing.T) {
	verifyFailoverLifecycle(t, false)
}
func verifyFailoverLifecycle(t *testing.T, checkOpenSkip bool) {
	f := newFailoverFixture(t)
	a, baseA := f.start(t, 5*time.Second, 6*time.Second, time.Second, 3)
	b, baseB := f.start(t, 5*time.Second, 6*time.Second, time.Second, 3)
	require.NotEqual(t, a.cmd.Process.Pid, b.cmd.Process.Pid)
	r := f.success(t, baseA)
	require.Contains(t, string(r.Body), "mapped-primary")
	require.Equal(t, 1, f.stats(t, 0).Business)
	require.Zero(t, f.stats(t, 1).Business)
	f.mode(t, 0, "429")
	for range 2 {
		r = f.success(t, baseA)
		require.Contains(t, string(r.Body), "mapped-backup")
	}
	snapshot, err := f.snapshot()
	require.NoError(t, err)
	require.Equal(t, "open", snapshot.State)
	require.True(t, snapshot.Health)
	before := snapshot.Primary
	if checkOpenSkip {
		f.success(t, baseB)
		require.Equal(t, before, f.stats(t, 0).Business, "open must skip the primary in the other OS process")
	}
	f.mode(t, 0, "hold")
	failoverWait(t, func() bool { s, e := f.snapshot(); return e == nil && s.Eligible })
	probe := make(chan failoverResponse, 1)
	go func() { probe <- f.chat(context.Background(), baseA, f.user, "owned-b5", false) }()
	failoverWait(t, func() bool { s, e := f.snapshot(); return e == nil && s.State == "half-open" && s.Active == 1 })
	const n = 24
	results := make(chan failoverResponse, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			base := baseA
			if i%2 != 0 {
				base = baseB
			}
			results <- f.chatUsing(ctx, client, base, f.user, "owned-b5", false)
		}(i)
	}
	responses := make([]failoverResponse, 0, n)
	for range n {
		responses = append(responses, <-results)
	}
	require.Equal(t, before+1, f.stats(t, 0).Business, "global single-probe bound: denied permits must never open a primary")
	for _, r := range responses {
		require.NoError(t, r.Err)
		require.Equal(t, 200, r.Status)
		require.Contains(t, string(r.Body), "mapped-backup")
	}
	require.Equal(t, before+1, f.stats(t, 0).Business, "one global half-open opener across 24 clients and two OS processes")
	pending, err := f.redis.ZCard(context.Background(), fmt.Sprintf("gaigate:breaker:pending:{%d}", f.channels[0].ID)).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, pending)
	require.NoError(t, f.mockControl(0, "", true))
	r = <-probe
	require.NoError(t, r.Err)
	require.Equal(t, 200, r.Status)
	failoverWait(t, func() bool { s, e := f.snapshot(); return e == nil && s.State == "closed" })
	backupBefore := f.stats(t, 1).Business
	r = f.success(t, baseB)
	require.Contains(t, string(r.Body), "mapped-primary")
	require.Equal(t, backupBefore, f.stats(t, 1).Business)
	f.privacy(t)
	t.Logf("actual primary=%d backup=%d; two gateways, 24 clients, one held probe, Redis closed after success; health still available", f.stats(t, 0).Business, f.stats(t, 1).Business)
}

func TestFailoverRedisInterruptionCancellationAndLogicalBudget(t *testing.T) {
	for _, mode := range []string{"interrupted", "cancel", "budget", "slow-reader", "max-one"} {
		t.Run(mode, func(t *testing.T) {
			f := newFailoverFixture(t)
			ttfb, overall, max := 800*time.Millisecond, time.Second, 3
			if mode == "cancel" {
				ttfb, overall = 5*time.Second, 6*time.Second
			}
			if mode == "max-one" {
				max = 1
			}
			gateway, base := f.start(t, ttfb, overall, time.Second, max)
			switch mode {
			case "interrupted":
				f.mode(t, 0, "interrupted")
				dir := t.TempDir()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, "curl", "--silent", "--noproxy", "*", "--http1.1", "--max-time", "4", "--config", "-", "--header", "Content-Type: application/json", "--data-binary", `{"model":"owned-b5","stream":true,"messages":[{"role":"user","content":"b5"}]}`, "--output", filepath.Join(dir, "body"), "--url", base+"/v1/chat/completions")
				cmd.Stdin = strings.NewReader(fmt.Sprintf("header = \"Authorization: Bearer %s\"\n", f.user))
				err := cmd.Run()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit)
				require.Equal(t, 18, exit.ExitCode())
				data, e := os.ReadFile(filepath.Join(dir, "body"))
				require.NoError(t, e)
				c, u, d := frameCounts(t, data)
				require.Equal(t, [3]int{3, 0, 0}, [3]int{c, u, d})
				require.Zero(t, f.stats(t, 1).Business)
				f.mode(t, 0, "normal")
				f.success(t, base)
				t.Log("actual curl18, 3 content, no usage/Done, zero backup, subsequent request succeeds")
			case "cancel":
				f.mode(t, 0, "hold")
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan failoverResponse, 1)
				go func() { done <- f.chat(ctx, base, f.user, "owned-b5", true) }()
				failoverWait(t, func() bool { return f.stats(t, 0).Active == 1 })
				cancel()
				require.Error(t, (<-done).Err)
				failoverWait(t, func() bool { return f.stats(t, 0).Active == 0 })
				require.Zero(t, f.stats(t, 1).Business)
			case "budget":
				f.mode(t, 0, "ttfb")
				f.mode(t, 1, "ttfb")
				start := time.Now()
				r := f.chat(context.Background(), base, f.user, "owned-b5", false)
				elapsed := time.Since(start)
				require.NoError(t, r.Err)
				require.Equal(t, 502, r.Status)
				require.Contains(t, string(r.Body), "gaigate_upstream_timeout")
				require.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
				require.Less(t, elapsed, 1450*time.Millisecond)
				require.Equal(t, 1, f.stats(t, 0).Business)
				require.Equal(t, 1, f.stats(t, 1).Business)
				t.Logf("logical Overall=1s, per-attempt TTFB=800ms, two candidates; elapsed=%s (not 2*TTFB)", elapsed)
			case "slow-reader":
				f.mode(t, 0, "bulk")
				conn, err := net.Dial("tcp", strings.TrimPrefix(base, "http://"))
				require.NoError(t, err)
				defer conn.Close()
				body := `{"model":"owned-b5","stream":true,"messages":[{"role":"user"}]}`
				_, err = fmt.Fprintf(conn, "POST /v1/chat/completions HTTP/1.1\r\nHost: fixture\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", f.user, len(body), body)
				require.NoError(t, err)
				failoverWait(t, func() bool { return f.stats(t, 0).Sent > 0 })
				failoverWait(t, func() bool { return f.stats(t, 0).Active == 0 })
				s := f.stats(t, 0)
				require.Less(t, s.Sent, 4096, "256MiB was not drained into a non-reading socket")
				require.Zero(t, f.stats(t, 1).Business)
				t.Logf("real non-reading client: upstream sent %d/4096 64KiB frames before cancellation; no backup", s.Sent)
			case "max-one":
				f.mode(t, 0, "429")
				r := f.chat(context.Background(), base, f.user, "owned-b5", false)
				require.NoError(t, r.Err)
				require.Equal(t, 502, r.Status)
				require.Equal(t, 1, f.stats(t, 0).Business)
				require.Zero(t, f.stats(t, 1).Business)
			}
			failoverWait(t, func() bool {
				for _, id := range []uint64{f.channels[0].ID, f.channels[1].ID} {
					n, e := f.redis.ZCard(context.Background(), fmt.Sprintf("gaigate:breaker:pending:{%d}", id)).Result()
					if e != nil || n != 0 {
						return false
					}
				}
				return true
			})
			f.privacy(t)
			require.NotContains(t, gateway.logs.String(), "WARNING: DATA RACE")
		})
	}
}

func TestFailoverRedisConcurrentUsersModelsAndPrivacy(t *testing.T) {
	f := newFailoverFixture(t)
	a, baseA := f.start(t, 2*time.Second, 3*time.Second, time.Second, 3)
	b, baseB := f.start(t, 2*time.Second, 3*time.Second, time.Second, 3)
	type identity struct {
		key string
		id  uint64
	}
	users := make([]identity, 12)
	secrets := []string{}
	for i := range users {
		name := fmt.Sprintf("b5-isolation-%d", i)
		users[i].key = gatewayUser(t, f.db, name)
		var row store.User
		require.NoError(t, f.db.Where("username = ?", name).First(&row).Error)
		users[i].id = row.ID
		secrets = append(secrets, users[i].key)
	}
	type observed struct {
		response failoverResponse
		model    string
		id       uint64
		p        *ownedProcess
	}
	results := make(chan observed, len(users)*4)
	for i, u := range users {
		go func(i int, u identity) {
			transport := http.DefaultTransport.(*http.Transport).Clone()
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 4 * time.Second}
			for j := 0; j < 4; j++ {
				model := "owned-b5"
				if (i+j)%2 == 1 {
					model = "owned-b5-other"
				}
				base, p := baseA, a
				if i%2 == 1 {
					base, p = baseB, b
				}
				results <- observed{f.chatUsing(context.Background(), client, base, u.key, model, false), model, u.id, p}
			}
		}(i, u)
	}
	for range len(users) * 4 {
		got := <-results
		require.NoError(t, got.response.Err)
		require.Equal(t, 200, got.response.Status)
		mapped := "mapped-primary"
		channel := f.channels[0].ID
		if got.model == "owned-b5-other" {
			mapped = "other-backup"
			channel = f.channels[1].ID
		}
		require.Contains(t, string(got.response.Body), mapped)
		found := 0
		for _, row := range got.p.logs.entries(t, got.response.ID) {
			if row["msg"] == "request_done" {
				found++
				require.EqualValues(t, got.id, row["user_id"])
				require.Equal(t, got.model, row["model"])
				require.EqualValues(t, channel, row["channel_id"])
			}
		}
		require.Equal(t, 1, found)
	}
	primary := f.stats(t, 0)
	require.Equal(t, 24, primary.Business)
	require.Equal(t, 24, primary.Models["mapped-primary"])
	require.Zero(t, primary.Models["other-primary"])
	backup := f.stats(t, 1)
	require.Equal(t, 24, backup.Business)
	require.Equal(t, 24, backup.Models["other-backup"])
	require.Zero(t, backup.Models["mapped-backup"], "client body/header cannot override either alias's configured chain")
	var usage int64
	require.NoError(t, f.db.Model(&store.UsageRecord{}).Count(&usage).Error)
	require.Zero(t, usage, "B5 does not settle usage")
	f.privacy(t, secrets...)
	t.Log("12 independent clients/users, two gateway processes, 48 requests/two aliases: mapped model, Key, principal and completion summary isolated; routing overrides ignored")
}

func runFailoverScript(t *testing.T, f *failoverFixture, a, b, control, owner, token, temp string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()
	source, err := os.ReadFile(filepath.Join(f.root, "test/e2e/failover.sh"))
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "failover.sh")
	require.NoError(t, os.WriteFile(path, bytes.ReplaceAll(source, []byte("\r\n"), []byte("\n")), 0600))
	cmd := exec.CommandContext(ctx, findBash(t), "--login", filepath.ToSlash(path), a, b, control)
	cmd.Dir = f.root
	cmd.Env = append(cleanProcessEnvironment(), "GAIGATE_E2E_OWNED_FIXTURE="+owner, "GAIGATE_E2E_API_KEY="+f.user, "GAIGATE_E2E_CONTROL_TOKEN="+token, "GAIGATE_E2E_MODEL=owned-b5", "TMPDIR="+filepath.ToSlash(temp))
	data, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "script must exit before outer watchdog")
	code := 0
	if err != nil {
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		code = exit.ExitCode()
	}
	for _, secret := range []string{f.token, f.user, f.keys[0], f.keys[1]} {
		require.NotContains(t, string(data), secret)
		require.NotContains(t, strings.Join(cmd.Args, " "), secret)
	}
	files, err := os.ReadDir(temp)
	require.NoError(t, err)
	require.Empty(t, files, "script-owned files must be removed after positive or negative exit")
	return code, string(data)
}
func TestFailoverRedisOwnedScriptAndNegativeWitnesses(t *testing.T) {
	for _, max := range []int{3, 1} {
		t.Run(fmt.Sprintf("attempts=%d", max), func(t *testing.T) {
			f := newFailoverFixture(t)
			_, a := f.start(t, 5*time.Second, 6*time.Second, time.Second, max)
			_, b := f.start(t, 5*time.Second, 6*time.Second, time.Second, max)
			for _, tc := range []struct {
				name, owner, token, control string
				want                        int
			}{{"unowned", "", f.token, f.control.URL, 2}, {"bad-control-token", "1", strings.Repeat("0", 64), f.control.URL, 1}, {"transport", "1", f.token, "http://" + processPort(t), 0}} {
				t.Run(tc.name, func(t *testing.T) {
					before, e := f.snapshot()
					require.NoError(t, e)
					code, output := runFailoverScript(t, f, a, b, tc.control, tc.owner, tc.token, t.TempDir())
					if tc.want != 0 {
						require.Equal(t, tc.want, code)
					} else {
						require.Contains(t, []int{7, 28}, code)
						require.Contains(t, output, fmt.Sprintf("curl=%d", code))
					}
					after, e := f.snapshot()
					require.NoError(t, e)
					require.Equal(t, before.Primary, after.Primary)
					require.Equal(t, before.Backup, after.Backup)
					t.Logf("expected negative script %s exit=%d, zero business traffic and no credential/file leak", tc.name, code)
				})
			}
			code, output := runFailoverScript(t, f, a, b, f.control.URL, "1", f.token, t.TempDir())
			if max == 3 {
				require.Zero(t, code, "B5 script: %s", output)
				require.Equal(t, 5, strings.Count(output, "[PASS]"))
				t.Logf("B5 actual owned script:\n%s", output)
			} else {
				require.Equal(t, 1, code)
				require.Contains(t, output, "[FAIL]")
				require.Zero(t, f.stats(t, 1).Business, "MaxAttempts=1 negative must fail at real fallback, not preparation")
				require.GreaterOrEqual(t, f.stats(t, 0).Business, 2)
				t.Log("expected negative MaxAttempts=1: script fails on HTTP502 after an actual 429 primary; zero backup")
			}
			f.privacy(t)
		})
	}
}
