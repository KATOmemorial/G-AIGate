package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/access"
	"gaigate/internal/store"
)

type ownedProcess struct {
	cmd      *exec.Cmd
	logs     *gatewayLogs
	done     chan struct{}
	err      error // read only after done
	stopOnce sync.Once
	stopped  bool
}

func startOwnedProcess(t *testing.T, root, binary string, env []string, args ...string) *ownedProcess {
	t.Helper()
	p := &ownedProcess{cmd: exec.Command(binary, args...), logs: newGatewayLogs(), done: make(chan struct{})}
	p.cmd.Dir, p.cmd.Env, p.cmd.Stdout, p.cmd.Stderr = root, env, p.logs, p.logs
	require.NoError(t, p.cmd.Start(), "cannot start owned process")
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(func() { p.stop(t) })
	return p
}

func (p *ownedProcess) stop(t *testing.T) {
	t.Helper()
	p.stopOnce.Do(func() {
		select {
		case <-p.done:
			return
		default:
		}
		if runtime.GOOS == "windows" {
			require.NoError(t, p.cmd.Process.Kill())
			<-p.done
			t.Log("owned process joined after Windows forced cleanup; no Unix signal claim")
		} else {
			require.NoError(t, p.cmd.Process.Signal(syscall.SIGTERM))
			select {
			case <-p.done:
				assert.NoError(t, p.err, "owned child graceful exit")
			case <-time.After(7 * time.Second):
				_ = p.cmd.Process.Kill()
				<-p.done
				t.Error("owned child did not exit within shutdown budget")
			}
		}
		p.stopped = true
	})
}

func waitProcessHealth(t *testing.T, p *ownedProcess, base string) {
	t.Helper()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	client := &http.Client{Timeout: 250 * time.Millisecond}
	for {
		resp, err := client.Get(base + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		select {
		case <-p.done:
			t.Fatalf("owned process exited before healthz: %v; logs: %s", p.err, p.logs.String())
		case <-deadline.C:
			t.Fatal("owned process healthz watchdog expired")
		case <-tick.C:
		}
	}
}

func processPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())
	return addr
}

func buildProcess(t *testing.T, root, dir, pkg string) string {
	t.Helper()
	name := filepath.Base(pkg)
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(dir, name)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	args := []string{"build"}
	if gatewayProcessRace {
		args = append(args, "-race")
	}
	args = append(args, "-o", path, pkg)
	command := exec.CommandContext(ctx, "go", args...)
	command.Dir = root
	data, err := command.CombinedOutput()
	require.NoError(t, err, "build failed: %s", data)
	return path
}

func processOptions(t *testing.T) store.Options {
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
	}
	return store.Options{Host: host, Port: port, User: "root", Password: password, Database: "gaigate_test"}
}

func recreateProcessDatabase(t *testing.T, opts store.Options) {
	t.Helper()
	require.Equal(t, "gaigate_test", opts.Database)
	opts.Database = ""
	db, err := sql.Open("mysql", opts.DSN())
	require.NoError(t, err)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx), "M3 requires real MySQL")
	for _, statement := range []string{"DROP DATABASE IF EXISTS gaigate_test", "CREATE DATABASE gaigate_test"} {
		_, err = db.ExecContext(ctx, statement)
		require.NoError(t, err)
	}
	// No migrations or seed here: the actual gateway process must do both.
}

func cleanProcessEnvironment() []string {
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		upper := strings.ToUpper(name)
		if !strings.HasPrefix(upper, "GAIGATE_") && upper != "MOCK_ADDR" {
			env = append(env, entry)
		}
	}
	return env
}

type gatewayProcessFixture struct {
	root, dir, gatewayBinary, mockBinary, keyBinary string
	opts                                            store.Options
	master                                          []byte
	mock                                            *ownedProcess
	mockURL                                         string
	redis                                           *miniredis.Miniredis
	db                                              *gorm.DB
	gateways                                        []*ownedProcess
	client                                          *http.Client
}

func newProcessFixture(t *testing.T) *gatewayProcessFixture {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	require.NoError(t, err)
	f := &gatewayProcessFixture{root: root, dir: t.TempDir(), opts: processOptions(t), master: bytes.Repeat([]byte{6}, 32), client: &http.Client{Timeout: 12 * time.Second}}
	f.gatewayBinary = buildProcess(t, root, f.dir, "./cmd/g-aigate")
	f.mockBinary = buildProcess(t, root, f.dir, "./cmd/mockprovider")
	f.keyBinary = buildProcess(t, root, f.dir, "./cmd/g-aigate-key")
	t.Logf("actual gateway/mock/key child builds: race=%t; OS=%s", gatewayProcessRace, runtime.GOOS)
	recreateProcessDatabase(t, f.opts)
	f.redis = miniredis.RunT(t)
	addr := processPort(t)
	f.mockURL = "http://" + addr
	f.mock = startOwnedProcess(t, root, f.mockBinary, append(cleanProcessEnvironment(), "MOCK_ADDR="+addr))
	waitProcessHealth(t, f.mock, f.mockURL)
	return f
}

func (f *gatewayProcessFixture) environment() []string {
	env := cleanProcessEnvironment()
	return append(env, "GAIGATE_MYSQL_HOST="+f.opts.Host, "GAIGATE_MYSQL_PORT="+strconv.Itoa(f.opts.Port), "GAIGATE_MYSQL_USER="+f.opts.User,
		"GAIGATE_MYSQL_PASSWORD="+f.opts.Password, "GAIGATE_MYSQL_DATABASE=gaigate_test", "GAIGATE_SECRET_MASTER_KEY="+base64.StdEncoding.EncodeToString(f.master),
		"GAIGATE_ADMIN_PASSWORD=owned-process-fixture", "GAIGATE_REDIS_ADDR="+f.redis.Addr())
}

func (f *gatewayProcessFixture) startGateway(t *testing.T, ttfb, overall time.Duration) (*ownedProcess, string) {
	return f.startGatewayWithProbeTimeout(t, ttfb, overall, 5*time.Second)
}

func (f *gatewayProcessFixture) startGatewayWithProbeTimeout(t *testing.T, ttfb, overall, probe time.Duration) (*ownedProcess, string) {
	t.Helper()
	addr := processPort(t)
	path := filepath.Join(f.dir, fmt.Sprintf("gateway-%d.yaml", len(f.gateways)))
	source := fmt.Sprintf("server:\n  addr: %q\n  shutdown-timeout: 2s\nproxy:\n  ttfb-timeout: %s\n  overall-timeout: %s\n  stream-buffer: 1\n", addr, ttfb, overall)
	source += fmt.Sprintf("channel:\n  probe-timeout: %s\n", probe)
	require.NoError(t, os.WriteFile(path, []byte(source), 0600))
	p := startOwnedProcess(t, f.root, f.gatewayBinary, f.environment(), "-config", path)
	f.gateways = append(f.gateways, p)
	base := "http://" + addr
	waitProcessHealth(t, p, base)
	if f.db == nil {
		var err error
		f.db, err = store.Open(f.opts)
		require.NoError(t, err)
		t.Cleanup(func() { pool, err := f.db.DB(); require.NoError(t, err); require.NoError(t, pool.Close()) })
	}
	var primary store.Channel
	require.NoError(t, f.db.Where("name = ?", "demo-primary").First(&primary).Error)
	primary.BaseURL = f.mockURL
	require.NoError(t, store.NewChannelRepo(f.db, store.NewConfigCache(0)).Update(context.Background(), &primary))
	actual, err := store.NewChannelRepo(f.db, store.NewConfigCache(0)).Get(context.Background(), int64(primary.ID))
	require.NoError(t, err)
	assert.Equal(t, f.mockURL, actual.BaseURL)
	var count int64
	require.NoError(t, f.db.Model(&store.APIKey{}).Count(&count).Error)
	t.Logf("gateway startup migrations/seed completed; test primary URL verified; existing API Key rows=%d", count)
	resp, _, readErr := gatewayRequest(t, f.client, base, "", "GET", "/readyz", "")
	require.NoError(t, readErr)
	assert.Equal(t, 200, resp.StatusCode)
	return p, base
}

func (f *gatewayProcessFixture) issueKey(t *testing.T) access.CreatedKey {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, f.keyBinary, "-config", filepath.Join(f.dir, "missing-cli.yaml"), "-user", "admin", "-name", "owned-m3")
	command.Dir, command.Env = f.root, f.environment()
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	require.NoError(t, ctx.Err(), "CLI must exit on its own budget")
	require.NoError(t, err, "CLI failed without exposing stdout")
	require.Empty(t, stderr.String())
	var created access.CreatedKey
	decoder := json.NewDecoder(&stdout)
	require.NoError(t, decoder.Decode(&created))
	var trailing any
	assert.ErrorIs(t, decoder.Decode(&trailing), io.EOF)
	require.True(t, created.ID > 0 && len(created.Key) == 46 && strings.HasPrefix(created.Key, "sk-"), "CLI must return one complete created Key")
	return created
}

func processLogEntries(t *testing.T, logs *gatewayLogs) []map[string]any {
	t.Helper()
	var entries []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry))
		entries = append(entries, entry)
	}
	return entries
}

func waitProcessLog(t *testing.T, logs *gatewayLogs, offset int, match func(map[string]any) bool) map[string]any {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		entries := processLogEntries(t, logs)
		for _, entry := range entries[min(offset, len(entries)):] {
			if match(entry) {
				return entry
			}
		}
		select {
		case <-logs.notify:
		case <-timer.C:
			t.Fatal("owned process did not emit expected lifecycle/request evidence")
			return nil
		}
	}
}

func findBash(t *testing.T) string {
	t.Helper()
	name := "bash"
	if runtime.GOOS == "windows" {
		name = filepath.Join(os.Getenv("ProgramFiles"), "Git", "bin", "bash.exe")
	}
	path, err := exec.LookPath(name)
	require.NoError(t, err, "M3 requires Bash")
	return path
}

func runM0Script(t *testing.T, f *gatewayProcessFixture, base, raw, temp string) (int, string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, findBash(t), "--login", filepath.ToSlash(filepath.Join(f.root, "test", "e2e", "m0_gateway.sh")), base, f.mockURL)
	command.Dir = f.root
	command.Env = append(cleanProcessEnvironment(), "GAIGATE_E2E_API_KEY="+raw, "TMPDIR="+filepath.ToSlash(temp))
	var out, errOut bytes.Buffer
	command.Stdout, command.Stderr = &out, &errOut
	err := command.Run()
	require.NoError(t, ctx.Err(), "script watchdog cannot count as its own exit")
	code := 0
	if err != nil {
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		code = exit.ExitCode()
	}
	return code, out.String(), errOut.String()
}
