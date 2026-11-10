package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This isolated control-plane simulation tests script ownership, not B5 routing.
// The held request uses a real curl and TCP connection, which must close before
// the fixture releases the hold or closes its server.
func checkFailoverScriptCleanup(t *testing.T, failure string, onHeld func(*exec.Cmd) func()) {
	t.Helper()
	var mu sync.Mutex
	primary, backup, active, mode, breaker := 0, 0, 0, "normal", "closed"
	entered, canceled, trigger, release := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = r.Body.Close()
		reply := func(code int, value any) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(value)
		}
		if r.URL.Path == "/mode" {
			var control struct{ Mode string }
			if json.Unmarshal(body, &control) != nil {
				reply(400, struct{}{})
				return
			}
			mu.Lock()
			mode = control.Mode
			mu.Unlock()
			reply(200, struct{}{})
			return
		}
		mu.Lock()
		if r.URL.Path == "/state" {
			state := struct {
				State    string `json:"state"`
				Primary  int    `json:"primary"`
				Backup   int    `json:"backup"`
				Active   int    `json:"active"`
				Eligible bool   `json:"eligible"`
				Health   bool   `json:"health"`
			}{breaker, primary, backup, active, true, true}
			mu.Unlock()
			if state.Active == 1 {
				select {
				case <-trigger:
				case <-release:
					return
				}
				if failure == "state" {
					reply(500, struct{}{})
					return
				}
			}
			reply(200, state)
			return
		}
		if active == 1 {
			mu.Unlock()
			select {
			case <-trigger:
			case <-release:
				return
			}
			code := 200 // A missing backup increment must fail the script assertion.
			if failure == "request" {
				code = 500
			}
			reply(code, struct{}{})
			return
		}
		if mode == "429" && breaker == "open" {
			backup++
		} else {
			primary++
			if mode == "429" {
				backup++
				if primary == 3 {
					breaker = "open"
				}
			}
		}
		if mode == "hold" {
			active, breaker = 1, "half-open"
			mu.Unlock()
			close(entered)
			select {
			case <-r.Context().Done():
				close(canceled)
			case <-release:
			}
			return
		}
		mu.Unlock()
		reply(200, struct{}{})
	}))
	// Release the held handler before Server.Close, including failed assertions.
	t.Cleanup(func() { close(release); server.CloseClientConnections(); server.Close() })

	root, err := filepath.Abs("../..")
	require.NoError(t, err)
	source, err := os.ReadFile(filepath.Join(root, "test/e2e/failover.sh"))
	require.NoError(t, err)
	script := filepath.Join(t.TempDir(), "failover.sh")
	require.NoError(t, os.WriteFile(script, bytes.ReplaceAll(source, []byte("\r\n"), []byte("\n")), 0600))
	temp := t.TempDir()
	log, err := os.Create(filepath.Join(t.TempDir(), "script.log"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, findBash(t), "--login", filepath.ToSlash(script), server.URL, server.URL, server.URL)
	user, token := "sk-"+strings.Repeat("a", 43), strings.Repeat("b", 64)
	cmd.Env = append(cleanProcessEnvironment(), "GAIGATE_E2E_OWNED_FIXTURE=1", "GAIGATE_E2E_API_KEY="+user,
		"GAIGATE_E2E_CONTROL_TOKEN="+token, "GAIGATE_E2E_MODEL=owned-b5", "TMPDIR="+filepath.ToSlash(temp))
	cmd.Stdout, cmd.Stderr = log, log
	require.NoError(t, cmd.Start())
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		cancel()
		// Wait has exactly one owner even if a precondition fails.
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("script did not exit after fixture cancellation")
		}
	})
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("script never entered the real held curl request")
	}
	verify := func() {}
	if onHeld != nil {
		verify = onHeld(cmd)
	}
	close(trigger)
	var exit *exec.ExitError
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("script did not promptly exit after the held-probe failure")
	}
	require.ErrorAs(t, waitErr, &exit)
	want := 1
	if failure == "signal" {
		want = 143
	}
	assert.Equal(t, want, exit.ExitCode())
	select {
	case <-canceled:
	case <-time.After(500 * time.Millisecond):
		t.Error("held curl TCP connection survived script exit, before fixture cleanup")
	}
	verify()
	files, err := os.ReadDir(temp)
	require.NoError(t, err)
	assert.Empty(t, files)
	output, err := os.ReadFile(log.Name())
	require.NoError(t, err)
	assert.Equal(t, 3, strings.Count(string(output), "[PASS]"))
	assert.NotContains(t, string(output), user)
	assert.NotContains(t, string(output), token)
	assert.NotContains(t, strings.Join(cmd.Args, " "), user)
	assert.NotContains(t, strings.Join(cmd.Args, " "), token)
	if !t.Failed() {
		t.Logf("held probe then %s: exit=%d, TCP canceled before fixture cleanup, temporary files empty", failure, exit.ExitCode())
	}
}

func TestFailoverScriptCancelsHeldProbeOnFailure(t *testing.T) {
	for _, failure := range []string{"request", "state", "assertion"} {
		t.Run(failure, func(t *testing.T) { checkFailoverScriptCleanup(t, failure, nil) })
	}
}
