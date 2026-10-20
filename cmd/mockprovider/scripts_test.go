package main

import (
	"bytes"
	"context"
	"io"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gaigate/internal/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Run the delivered script against the real entrypoint HTTP assembly with
// nondefault fixtures. The script cannot reset unrelated records or rely on
// initially absent seed state. Only this isolated test Engine owns the fixtures;
// no user service, database or real seed configuration is changed.
func TestScenarioScriptPreservesUnrelatedState(t *testing.T) {
	bash := "bash"
	if runtime.GOOS == "windows" {
		bash = filepath.Join(os.Getenv("ProgramFiles"), "Git", "bin", "bash.exe")
	}
	bash, err := exec.LookPath(bash)
	require.NoError(t, err, "script acceptance requires Bash (Git Bash on Windows)")
	script, err := filepath.Abs(filepath.Join("..", "..", "test", "e2e", "mock_scenarios.sh"))
	require.NoError(t, err)
	e := mock.NewEngine()
	for i, key := range []string{"sk-mock-a", "sk-mock-b", "sk-other-caller"} {
		params := mock.DefaultScenarioParams()
		params.Chunks, params.PromptTokens, params.TTFBDelay = 13+i, 71+i, 5*time.Second
		require.NoError(t, e.Set(key, mock.Slow, params))
	}
	before := e.Snapshot()
	s := httptest.NewServer(newHandler(e, mock.NewLogger(io.Discard)))
	defer s.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, bash, "--login", filepath.ToSlash(script), s.URL)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err = command.Run()
	require.NoError(t, err, "script failed: %s\n%s", stdout.String(), stderr.String())
	assert.Empty(t, stderr.String())
	assert.Equal(t, 5, bytes.Count(stdout.Bytes(), []byte("[PASS]")))
	assert.Regexp(t, `interrupted .*curl=(18|56) content=3 usage=0 DONE=0`, stdout.String())
	after := e.Snapshot()
	for _, key := range []string{"sk-mock-e2e-a", "sk-mock-e2e-b"} {
		assert.Equal(t, mock.ScenarioConfig{Scenario: mock.Normal, Params: mock.DefaultScenarioParams()}, after[key])
		delete(after, key)
		assert.NotContains(t, stdout.String()+stderr.String(), key)
	}
	assert.Equal(t, before, after, "seed and all unrelated caller parameters must remain unchanged")
	t.Logf("five scenarios and fixture isolation passed:\n%s", stdout.String())
}
