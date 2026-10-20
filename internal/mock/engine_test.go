package mock_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gaigate/internal/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// §5⑦: exact defaults, absent-key reads, precedence, and key isolation.
func TestEngineDefaultsAndPrecedence(t *testing.T) {
	wantDefaults := mock.ScenarioParams{
		Chunks: 8, ChunkInterval: 50 * time.Millisecond, TTFBDelay: 2 * time.Second,
		DowntimeMode: "503", FailAfterChunks: 3, PromptTokens: 12, CompletionTokens: 34,
	}
	require.Equal(t, wantDefaults, mock.DefaultScenarioParams())
	e := mock.NewEngine()
	require.NotNil(t, e.Snapshot())
	require.Empty(t, e.Snapshot())

	s, p := e.Get("sk-mock-absent")
	assert.Equal(t, mock.Normal, s)
	assert.Equal(t, wantDefaults, p)
	s, p, err := e.Resolve("sk-mock-absent", "")
	require.NoError(t, err)
	assert.Equal(t, mock.Normal, s)
	assert.Equal(t, wantDefaults, p)
	s, p, err = e.Resolve("sk-mock-absent", "slow")
	require.NoError(t, err)
	assert.Equal(t, mock.Slow, s)
	assert.Equal(t, wantDefaults, p)
	assert.Empty(t, e.Snapshot(), "reads and header overrides must not insert keys")

	a := wantDefaults
	a.Chunks, a.TTFBDelay = 9, 5*time.Second
	b := wantDefaults
	b.ChunkInterval, b.PromptTokens = time.Nanosecond, 99
	require.NoError(t, e.Set("sk-mock-a", mock.Slow, a))
	require.NoError(t, e.Set("sk-mock-b", mock.RateLimited, b))
	before := e.Snapshot()
	for _, tc := range []struct {
		key, header string
		scenario    mock.Scenario
		params      mock.ScenarioParams
	}{
		{"sk-mock-a", "", mock.Slow, a},
		{"sk-mock-b", "", mock.RateLimited, b},
		{"sk-mock-a", "normal", mock.Normal, a},
		{"sk-mock-a", "interrupted", mock.Interrupted, a},
		{"sk-mock-b", "downtime", mock.Downtime, b},
		{"sk-mock-unknown", "", mock.Normal, wantDefaults},
	} {
		t.Run(tc.key+"/"+tc.header, func(t *testing.T) {
			s, p, err := e.Resolve(tc.key, tc.header)
			require.NoError(t, err)
			assert.Equal(t, tc.scenario, s)
			assert.Equal(t, tc.params, p, "override must preserve all parameters")
		})
	}
	for _, header := range []string{"unknown", "NORMAL", " slow", "slow "} {
		_, _, err := e.Resolve("sk-mock-a", header)
		require.ErrorContains(t, err, "unknown scenario")
	}
	assert.Equal(t, before, e.Snapshot(), "header resolution must never write state")
}

// §5⑦: all exported snapshots/default values are owned by the caller.
func TestEngineReturnedValuesAreIndependent(t *testing.T) {
	e := mock.NewEngine()
	p := mock.DefaultScenarioParams()
	require.NoError(t, e.Set("sk-mock-a", mock.Slow, p))
	want := mock.ScenarioConfig{Scenario: mock.Slow, Params: p}
	p.Chunks = 100 // Set also stores a value, not caller-owned mutable state.
	_, got := e.Get("sk-mock-a")
	got.Chunks = 200
	_, resolved, err := e.Resolve("sk-mock-a", "normal")
	require.NoError(t, err)
	resolved.PromptTokens = 999
	snapshot := e.Snapshot()
	snapshot["sk-mock-a"] = mock.ScenarioConfig{Scenario: mock.Downtime, Params: got}
	snapshot["injected"] = mock.ScenarioConfig{Scenario: mock.Normal, Params: resolved}
	delete(snapshot, "sk-mock-a")
	assert.Equal(t, map[string]mock.ScenarioConfig{"sk-mock-a": want}, e.Snapshot())
	s, actual := e.Get("sk-mock-a")
	assert.Equal(t, want, mock.ScenarioConfig{Scenario: s, Params: actual})

	defaults := mock.DefaultScenarioParams()
	defaults.Chunks = 1
	_, missing := e.Get("not-configured")
	missing.Chunks = 2
	assert.Equal(t, 8, mock.DefaultScenarioParams().Chunks)
	s, actual = e.Get("not-configured")
	assert.Equal(t, mock.Normal, s)
	assert.Equal(t, mock.DefaultScenarioParams(), actual)
}

// §3.2 / §5⑧: every invalid Set preserves the entire map, for both a new key
// and an existing key. Even parameters irrelevant to the scenario are checked.
func TestEngineInvalidSetLeavesStateUnchanged(t *testing.T) {
	cases := []struct {
		name   string
		change func(*mock.ScenarioParams)
		reason string
	}{
		{"zero chunks", func(p *mock.ScenarioParams) { p.Chunks = 0 }, "chunks"},
		{"negative chunks", func(p *mock.ScenarioParams) { p.Chunks = -1 }, "chunks"},
		{"negative interval", func(p *mock.ScenarioParams) { p.ChunkInterval = -time.Nanosecond }, "chunk_interval"},
		{"negative delay", func(p *mock.ScenarioParams) { p.TTFBDelay = -time.Nanosecond }, "ttfb_delay"},
		{"empty downtime", func(p *mock.ScenarioParams) { p.DowntimeMode = "" }, "downtime_mode"},
		{"unknown downtime", func(p *mock.ScenarioParams) { p.DowntimeMode = "500" }, "downtime_mode"},
		{"negative fail count", func(p *mock.ScenarioParams) { p.FailAfterChunks = -1 }, "fail_after_chunks"},
		{"negative prompt", func(p *mock.ScenarioParams) { p.PromptTokens = -1 }, "prompt_tokens"},
		{"negative completion", func(p *mock.ScenarioParams) { p.CompletionTokens = -1 }, "completion_tokens"},
		{"token sum overflow", func(p *mock.ScenarioParams) {
			p.PromptTokens, p.CompletionTokens = int(^uint(0)>>1), 1
		}, "total_tokens"},
	}
	for _, s := range []mock.Scenario{mock.Normal, mock.Slow, mock.Downtime, mock.RateLimited, mock.Interrupted} {
		for _, tc := range cases {
			t.Run(string(s)+"/"+tc.name, func(t *testing.T) {
				e := mock.NewEngine()
				require.NoError(t, e.Set("sk-mock-existing", mock.Slow, mock.DefaultScenarioParams()))
				before := e.Snapshot()
				p := mock.DefaultScenarioParams()
				tc.change(&p)
				for _, key := range []string{"sk-mock-new", "sk-mock-existing"} {
					err := e.Set(key, s, p)
					require.ErrorContains(t, err, tc.reason)
					assert.NotContains(t, err.Error(), key)
					assert.Equal(t, before, e.Snapshot())
				}
			})
		}
	}
	for _, s := range []mock.Scenario{"", "unknown", "Normal", "normal "} {
		t.Run("scenario/"+string(s), func(t *testing.T) {
			e := mock.NewEngine()
			require.NoError(t, e.Set("sk-mock-existing", mock.Normal, mock.DefaultScenarioParams()))
			before := e.Snapshot()
			for _, key := range []string{"sk-mock-new", "sk-mock-existing"} {
				require.ErrorContains(t, e.Set(key, s, mock.DefaultScenarioParams()), "unknown scenario")
				assert.Equal(t, before, e.Snapshot())
			}
		})
	}
	e := mock.NewEngine()
	require.Error(t, e.Set("sk-mock-zero", mock.Normal, mock.ScenarioParams{}))
	assert.Empty(t, e.Snapshot(), "zero struct must not be silently defaulted")
}

func TestEngineKeyValidation(t *testing.T) {
	e := mock.NewEngine()
	for _, key := range []string{"sk-mock-a", "sk-mock-b", "a", "测试标识", "sk-Mock_123!"} {
		require.NoError(t, e.Set(key, mock.Normal, mock.DefaultScenarioParams()))
	}
	before := e.Snapshot()
	for _, key := range []string{
		"", " ", " sk-mock-a", "sk-mock-a ", "sk mock", "sk\tmock", "sk\nmock",
		"sk\rmock", "sk\x00mock", "sk\x1fmock", "sk\x7fmock", "sk\u0085mock", "sk\u00a0mock", "sk\u2003mock",
	} {
		t.Run(fmt.Sprintf("%q", key), func(t *testing.T) {
			err := e.Set(key, mock.Normal, mock.DefaultScenarioParams())
			require.ErrorContains(t, err, "test key")
			if len(key) > 1 {
				assert.NotContains(t, err.Error(), key)
			}
			_, _, err = e.Resolve(key, "")
			require.ErrorContains(t, err, "test key")
			assert.Equal(t, before, e.Snapshot(), "reject keys without trimming or changing state")
		})
	}
}

// §5⑧: explicit zeros, submillisecond durations and maximum safe int sums.
func TestEngineCompleteParametersRoundTrip(t *testing.T) {
	const maxInt = int(^uint(0) >> 1)
	cases := []struct {
		name string
		p    mock.ScenarioParams
	}{
		{"explicit zeros", mock.ScenarioParams{Chunks: 1, DowntimeMode: "503"}},
		{"submillisecond", mock.ScenarioParams{
			Chunks: 2, ChunkInterval: 500 * time.Microsecond, TTFBDelay: 1500 * time.Microsecond,
			DowntimeMode: "refuse", FailAfterChunks: 1, PromptTokens: 0, CompletionTokens: 1,
		}},
		{"nanoseconds", mock.ScenarioParams{
			Chunks: maxInt, ChunkInterval: time.Nanosecond, TTFBDelay: time.Nanosecond,
			DowntimeMode: "503", FailAfterChunks: maxInt, PromptTokens: maxInt - 1, CompletionTokens: 1,
		}},
		{"max prompt", mock.ScenarioParams{Chunks: 1, DowntimeMode: "503", PromptTokens: maxInt}},
		{"max completion", mock.ScenarioParams{Chunks: 1, DowntimeMode: "refuse", CompletionTokens: maxInt}},
		{"max duration", mock.ScenarioParams{
			Chunks: 1, DowntimeMode: "503", ChunkInterval: time.Duration(1<<63 - 1), TTFBDelay: time.Duration(1<<63 - 1),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := mock.NewEngine()
			require.NoError(t, e.Set("sk-mock-a", mock.Slow, mock.DefaultScenarioParams()))
			require.NoError(t, e.Set("sk-mock-a", mock.Normal, tc.p))
			s, p := e.Get("sk-mock-a")
			assert.Equal(t, mock.Normal, s)
			assert.Equal(t, tc.p, p, "replacement must preserve every explicit zero and duration bit")
			s, p, err := e.Resolve("sk-mock-a", "slow")
			require.NoError(t, err)
			assert.Equal(t, mock.Slow, s)
			assert.Equal(t, tc.p, p)
		})
	}
}

func TestEngineInterruptedBoundsAndFinalOverrideValidation(t *testing.T) {
	for _, tc := range []struct {
		chunks, fail int
		valid        bool
	}{
		{1, 0, false}, {1, 1, false}, {2, 0, false}, {2, 1, true},
		{8, 1, true}, {8, 7, true}, {8, 8, false}, {8, 9, false},
	} {
		t.Run(fmt.Sprintf("n=%d/k=%d", tc.chunks, tc.fail), func(t *testing.T) {
			e := mock.NewEngine()
			p := mock.DefaultScenarioParams()
			p.Chunks, p.FailAfterChunks = tc.chunks, tc.fail
			// The extra range restriction applies only to the final scenario.
			for _, s := range []mock.Scenario{mock.Normal, mock.Slow, mock.Downtime, mock.RateLimited} {
				require.NoError(t, e.Set("sk-mock-a", s, p))
			}
			before := e.Snapshot()
			s, got, err := e.Resolve("sk-mock-a", "interrupted")
			if tc.valid {
				require.NoError(t, err)
				assert.Equal(t, mock.Interrupted, s)
				assert.Equal(t, p, got)
				require.NoError(t, e.Set("sk-mock-b", mock.Interrupted, p))
				assert.Equal(t, mock.ScenarioConfig{Scenario: mock.Interrupted, Params: p}, e.Snapshot()["sk-mock-b"])
			} else {
				require.ErrorContains(t, err, "1 <= fail_after_chunks < chunks")
				for _, key := range []string{"sk-mock-a", "sk-mock-b"} {
					require.Error(t, e.Set(key, mock.Interrupted, p))
				}
				assert.Equal(t, before, e.Snapshot())
			}
		})
	}
}

// Engine portion of §5⑩: channel barriers establish resolve -> Set completion ->
// new resolve -> old consumer resumes, without sleeps or holding engine locks.
// The actual HTTP slow/stream snapshot cases belong to milestone ②.
func TestEngineResolvedSnapshotSurvivesSet(t *testing.T) {
	e := mock.NewEngine()
	old := mock.ScenarioConfig{Scenario: mock.Slow, Params: mock.DefaultScenarioParams()}
	require.NoError(t, e.Set("sk-mock-a", old.Scenario, old.Params))
	captured := make(chan mock.ScenarioConfig, 1)
	resume := make(chan struct{})
	finished := make(chan mock.ScenarioConfig, 1)
	var release sync.Once
	defer release.Do(func() { close(resume) })
	go func() {
		s, p, err := e.Resolve("sk-mock-a", "")
		if !assert.NoError(t, err) {
			return
		}
		c := mock.ScenarioConfig{Scenario: s, Params: p}
		captured <- c
		<-resume
		finished <- c
	}()
	select {
	case c := <-captured:
		require.Equal(t, old, c)
	case <-time.After(2 * time.Second):
		t.Fatal("old consumer did not resolve")
	}
	next := mock.ScenarioConfig{Scenario: mock.Downtime, Params: mock.DefaultScenarioParams()}
	next.Params.Chunks, next.Params.TTFBDelay = 2, 0
	updated := make(chan error, 1)
	go func() { updated <- e.Set("sk-mock-a", next.Scenario, next.Params) }()
	select {
	case err := <-updated:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Set blocked on an already resolved consumer")
	}
	s, p, err := e.Resolve("sk-mock-a", "")
	require.NoError(t, err)
	assert.Equal(t, next, mock.ScenarioConfig{Scenario: s, Params: p})
	release.Do(func() { close(resume) })
	select {
	case c := <-finished:
		assert.Equal(t, old, c, "already resolved consumer keeps the full old snapshot")
	case <-time.After(2 * time.Second):
		t.Fatal("old consumer did not finish")
	}
}

// §5⑪: simultaneous full replacements and all read methods must return one
// complete committed value, including the scenario name. Also mutate snapshot
// maps concurrently to expose accidental sharing under -race.
func TestEngineConcurrentCompleteSnapshots(t *testing.T) {
	e := mock.NewEngine()
	a := mock.ScenarioConfig{Scenario: mock.Normal, Params: mock.DefaultScenarioParams()}
	b := mock.ScenarioConfig{Scenario: mock.Interrupted, Params: mock.ScenarioParams{
		Chunks: 17, ChunkInterval: 123 * time.Nanosecond, TTFBDelay: 456 * time.Microsecond,
		DowntimeMode: "refuse", FailAfterChunks: 7, PromptTokens: 89, CompletionTokens: 101,
	}}
	keys := []string{"sk-mock-a", "sk-mock-b"}
	for _, key := range keys {
		require.NoError(t, e.Set(key, a.Scenario, a.Params))
	}
	check := func(c mock.ScenarioConfig) error {
		if c != a && c != b {
			return fmt.Errorf("mixed or uncommitted configuration: %+v", c)
		}
		return nil
	}
	const workers = 16
	var ready sync.WaitGroup
	ready.Add(workers)
	start := make(chan struct{})
	var group errgroup.Group
	for worker := range workers {
		group.Go(func() error {
			ready.Done()
			<-start
			for i := range 500 {
				key := keys[i%len(keys)]
				switch worker % 4 {
				case 0:
					c := a
					if i%2 == (worker/4)%2 {
						c = b
					}
					if err := e.Set(key, c.Scenario, c.Params); err != nil {
						return err
					}
				case 1:
					s, p := e.Get(key)
					if err := check(mock.ScenarioConfig{Scenario: s, Params: p}); err != nil {
						return err
					}
				case 2:
					s, p, err := e.Resolve(key, "")
					if err != nil {
						return err
					}
					if err := check(mock.ScenarioConfig{Scenario: s, Params: p}); err != nil {
						return err
					}
					s, p, err = e.Resolve(key, "slow")
					if err != nil {
						return err
					}
					if s != mock.Slow || (p != a.Params && p != b.Params) {
						return fmt.Errorf("override mixed parameters: %s %+v", s, p)
					}
				case 3:
					snapshot := e.Snapshot()
					if len(snapshot) != len(keys) {
						return fmt.Errorf("snapshot changed key count: %d", len(snapshot))
					}
					for _, expectedKey := range keys {
						if err := check(snapshot[expectedKey]); err != nil {
							return err
						}
					}
					snapshot[key] = mock.ScenarioConfig{}
					delete(snapshot, keys[(i+1)%len(keys)])
				}
			}
			return nil
		})
	}
	ready.Wait()
	close(start)
	require.NoError(t, group.Wait())
	for _, c := range e.Snapshot() {
		require.NoError(t, check(c))
	}
}
