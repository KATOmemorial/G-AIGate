package channel

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/store"

	"github.com/stretchr/testify/require"
)

func TestHealthProbeGenerationInterleavings(t *testing.T) {
	for _, event := range []string{"same_generation", "ordinary_failure", "ordinary_success", "new_disable", "recovered"} {
		t.Run(event, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				now := time.Unix(100, 0)
				state := &healthMemory{}
				h := healthForTest(t, state, func() time.Time { return now }, nil, healthPolicy{failureThreshold: 1})
				ctx := context.Background()
				fingerprint := testFingerprint()
				first, eligible, err := h.beginProbe(ctx, 1, fingerprint)
				require.NoError(t, err)
				require.True(t, eligible)
				second := first
				entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var staleCommitted bool
				var staleErr error
				go func() {
					defer close(finished)
					close(entered) // actual provider-call phase in this controlled fixture
					<-release
					staleCommitted, staleErr = h.finishProbe(ctx, 1, first, ProbeResult{OK: true}, nil, now,
						func(context.Context) (probeFingerprint, error) { return fingerprint, nil })
				}()
				<-entered
				switch event {
				case "same_generation":
					committed, err := h.finishProbe(ctx, 1, second, ProbeResult{OK: false}, nil, now,
						func(context.Context) (probeFingerprint, error) { return fingerprint, nil })
					require.NoError(t, err)
					require.True(t, committed)
				case "ordinary_failure", "new_disable":
					if event == "ordinary_failure" {
						h.policy.failureThreshold = 5
					}
					h.ReportRequestResult(ctx, 1, false)
				case "ordinary_success":
					h.ReportRequestResult(ctx, 1, true)
				case "recovered":
					h.ReportRequestResult(ctx, 1, false)
					now = time.Unix(160, 0)
					completeHealthProbe(t, h, state, true, now)
				}
				before, writes, reads, cas := state.record, state.writes, state.loads, state.cas
				close(release)
				<-finished
				require.NoError(t, staleErr)
				require.False(t, staleCommitted)
				require.Equal(t, before, state.record)
				require.Equal(t, writes, state.writes)
				require.Equal(t, reads, state.loads, "old probe must not reload/replay on new health state")
				require.Equal(t, cas+1, state.cas, "old probe must attempt its original CAS only once")
			})
		})
	}
}

func TestHealthProbeConfigurationGenerationAndValueCopies(t *testing.T) {
	for _, change := range []struct {
		name string
		edit func(*probeFingerprint)
	}{
		{"enabled", func(f *probeFingerprint) { f.enabled = false }},
		{"provider", func(f *probeFingerprint) { f.provider = "other" }},
		{"url", func(f *probeFingerprint) { f.baseURL = "http://changed.invalid" }},
		{"model", func(f *probeFingerprint) { f.mapping.Model = "changed" }},
		{"upstream_model", func(f *probeFingerprint) { f.mapping.UpstreamModel = "changed" }},
		{"key_id", func(f *probeFingerprint) { f.keyID++ }},
		{"key_status", func(f *probeFingerprint) { f.keyStatus = 0 }},
		{"ciphertext", func(f *probeFingerprint) { f.keyCiphertext = "changed-private-marker" }},
	} {
		t.Run(change.name, func(t *testing.T) {
			state := &healthMemory{}
			h := healthForTest(t, state, func() time.Time { return time.Unix(100, 0) }, nil, healthPolicy{})
			original := testFingerprint()
			generation, eligible, err := h.beginProbe(context.Background(), 1, original)
			require.NoError(t, err)
			require.True(t, eligible)
			changed := original
			change.edit(&changed)
			committed, err := h.finishProbe(context.Background(), 1, generation, ProbeResult{OK: true}, nil, time.Unix(110, 0),
				func(context.Context) (probeFingerprint, error) { return changed, nil })
			require.NoError(t, err)
			require.False(t, committed)
			require.Zero(t, state.cas)
			require.Zero(t, state.writes)
		})
	}
	key := store.ChannelKey{ProviderKeyEnc: []byte("private-original")}
	fingerprint := newProbeFingerprint(store.Channel{}, store.ChannelModel{}, key)
	key.ProviderKeyEnc[0] = 'X'
	require.Equal(t, "private-original", fingerprint.keyCiphertext)
}

func TestHealthProbeInvalidResultsAndReloadFailures(t *testing.T) {
	for _, mode := range []string{"prepare_error", "parent_cancel", "parent_deadline", "deleted", "reload_error", "cas_error"} {
		t.Run(mode, func(t *testing.T) {
			state := &healthMemory{}
			var logs bytes.Buffer
			h := healthForTest(t, state, func() time.Time { return time.Unix(100, 0) }, &logs, healthPolicy{})
			ctx := context.Background()
			fingerprint := testFingerprint()
			generation, eligible, err := h.beginProbe(ctx, 1, fingerprint)
			require.NoError(t, err)
			require.True(t, eligible)
			var probeErr error
			sensitive := errors.New("private-credential-error")
			switch mode {
			case "prepare_error":
				probeErr = sensitive
			case "parent_cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "parent_deadline":
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
			case "cas_error":
				state.casFn = func(context.Context) (bool, error) { return false, sensitive }
			}
			reloads := 0
			committed, err := h.finishProbe(ctx, 1, generation, ProbeResult{}, probeErr, time.Unix(101, 0),
				func(context.Context) (probeFingerprint, error) {
					reloads++
					if mode == "deleted" {
						return probeFingerprint{}, store.ErrNotFound
					}
					if mode == "reload_error" {
						return probeFingerprint{}, sensitive
					}
					return fingerprint, nil
				})
			require.False(t, committed)
			require.Zero(t, state.writes)
			if mode == "prepare_error" || mode == "reload_error" || mode == "cas_error" {
				require.ErrorIs(t, err, sensitive)
				require.NotContains(t, err.Error(), sensitive.Error())
			} else {
				require.NoError(t, err)
			}
			if mode == "parent_cancel" || mode == "parent_deadline" || mode == "prepare_error" {
				require.Zero(t, reloads)
			}
			if mode != "cas_error" {
				require.Zero(t, state.cas)
			}
			require.NotContains(t, logs.String(), "private-credential")
			if mode == "prepare_error" {
				require.Contains(t, logs.String(), `"operation":"probe_preparation"`)
			}
		})
	}
}

func TestHealthProbeOnlyIssuedTesterResultsAreSignals(t *testing.T) {
	for _, mode := range []string{"success", "http400", "http503", "network", "own_timeout", "parent_cancel", "prepare_error"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				state := &healthMemory{}
				h := healthForTest(t, state, time.Now, nil, healthPolicy{})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fingerprint := testFingerprint()
				generation, eligible, err := h.beginProbe(ctx, 1, fingerprint)
				require.NoError(t, err)
				require.True(t, eligible)
				calls := 0
				picker := pickerFunc(func(context.Context, *store.Channel, string) (*provider.Endpoint, error) {
					if mode == "prepare_error" {
						return nil, errors.New("bad ciphertext")
					}
					return &provider.Endpoint{Provider: "openai"}, nil
				})
				upstream := &upstreamFake{chat: func(callCtx context.Context, _ *provider.Endpoint, _ *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
					calls++
					switch mode {
					case "own_timeout":
						<-callCtx.Done()
						return nil, nil, callCtx.Err()
					case "parent_cancel":
						cancel()
						return nil, nil, callCtx.Err()
					case "http400":
						return nil, nil, &provider.UpstreamError{StatusCode: 400}
					case "http503":
						return nil, nil, &provider.UpstreamError{StatusCode: 503}
					case "network":
						return nil, nil, errors.New("network failure")
					default:
						return &provider.ChatResponse{}, nil, nil
					}
				}}
				tester, err := NewConnectivityTester(picker, upstream, 20*time.Millisecond)
				require.NoError(t, err)
				result, probeErr := tester.Test(ctx, &store.Channel{ID: 1}, "alias")
				completion := time.Now()
				committed, err := h.finishProbe(ctx, 1, generation, result, probeErr, completion,
					func(context.Context) (probeFingerprint, error) { return fingerprint, nil })
				if mode == "prepare_error" {
					require.Error(t, err)
					require.Zero(t, calls)
				} else {
					require.NoError(t, err)
					require.Equal(t, 1, calls)
				}
				if mode == "prepare_error" || mode == "parent_cancel" {
					require.False(t, committed)
					require.Zero(t, state.cas)
				} else {
					require.True(t, committed)
					require.Equal(t, completion.Unix(), state.record.Snapshot.LastProbeAt)
					require.Equal(t, mode == "success", state.record.Snapshot.LastProbeOk)
					if mode == "success" {
						require.Zero(t, state.record.Snapshot.ConsecutiveFailures)
					} else {
						require.Equal(t, int32(1), state.record.Snapshot.ConsecutiveFailures)
					}
				}
			})
		})
	}
}

func TestHealthProbeExpiryBarrierAndSameVersionCompetition(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := &healthMemory{record: store.HealthRecord{Snapshot: store.HealthSnapshot{ConsecutiveFailures: 5, DisabledUntil: 100}, Version: 9}}
		h := healthForTest(t, state, func() time.Time { return time.Unix(100, 0) }, nil, healthPolicy{})
		ctx := context.Background()
		fingerprint := testFingerprint()
		first, eligible, err := h.beginProbe(ctx, 1, fingerprint)
		require.NoError(t, err)
		require.True(t, eligible)
		second, eligible, err := h.beginProbe(ctx, 1, fingerprint)
		require.NoError(t, err)
		require.True(t, eligible)
		entered, release := make(chan struct{}, 2), make(chan struct{})
		var wg sync.WaitGroup
		results := make(chan bool, 2)
		errs := make(chan error, 2)
		for _, generation := range []probeGeneration{first, second} {
			wg.Go(func() {
				entered <- struct{}{}
				<-release
				committed, err := h.finishProbe(ctx, 1, generation, ProbeResult{OK: true}, nil, time.Unix(101, 0),
					func(context.Context) (probeFingerprint, error) { return fingerprint, nil })
				results <- committed
				errs <- err
			})
		}
		<-entered
		<-entered
		require.False(t, h.IsAvailable(ctx, 1), "cooldown is over but neither issued probe has succeeded yet")
		require.Zero(t, state.writes)
		close(release)
		wg.Wait()
		close(results)
		close(errs)
		commits := 0
		for result := range results {
			if result {
				commits++
			}
		}
		for err := range errs {
			require.NoError(t, err)
		}
		require.Equal(t, 1, commits)
		require.Equal(t, 1, state.writes)
		require.Equal(t, uint64(10), state.record.Version)
		require.True(t, h.IsAvailable(ctx, 1))
	})
}

func TestHealthProbeCompletionSharesReloadAndCASBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := &healthMemory{}
		h := healthForTest(t, state, time.Now, nil, healthPolicy{stateTimeout: 50 * time.Millisecond})
		ctx := context.Background()
		fingerprint := testFingerprint()
		generation, eligible, err := h.beginProbe(ctx, 1, fingerprint)
		require.NoError(t, err)
		require.True(t, eligible)
		start := time.Now()
		state.casFn = func(ctx context.Context) (bool, error) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(50*time.Millisecond), deadline)
			<-ctx.Done()
			return false, ctx.Err()
		}
		committed, err := h.finishProbe(ctx, 1, generation, ProbeResult{OK: true}, nil, start,
			func(ctx context.Context) (probeFingerprint, error) {
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.Equal(t, start.Add(50*time.Millisecond), deadline)
				time.Sleep(30 * time.Millisecond)
				return fingerprint, nil
			})
		require.False(t, committed)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 50*time.Millisecond, time.Since(start))
		require.Zero(t, state.writes)
	})
}
