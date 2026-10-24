//go:build redis_integration

package channel

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"gaigate/internal/store"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func realHealthState(t *testing.T) (*redis.Client, store.VersionedHealthState) {
	t.Helper()
	addr := os.Getenv("GAIGATE_TEST_REDIS_ADDR")
	require.NotEmpty(t, addr, "explicit owned Redis required; never SKIP")
	c := store.NewRedis(store.RedisOptions{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, c.Ping(ctx).Err())
	return c, store.NewVersionedHealthState(c)
}

func TestHealthCore_RealRedisConcurrentFailuresAndProbeGenerations(t *testing.T) {
	c, state := realHealthState(t)
	id := time.Now().UnixNano()
	key := fmt.Sprintf("gaigate:health:%d", id)
	t.Cleanup(func() { require.NoError(t, c.Del(context.Background(), key).Err()) })
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	ctx := context.Background()
	cores := make([]*healthCore, 8)
	for i := range cores {
		_, independent := realHealthState(t)
		cores[i] = healthForTest(t, independent, clock, nil, healthPolicy{failureThreshold: 100, stateTimeout: 3 * time.Second})
	}
	start, results := make(chan struct{}), make(chan error, len(cores))
	for _, h := range cores {
		go func() {
			<-start
			ok, err := h.report(ctx, id, false)
			if err == nil && !ok {
				err = fmt.Errorf("missing request commit")
			}
			results <- err
		}()
	}
	close(start)
	for range cores {
		require.NoError(t, <-results)
	}
	r, err := state.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, uint64(8), r.Version)
	require.Equal(t, int32(8), r.Snapshot.ConsecutiveFailures)

	first, second := cores[0], cores[1]
	fingerprint := testFingerprint()
	g1, eligible, err := first.beginProbe(ctx, id, fingerprint)
	require.NoError(t, err)
	require.True(t, eligible)
	g2, eligible, err := second.beginProbe(ctx, id, fingerprint)
	require.NoError(t, err)
	require.True(t, eligible)
	require.Equal(t, g1.record, g2.record)
	reload := func(context.Context) (probeFingerprint, error) { return fingerprint, nil }
	probeResults := make(chan bool, 2)
	probeErrors := make(chan error, 2)
	for i, h := range []*healthCore{first, second} {
		generation := []probeGeneration{g1, g2}[i]
		go func() {
			ok, err := h.finishProbe(ctx, id, generation, ProbeResult{OK: true}, nil, now, reload)
			probeErrors <- err
			probeResults <- ok
		}()
	}
	commits := 0
	for range 2 {
		require.NoError(t, <-probeErrors)
		if <-probeResults {
			commits++
		}
	}
	require.Equal(t, 1, commits)
	r, err = state.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, uint64(9), r.Version)
	require.Equal(t, store.HealthSnapshot{LastProbeAt: 1000, LastProbeOk: true}, r.Snapshot)

	late, eligible, err := first.beginProbe(ctx, id, fingerprint)
	require.NoError(t, err)
	require.True(t, eligible)
	ok, err := second.report(ctx, id, false)
	require.NoError(t, err)
	require.True(t, ok)
	before, err := state.LoadVersioned(ctx, id)
	require.NoError(t, err)
	ok, err = first.finishProbe(ctx, id, late, ProbeResult{OK: true}, nil, now, reload)
	require.NoError(t, err)
	require.False(t, ok)
	after, err := state.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, before, after)
	changed, eligible, err := first.beginProbe(ctx, id, fingerprint)
	require.NoError(t, err)
	require.True(t, eligible)
	fingerprint.baseURL += "/changed"
	ok, err = first.finishProbe(ctx, id, changed, ProbeResult{OK: true}, nil, now, reload)
	require.NoError(t, err)
	require.False(t, ok)
	after, err = state.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, before, after)

	first.policy.failureThreshold = 1
	ok, err = first.report(ctx, id, false)
	require.NoError(t, err)
	require.True(t, ok)
	now = time.Unix(1060, 0)
	require.False(t, second.IsAvailable(ctx, id), "real Redis expiry does not recover")
	before, err = state.LoadVersioned(ctx, id)
	require.NoError(t, err)
	first.ReportRequestResult(ctx, id, true)
	after, err = state.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, before, after)
	t.Log("8 independent clients accumulated 8 failures; same-version probes committed once; late/config-changed signals wrote zero; expired disabled state remains closed")
}
