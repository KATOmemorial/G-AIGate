package store

import (
	"context"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestVersionedHealthCompatibilityAndPersistence(t *testing.T) {
	mr, client := newTestRedis(t)
	ctx := context.Background()
	state := NewVersionedHealthState(client)
	missing, err := state.LoadVersioned(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, HealthRecord{}, missing)
	require.False(t, mr.Exists(healthKey(1)))
	snapshot := HealthSnapshot{3, 123, 100, true}
	require.NoError(t, NewHealthState(client).Save(ctx, 1, snapshot))
	require.NoError(t, client.HSet(ctx, healthKey(1), "future_field", "preserved").Err())
	old, err := state.LoadVersioned(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, HealthRecord{Snapshot: snapshot}, old)
	// Even an accidental external TTL is removed only on a successful CAS.
	require.NoError(t, client.Expire(ctx, healthKey(1), time.Minute).Err())
	next := HealthSnapshot{LastProbeAt: 200, LastProbeOk: true}
	ok, err := state.CompareAndSwap(ctx, 1, old, next)
	require.NoError(t, err)
	require.True(t, ok)
	got, err := state.LoadVersioned(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, HealthRecord{next, 1}, got)
	require.Equal(t, "preserved", mr.HGet(healthKey(1), "future_field"))
	require.Zero(t, mr.TTL(healthKey(1)))
	legacy, err := NewHealthState(client).Load(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, next, legacy)
	// Legacy Save behavior remains intact and does not invent a CAS version.
	require.NoError(t, NewHealthState(client).Save(ctx, 1, snapshot))
	got, err = state.LoadVersioned(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, HealthRecord{snapshot, 1}, got)

	ok, err = state.CompareAndSwap(ctx, 2, HealthRecord{}, next)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []string{healthKey(1), healthKey(2)}, mr.Keys())
}

func TestVersionedHealthFullSnapshotConflictIsZeroWrite(t *testing.T) {
	_, client := newTestRedis(t)
	ctx := context.Background()
	state := NewVersionedHealthState(client)
	base := HealthSnapshot{2, 123, 100, true}
	require.NoError(t, NewHealthState(client).Save(ctx, 1, base))
	require.NoError(t, client.Expire(ctx, healthKey(1), time.Minute).Err())
	for _, change := range []func(*HealthRecord){
		func(r *HealthRecord) { r.Version++ },
		func(r *HealthRecord) { r.Snapshot.ConsecutiveFailures++ },
		func(r *HealthRecord) { r.Snapshot.DisabledUntil++ },
		func(r *HealthRecord) { r.Snapshot.LastProbeAt++ },
		func(r *HealthRecord) { r.Snapshot.LastProbeOk = false },
	} {
		expected := HealthRecord{Snapshot: base}
		change(&expected)
		before := client.HGetAll(ctx, healthKey(1)).Val()
		ok, err := state.CompareAndSwap(ctx, 1, expected, HealthSnapshot{})
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, before, client.HGetAll(ctx, healthKey(1)).Val())
		require.Positive(t, client.PTTL(ctx, healthKey(1)).Val())
	}
}

// A separate client mutates the watched hash after the read has returned but
// before EXEC. This exercises the Redis transaction conflict, not just equality.
type healthWatchHook struct {
	onRead func()
}

func (h healthWatchHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h healthWatchHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h healthWatchHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if err == nil && cmd.Name() == "hgetall" {
			h.onRead()
		}
		return err
	}
}

func TestVersionedHealthWatchConflictDoesNotRetry(t *testing.T) {
	mr, client := newTestRedis(t)
	other := redisFor(t, mr)
	ctx := context.Background()
	reads := 0
	client.AddHook(healthWatchHook{onRead: func() {
		reads++
		require.NoError(t, other.HSet(ctx, healthKey(1), "future_field", "external").Err())
	}})
	ok, err := NewVersionedHealthState(client).CompareAndSwap(ctx, 1, HealthRecord{}, HealthSnapshot{ConsecutiveFailures: 1})
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 1, reads)
	require.Equal(t, map[string]string{"future_field": "external"}, other.HGetAll(ctx, healthKey(1)).Val())
}

func TestVersionedHealthCancellationAfterWatchReadIsZeroWrite(t *testing.T) {
	mr, client := newTestRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client.AddHook(healthWatchHook{onRead: cancel})
	ok, err := NewVersionedHealthState(client).CompareAndSwap(ctx, 1, HealthRecord{}, HealthSnapshot{ConsecutiveFailures: 1})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
	require.Empty(t, mr.Keys())
}

func TestVersionedHealthStrictValidationAndUint64(t *testing.T) {
	_, client := newTestRedis(t)
	ctx := context.Background()
	state := NewVersionedHealthState(client)
	for _, field := range []string{healthFieldConsecutiveFailures, healthFieldDisabledUntil, healthFieldLastProbeAt, healthFieldLastProbeOk, healthFieldVersion} {
		badValues := []string{"", "-1", "+1", "1.0", "1e2", " 1", "secret-invalid-marker", "18446744073709551616"}
		if field == healthFieldLastProbeOk {
			badValues = append(badValues, "true", "false", "2", "01")
		}
		if field == healthFieldConsecutiveFailures {
			badValues = append(badValues, "2147483648")
		}
		if field == healthFieldDisabledUntil || field == healthFieldLastProbeAt {
			badValues = append(badValues, "9223372036854775808")
		}
		for i, bad := range badValues {
			t.Run(field+"/"+strconv.Itoa(i), func(t *testing.T) {
				require.NoError(t, NewHealthState(client).Save(ctx, 1, HealthSnapshot{}))
				require.NoError(t, client.HSet(ctx, healthKey(1), healthFieldVersion, "0", field, bad).Err())
				before := client.HGetAll(ctx, healthKey(1)).Val()
				_, err := state.LoadVersioned(ctx, 1)
				require.Error(t, err)
				require.NotContains(t, err.Error(), "secret-invalid-marker")
				ok, err := state.CompareAndSwap(ctx, 1, HealthRecord{}, HealthSnapshot{})
				require.Error(t, err)
				require.False(t, ok)
				require.Equal(t, before, client.HGetAll(ctx, healthKey(1)).Val())
			})
		}
	}
	for _, field := range []string{healthFieldConsecutiveFailures, healthFieldDisabledUntil, healthFieldLastProbeAt, healthFieldLastProbeOk} {
		require.NoError(t, NewHealthState(client).Save(ctx, 1, HealthSnapshot{}))
		require.NoError(t, client.HDel(ctx, healthKey(1), field).Err())
		_, err := state.LoadVersioned(ctx, 1)
		require.Error(t, err)
	}
	for _, version := range []uint64{1<<53 + 1, 1 << 63, math.MaxUint64 - 1, math.MaxUint64} {
		require.NoError(t, NewHealthState(client).Save(ctx, 1, HealthSnapshot{ConsecutiveFailures: math.MaxInt32, DisabledUntil: math.MaxInt64, LastProbeAt: math.MaxInt64}))
		require.NoError(t, client.HSet(ctx, healthKey(1), healthFieldVersion, strconv.FormatUint(version, 10)).Err())
		record, err := state.LoadVersioned(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, version, record.Version)
		before := client.HGetAll(ctx, healthKey(1)).Val()
		ok, err := state.CompareAndSwap(ctx, 1, record, HealthSnapshot{})
		if version == math.MaxUint64 {
			require.Error(t, err)
			require.False(t, ok)
			require.Equal(t, before, client.HGetAll(ctx, healthKey(1)).Val())
		} else {
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, strconv.FormatUint(version+1, 10), client.HGet(ctx, healthKey(1), healthFieldVersion).Val())
		}
	}
}

func TestVersionedHealthInvalidInputAndErrors(t *testing.T) {
	mr, client := newTestRedis(t)
	ctx := context.Background()
	state := NewVersionedHealthState(client)
	for _, id := range []int64{0, -1, math.MinInt64} {
		_, err := state.LoadVersioned(ctx, id)
		require.Error(t, err)
		ok, err := state.CompareAndSwap(ctx, id, HealthRecord{}, HealthSnapshot{})
		require.Error(t, err)
		require.False(t, ok)
	}
	for _, next := range []HealthSnapshot{{ConsecutiveFailures: -1}, {DisabledUntil: -1}, {LastProbeAt: -1}} {
		ok, err := state.CompareAndSwap(ctx, 1, HealthRecord{}, next)
		require.Error(t, err)
		require.False(t, ok)
	}
	require.Empty(t, mr.Keys())
	for _, canceled := range []bool{true, false} {
		var child context.Context
		var cancel context.CancelFunc
		want := context.Canceled
		if canceled {
			child, cancel = context.WithCancel(ctx)
			cancel()
		} else {
			child, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
			want = context.DeadlineExceeded
		}
		_, err := state.LoadVersioned(child, 1)
		require.ErrorIs(t, err, want)
		ok, err := state.CompareAndSwap(child, 1, HealthRecord{}, HealthSnapshot{})
		require.ErrorIs(t, err, want)
		require.False(t, ok)
		cancel()
	}
	require.Empty(t, mr.Keys())
	require.NoError(t, client.Set(ctx, healthKey(1), "wrong type", 0).Err())
	_, err := state.LoadVersioned(ctx, 1)
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "WRONGTYPE"))
	ok, err := state.CompareAndSwap(ctx, 1, HealthRecord{}, HealthSnapshot{})
	require.Error(t, err)
	require.False(t, ok)
	require.Equal(t, "wrong type", client.Get(ctx, healthKey(1)).Val())
	require.NoError(t, client.Close())
	_, err = state.LoadVersioned(ctx, 1)
	require.True(t, errors.Is(err, redis.ErrClosed))
	ok, err = state.CompareAndSwap(ctx, 1, HealthRecord{}, HealthSnapshot{})
	require.ErrorIs(t, err, redis.ErrClosed)
	require.False(t, ok)
}

func TestVersionedHealthSameGenerationSingleCommit(t *testing.T) {
	mr, client := newTestRedis(t)
	states := []VersionedHealthState{NewVersionedHealthState(client), NewVersionedHealthState(redisFor(t, mr))}
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	start := make(chan struct{})
	for _, state := range states {
		wg.Go(func() {
			<-start
			ok, err := state.CompareAndSwap(context.Background(), 1, HealthRecord{}, HealthSnapshot{ConsecutiveFailures: 1})
			require.NoError(t, err)
			results <- ok
		})
	}
	close(start)
	wg.Wait()
	close(results)
	commits := 0
	for ok := range results {
		if ok {
			commits++
		}
	}
	require.Equal(t, 1, commits)
	record, err := states[0].LoadVersioned(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, HealthRecord{Snapshot: HealthSnapshot{ConsecutiveFailures: 1}, Version: 1}, record)
}
