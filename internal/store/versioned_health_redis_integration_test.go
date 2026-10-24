//go:build redis_integration

package store

import (
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func realHealthKey(t *testing.T, client *redis.Client) (int64, string) {
	t.Helper()
	id := time.Now().UnixNano()
	key := healthKey(id)
	require.Zero(t, client.Exists(context.Background(), key).Val())
	t.Cleanup(func() { require.NoError(t, client.Del(context.Background(), key).Err()) })
	return id, key
}

func TestVersionedHealth_RealRedisCompatibilityPrecisionAndZeroWrites(t *testing.T) {
	c := quotaIntegrationRedis(t)
	id, key := realHealthKey(t, c)
	ctx := context.Background()
	s := NewVersionedHealthState(c)
	r, err := s.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, HealthRecord{}, r)
	require.Zero(t, c.Exists(ctx, key).Val(), "read does not create a hash")
	base := HealthSnapshot{ConsecutiveFailures: 2, DisabledUntil: 123, LastProbeAt: 100, LastProbeOk: true}
	require.NoError(t, NewHealthState(c).Save(ctx, id, base))
	require.NoError(t, c.HSet(ctx, key, "future", "preserve").Err())
	r, err = s.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, HealthRecord{Snapshot: base}, r)
	require.NoError(t, c.Expire(ctx, key, time.Minute).Err())
	for _, change := range []func(*HealthRecord){
		func(r *HealthRecord) { r.Version++ },
		func(r *HealthRecord) { r.Snapshot.ConsecutiveFailures++ },
		func(r *HealthRecord) { r.Snapshot.DisabledUntil++ },
		func(r *HealthRecord) { r.Snapshot.LastProbeAt++ },
		func(r *HealthRecord) { r.Snapshot.LastProbeOk = false },
	} {
		expected := r
		change(&expected)
		before := c.HGetAll(ctx, key).Val()
		ok, err := s.CompareAndSwap(ctx, id, expected, HealthSnapshot{})
		require.NoError(t, err)
		require.False(t, ok)
		require.Equal(t, before, c.HGetAll(ctx, key).Val())
		require.Positive(t, c.PTTL(ctx, key).Val(), "conflict must not PERSIST")
	}
	for _, version := range []uint64{0, 1<<53 + 1, 1<<63 + 1, math.MaxUint64 - 1} {
		t.Run(strconv.FormatUint(version, 10), func(t *testing.T) {
			require.NoError(t, c.HSet(ctx, key, "version", strconv.FormatUint(version, 10)).Err())
			r, err := s.LoadVersioned(ctx, id)
			require.NoError(t, err)
			require.Equal(t, version, r.Version)
			ok, err := s.CompareAndSwap(ctx, id, r, base)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, strconv.FormatUint(version+1, 10), c.HGet(ctx, key, "version").Val())
			require.Equal(t, "preserve", c.HGet(ctx, key, "future").Val())
			require.Equal(t, time.Duration(-1), c.PTTL(ctx, key).Val())
			old, err := NewHealthState(c).Load(ctx, id)
			require.NoError(t, err)
			require.Equal(t, base, old)
		})
	}
	r, err = s.LoadVersioned(ctx, id)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), r.Version)
	before := c.HGetAll(ctx, key).Val()
	ok, err := s.CompareAndSwap(ctx, id, r, HealthSnapshot{})
	require.Error(t, err)
	require.False(t, ok)
	require.Equal(t, before, c.HGetAll(ctx, key).Val())
}

func TestVersionedHealth_RealRedisBadDataAndContext(t *testing.T) {
	c := quotaIntegrationRedis(t)
	id, key := realHealthKey(t, c)
	ctx := context.Background()
	s := NewVersionedHealthState(c)
	for _, field := range []string{healthFieldConsecutiveFailures, healthFieldDisabledUntil, healthFieldLastProbeAt, healthFieldLastProbeOk, healthFieldVersion} {
		values := []string{"", "-1", "+1", "1.0", " 1", "18446744073709551616", "sensitive-marker"}
		if field == healthFieldLastProbeOk {
			values = append(values, "true", "false", "2", "01")
		}
		if field == healthFieldConsecutiveFailures {
			values = append(values, "2147483648")
		}
		if field == healthFieldDisabledUntil || field == healthFieldLastProbeAt {
			values = append(values, "9223372036854775808")
		}
		for index, value := range values {
			t.Run(field+"/"+strconv.Itoa(index), func(t *testing.T) {
				require.NoError(t, NewHealthState(c).Save(ctx, id, HealthSnapshot{}))
				require.NoError(t, c.HSet(ctx, key, "version", "0", field, value).Err())
				before := c.HGetAll(ctx, key).Val()
				_, err := s.LoadVersioned(ctx, id)
				require.Error(t, err)
				require.NotContains(t, err.Error(), "sensitive-marker")
				ok, err := s.CompareAndSwap(ctx, id, HealthRecord{}, HealthSnapshot{})
				require.Error(t, err)
				require.False(t, ok)
				require.Equal(t, before, c.HGetAll(ctx, key).Val())
			})
		}
	}
	for _, field := range []string{healthFieldConsecutiveFailures, healthFieldDisabledUntil, healthFieldLastProbeAt, healthFieldLastProbeOk} {
		require.NoError(t, NewHealthState(c).Save(ctx, id, HealthSnapshot{}))
		require.NoError(t, c.HSet(ctx, key, "version", "0").Err())
		require.NoError(t, c.HDel(ctx, key, field).Err())
		_, err := s.LoadVersioned(ctx, id)
		require.Error(t, err)
	}
	require.NoError(t, c.Del(ctx, key).Err())
	for _, invalidID := range []int64{0, -1, math.MinInt64} {
		_, err := s.LoadVersioned(ctx, invalidID)
		require.Error(t, err)
		ok, err := s.CompareAndSwap(ctx, invalidID, HealthRecord{}, HealthSnapshot{})
		require.Error(t, err)
		require.False(t, ok)
	}
	for _, next := range []HealthSnapshot{{ConsecutiveFailures: -1}, {DisabledUntil: -1}, {LastProbeAt: -1}} {
		ok, err := s.CompareAndSwap(ctx, id, HealthRecord{}, next)
		require.Error(t, err)
		require.False(t, ok)
		require.Zero(t, c.Exists(ctx, key).Val())
	}
	for _, deadline := range []bool{false, true} {
		canceled, cancel := context.WithCancel(ctx)
		want := context.Canceled
		if deadline {
			cancel()
			canceled, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
			want = context.DeadlineExceeded
		}
		cancel()
		_, err := s.LoadVersioned(canceled, id)
		require.ErrorIs(t, err, want)
		ok, err := s.CompareAndSwap(canceled, id, HealthRecord{}, HealthSnapshot{})
		require.ErrorIs(t, err, want)
		require.False(t, ok)
	}
	other := quotaIntegrationRedis(t)
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	other.AddHook(healthWatchHook{onRead: cancel})
	ok, err := NewVersionedHealthState(other).CompareAndSwap(watchCtx, id, HealthRecord{}, HealthSnapshot{})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, ok)
	require.Zero(t, c.Exists(ctx, key).Val())
	require.NoError(t, c.Set(ctx, key, "wrong type", 0).Err())
	_, err = s.LoadVersioned(ctx, id)
	require.ErrorContains(t, err, "WRONGTYPE")
	ok, err = s.CompareAndSwap(ctx, id, HealthRecord{}, HealthSnapshot{})
	require.ErrorContains(t, err, "WRONGTYPE")
	require.False(t, ok)
	require.Equal(t, "wrong type", c.Get(ctx, key).Val())
	closed := quotaIntegrationRedis(t)
	require.NoError(t, closed.Close())
	_, err = NewVersionedHealthState(closed).LoadVersioned(ctx, id)
	require.ErrorIs(t, err, redis.ErrClosed)
}

func TestVersionedHealth_RealRedisIndependentClientsWatchCompetition(t *testing.T) {
	c := quotaIntegrationRedis(t)
	id, key := realHealthKey(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	arrived, release := make(chan struct{}, 2), make(chan struct{})
	type result struct {
		ok  bool
		err error
	}
	results := make(chan result, 2)
	for range 2 {
		client := quotaIntegrationRedis(t)
		client.AddHook(healthWatchHook{onRead: func() {
			arrived <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
		}})
		go func() {
			ok, err := NewVersionedHealthState(client).CompareAndSwap(ctx, id, HealthRecord{}, HealthSnapshot{ConsecutiveFailures: 1})
			results <- result{ok, err}
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-ctx.Done():
			t.Fatal("WATCH read barrier timed out")
		}
	}
	close(release)
	commits := 0
	for range 2 {
		r := <-results
		require.NoError(t, r.err)
		if r.ok {
			commits++
		}
	}
	require.Equal(t, 1, commits)
	require.Equal(t, "1", c.HGet(ctx, key, "version").Val())
	require.Equal(t, "1", c.HGet(ctx, key, healthFieldConsecutiveFailures).Val())
	require.Len(t, c.HGetAll(ctx, key).Val(), 5, "no distributed lock keys or extra fields")
	t.Log("two independent TCP clients both completed WATCH+HGETALL before release; exactly one EXEC committed")
}
