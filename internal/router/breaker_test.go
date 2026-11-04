package router

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

var breakerCtx = context.Background()
var breakerTime = time.Unix(1800000000, 0)

func breakerDefaults() BreakerOptions {
	return BreakerOptions{Window: time.Minute, OpenDuration: 30 * time.Second, StateTimeout: 500 * time.Millisecond, MinSamples: 20, ErrorThreshold: 0.5}
}
func breakerFixture(t *testing.T, opts BreakerOptions) (*miniredis.Miniredis, *redisBreaker, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	mr.SetTime(breakerTime)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = c.Close() })
	b, err := NewBreaker(c, opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err, "%v", errors.Unwrap(err))
	return mr, b.(*redisBreaker), c
}
func breakerAcquire(t *testing.T, b Breaker, id int64) *Permit {
	t.Helper()
	p, err := b.Acquire(breakerCtx, id, time.Hour)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	require.NotNil(t, p)
	return p
}
func breakerView(t *testing.T, b Breaker, id int64, state BreakerState, eligible bool) {
	t.Helper()
	v, err := b.Inspect(breakerCtx, id)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	require.Equal(t, BreakerView{State: state, Eligible: eligible}, v)
}
func breakerAdvance(mr *miniredis.Miniredis, d time.Duration) {
	c := redis.NewClient(&redis.Options{Addr: mr.Addr(), ContextTimeoutEnabled: true})
	now, err := c.Time(breakerCtx).Result()
	_ = c.Close()
	if err != nil {
		panic(err)
	}
	mr.SetTime(now.Add(d))
	mr.FastForward(d)
}

type breakerHook struct {
	calls atomic.Int64
	run   func(context.Context, redis.Cmder) error
}

func (h *breakerHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		h.calls.Add(1)
		return next(ctx, network, addr)
	}
}
func (h *breakerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, c redis.Cmder) error {
		h.calls.Add(1)
		if h.run != nil {
			if err := h.run(ctx, c); err != nil {
				return err
			}
		}
		return next(ctx, c)
	}
}
func (h *breakerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestBreakerValidationBeforeRedis(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", ContextTimeoutEnabled: true})
	defer c.Close()
	h := &breakerHook{}
	c.AddHook(h)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	opts := breakerDefaults()
	_, err := NewBreaker(nil, opts, logger)
	require.Error(t, err)
	_, err = NewBreaker(c, opts, nil)
	require.Error(t, err)
	for _, d := range []time.Duration{0, -time.Millisecond, time.Nanosecond, 1500 * time.Microsecond, time.Hour + time.Millisecond, time.Duration(math.MaxInt64)} {
		for _, field := range []string{"window", "open", "timeout"} {
			t.Run(fmt.Sprintf("%s/%d", field, d), func(t *testing.T) {
				o := opts
				switch field {
				case "window":
					o.Window = d
				case "open":
					o.OpenDuration = d
				case "timeout":
					o.StateTimeout = d
				}
				_, err := NewBreaker(c, o, logger)
				require.Error(t, err)
			})
		}
	}
	for _, v := range []int{0, -1, 1000001} {
		o := opts
		o.MinSamples = v
		_, err := NewBreaker(c, o, logger)
		require.Error(t, err)
	}
	for _, v := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1), 1.000001, 0.0000009, 0.1234567, 0.5000001, math.Nextafter(0.000001, 0), math.Nextafter(1, 2)} {
		o := opts
		o.ErrorThreshold = v
		_, err := NewBreaker(c, o, logger)
		require.Error(t, err)
	}
	for _, v := range []float64{0.000001, 0.123456, 0.5, 0.999999, 1, math.Nextafter(0.123456, 1)} {
		o := opts
		o.ErrorThreshold = v
		_, err := NewBreaker(c, o, logger)
		require.NoError(t, err, "%v", errors.Unwrap(err))
	}
	v := 0.123456
	for range 3 {
		v = math.Nextafter(v, 1)
	}
	_, ok := breakerThreshold(v)
	require.False(t, ok)
	b, err := NewBreaker(c, opts, logger)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	_, err = b.Inspect(breakerCtx, 0)
	require.Error(t, err)
	for _, d := range []time.Duration{0, -1, time.Nanosecond, 1500 * time.Microsecond, 24*time.Hour + time.Millisecond} {
		_, err = b.Acquire(breakerCtx, 1, d)
		require.Error(t, err)
	}
	_, err = b.Acquire(breakerCtx, -1, time.Second)
	require.Error(t, err)
	for _, p := range []*Permit{nil, {}, {channelID: 1, epoch: strings.Repeat("a", 32), token: strings.Repeat("b", 32), kind: "x", until: 1}} {
		require.Error(t, b.Complete(breakerCtx, p, BreakerSuccess))
	}
	require.Error(t, b.Complete(breakerCtx, &Permit{channelID: 1, epoch: strings.Repeat("a", 32), token: strings.Repeat("b", 32), kind: "c", until: 1}, "bad"))
	cause := errors.New("private cause redis://secret")
	ctx, cancel := context.WithCancelCause(breakerCtx)
	cancel(cause)
	_, err = b.Inspect(ctx, 1)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, context.Canceled)
	require.NotContains(t, err.Error(), "secret")
	_, err = b.Acquire(ctx, 1, time.Second)
	require.ErrorIs(t, err, cause)
	require.Zero(t, h.calls.Load(), "constructors, invalid inputs and pre-canceled operations must perform zero Redis access")
}

func TestBreakerDefaultWindowEqualityNeutralAndDedup(t *testing.T) {
	mr, b, c := breakerFixture(t, breakerDefaults())
	keys := breakerKeys(1)
	for range 100 {
		breakerView(t, b, 1, BreakerClosed, true)
	}
	require.Empty(t, mr.Keys())
	neutral := breakerAcquire(t, b, 1)
	require.NoError(t, b.Complete(breakerCtx, neutral, BreakerNeutral))
	require.False(t, mr.Exists(keys[1]))
	for i := range 20 {
		p := breakerAcquire(t, b, 1)
		outcome := BreakerSuccess
		if i >= 10 {
			outcome = BreakerFailure
		}
		require.NoError(t, b.Complete(breakerCtx, p, outcome))
		before := mr.Dump()
		require.NoError(t, b.Complete(breakerCtx, p, outcome))
		require.Equal(t, before, mr.Dump())
		if i < 19 {
			breakerView(t, b, 1, BreakerClosed, true)
		}
	}
	breakerView(t, b, 1, BreakerOpen, false)
	require.EqualValues(t, 20, c.ZCard(breakerCtx, keys[1]).Val())
	require.Equal(t, time.Minute, mr.TTL(keys[1]))
	require.Zero(t, mr.TTL(keys[0]))
	before := mr.Dump()
	ttl := mr.TTL(keys[1])
	for range 100 {
		breakerView(t, b, 1, BreakerOpen, false)
	}
	require.Equal(t, before, mr.Dump())
	require.Equal(t, ttl, mr.TTL(keys[1]))
	p, err := b.Acquire(breakerCtx, 1, time.Second)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	require.Nil(t, p)
	require.Equal(t, before, mr.Dump())
	breakerAdvance(mr, 30*time.Second)
	breakerView(t, b, 1, BreakerOpen, true)
	probe := breakerAcquire(t, b, 1)
	breakerView(t, b, 1, BreakerHalfOpen, false)
	require.NoError(t, b.Complete(breakerCtx, probe, BreakerSuccess))
	breakerView(t, b, 1, BreakerClosed, true)
	require.False(t, mr.Exists(keys[1]))
}

func TestBreakerWindowOpenLowerClosedUpperAndCompleteTime(t *testing.T) {
	o := breakerDefaults()
	o.MinSamples = 1000000
	mr, b, c := breakerFixture(t, o)
	keys := breakerKeys(1)
	p := breakerAcquire(t, b, 1)
	breakerAdvance(mr, 30*time.Second)
	require.NoError(t, b.Complete(breakerCtx, p, BreakerFailure))
	require.EqualValues(t, breakerTime.Add(30*time.Second).UnixMilli(), c.ZRangeWithScores(breakerCtx, keys[1], 0, -1).Val()[0].Score)
	breakerAdvance(mr, time.Minute-time.Millisecond)
	p = breakerAcquire(t, b, 1)
	require.NoError(t, b.Complete(breakerCtx, p, BreakerSuccess))
	require.EqualValues(t, 2, c.ZCard(breakerCtx, keys[1]).Val())
	breakerAdvance(mr, time.Millisecond)
	p = breakerAcquire(t, b, 1)
	require.NoError(t, b.Complete(breakerCtx, p, BreakerSuccess))
	require.EqualValues(t, 2, c.ZCard(breakerCtx, keys[1]).Val())
	for _, e := range c.ZRangeWithScores(breakerCtx, keys[1], 0, -1).Val() {
		require.True(t, e.Score > float64(breakerTime.Add(30*time.Second).UnixMilli()))
	}
	breakerAdvance(mr, time.Minute)
	require.False(t, mr.Exists(keys[1]))
	require.True(t, mr.Exists(keys[0]))
	breakerView(t, b, 1, BreakerClosed, true)
}

func TestBreakerProbeOutcomesExpiryAndOldEpoch(t *testing.T) {
	for _, outcome := range []BreakerOutcome{BreakerSuccess, BreakerFailure, BreakerNeutral} {
		t.Run(string(outcome), func(t *testing.T) {
			o := breakerDefaults()
			o.MinSamples = 1
			mr, b, _ := breakerFixture(t, o)
			late := breakerAcquire(t, b, 1)
			trip := breakerAcquire(t, b, 1)
			require.NoError(t, b.Complete(breakerCtx, trip, BreakerFailure))
			before := mr.Dump()
			require.NoError(t, b.Complete(breakerCtx, late, BreakerSuccess))
			require.Equal(t, before, mr.Dump())
			breakerAdvance(mr, 30*time.Second-time.Millisecond)
			breakerView(t, b, 1, BreakerOpen, false)
			breakerAdvance(mr, time.Millisecond)
			old, err := b.Acquire(breakerCtx, 1, time.Millisecond)
			require.NoError(t, err, "%v", errors.Unwrap(err))
			require.NotNil(t, old)
			breakerAdvance(mr, time.Millisecond)
			breakerView(t, b, 1, BreakerHalfOpen, true)
			current := breakerAcquire(t, b, 1)
			require.NotEqual(t, old.epoch, current.epoch)
			before = mr.Dump()
			require.NoError(t, b.Complete(breakerCtx, old, BreakerSuccess))
			require.Equal(t, before, mr.Dump())
			require.NoError(t, b.Complete(breakerCtx, current, outcome))
			switch outcome {
			case BreakerSuccess:
				breakerView(t, b, 1, BreakerClosed, true)
			case BreakerFailure:
				breakerView(t, b, 1, BreakerOpen, false)
			case BreakerNeutral:
				breakerView(t, b, 1, BreakerOpen, true)
			}
			before = mr.Dump()
			require.NoError(t, b.Complete(breakerCtx, current, outcome))
			require.Equal(t, before, mr.Dump())
		})
	}
}

func TestBreakerAcquireRetryCapacityAndLeaseTTL(t *testing.T) {
	o := breakerDefaults()
	o.StateTimeout = time.Minute
	mr, b, c := breakerFixture(t, o)
	keys := breakerKeys(1)
	p := breakerAcquire(t, b, 1)
	argv := []any{p.token, strings.Repeat("d", 32), int64(1)}
	before := mr.Dump()
	r, err := b.run(breakerCtx, 1, "acquire", argv...)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	require.Equal(t, []interface{}{p.epoch, "c", fmt.Sprint(p.until)}, r)
	require.Equal(t, before, mr.Dump())
	short, err := b.Acquire(breakerCtx, 1, time.Millisecond)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	require.NotNil(t, short)
	require.Equal(t, time.Hour, mr.TTL(keys[2]))
	require.NoError(t, b.Complete(breakerCtx, short, BreakerNeutral))
	require.Equal(t, time.Hour, mr.TTL(keys[2]))
	pipe := c.Pipeline()
	for i := 1; i < 4096; i++ {
		pipe.ZAdd(breakerCtx, keys[2], redis.Z{Score: float64(p.until), Member: fmt.Sprintf("%s:%032x:c", p.epoch, i)})
	}
	_, err = pipe.Exec(breakerCtx)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	breakerView(t, b, 1, BreakerClosed, false)
	before = mr.Dump()
	denied, err := b.Acquire(breakerCtx, 1, time.Second)
	require.NoError(t, err, "%v", errors.Unwrap(err))
	require.Nil(t, denied)
	require.Equal(t, before, mr.Dump())
	breakerAdvance(mr, time.Hour)
	expired := mr.Dump()
	require.NoError(t, b.Complete(breakerCtx, p, BreakerFailure))
	require.Equal(t, expired, mr.Dump())
	breakerView(t, b, 1, BreakerClosed, true)
	breakerAcquire(t, b, 1)
	require.EqualValues(t, 1, c.ZCard(breakerCtx, keys[2]).Val())
}

func TestBreakerBadStateHasZeroPartialWrites(t *testing.T) {
	cases := map[string]func(*redis.Client, []string){
		"hash-type":      func(c *redis.Client, k []string) { c.Del(breakerCtx, k[0]); c.Set(breakerCtx, k[0], "bad", 0) },
		"events-type":    func(c *redis.Client, k []string) { c.Set(breakerCtx, k[1], "bad", 0) },
		"pending-type":   func(c *redis.Client, k []string) { c.Del(breakerCtx, k[2]); c.Set(breakerCtx, k[2], "bad", 0) },
		"orphan":         func(c *redis.Client, k []string) { c.Del(breakerCtx, k[0]) },
		"schema":         func(c *redis.Client, k []string) { c.HSet(breakerCtx, k[0], "schema", "2") },
		"policy":         func(c *redis.Client, k []string) { c.HSet(breakerCtx, k[0], "policy", "other") },
		"field-missing":  func(c *redis.Client, k []string) { c.HDel(breakerCtx, k[0], "probe_token") },
		"state":          func(c *redis.Client, k []string) { c.HSet(breakerCtx, k[0], "state", "bad") },
		"epoch":          func(c *redis.Client, k []string) { c.HSet(breakerCtx, k[0], "epoch", "bad") },
		"number":         func(c *redis.Client, k []string) { c.HSet(breakerCtx, k[0], "open_until_ms", "9007199254740992") },
		"fraction":       func(c *redis.Client, k []string) { c.HSet(breakerCtx, k[0], "probe_until_ms", "0.5") },
		"hash-ttl":       func(c *redis.Client, k []string) { c.Expire(breakerCtx, k[0], time.Hour) },
		"pending-member": func(c *redis.Client, k []string) { c.ZAdd(breakerCtx, k[2], redis.Z{Score: 1, Member: "bad"}) },
		"event-member":   func(c *redis.Client, k []string) { c.ZAdd(breakerCtx, k[1], redis.Z{Score: 1, Member: "bad"}) },
		"probe-without-pending": func(c *redis.Client, k []string) {
			c.HSet(breakerCtx, k[0], "state", "half-open", "probe_token", strings.Repeat("a", 32), "probe_until_ms", breakerTime.Add(time.Hour).UnixMilli())
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			mr, b, c := breakerFixture(t, breakerDefaults())
			p := breakerAcquire(t, b, 1)
			keys := breakerKeys(1)
			require.NoError(t, c.ZAdd(breakerCtx, keys[2], redis.Z{Score: 1, Member: fmt.Sprintf("%s:%s:c", p.epoch, strings.Repeat("e", 32))}).Err())
			corrupt(c, keys)
			before := mr.Dump()
			ttls := []time.Duration{mr.TTL(keys[0]), mr.TTL(keys[1]), mr.TTL(keys[2])}
			_, err := b.Inspect(breakerCtx, 1)
			require.Error(t, err)
			_, err = b.Acquire(breakerCtx, 1, time.Second)
			require.Error(t, err)
			require.Error(t, b.Complete(breakerCtx, p, BreakerFailure))
			require.Equal(t, before, mr.Dump())
			require.Equal(t, ttls, []time.Duration{mr.TTL(keys[0]), mr.TTL(keys[1]), mr.TTL(keys[2])})
		})
	}
}

func TestBreakerConcurrentCompletesAndSingleProbe(t *testing.T) {
	o := breakerDefaults()
	o.MinSamples = 1000000
	o.StateTimeout = time.Minute
	mr, b, c := breakerFixture(t, o)
	const count = 64
	permits := make([]*Permit, count)
	for i := range permits {
		permits[i] = breakerAcquire(t, b, 1)
	}
	start := make(chan struct{})
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for _, p := range permits {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; errs <- b.Complete(breakerCtx, p, BreakerFailure) }()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "%v", errors.Unwrap(err))
	}
	require.EqualValues(t, count, c.ZCard(breakerCtx, breakerKeys(1)[1]).Val())
	require.Zero(t, c.ZCard(breakerCtx, breakerKeys(1)[2]).Val())
	o.MinSamples = 1
	// Competition against the SAME server uses independent clients, no local lock.
	mr2 := miniredis.RunT(t)
	mr2.SetTime(breakerTime)
	clients := make([]Breaker, 16)
	for i := range clients {
		cc := redis.NewClient(&redis.Options{Addr: mr2.Addr(), ContextTimeoutEnabled: true})
		t.Cleanup(func() { _ = cc.Close() })
		var err error
		clients[i], err = NewBreaker(cc, o, slog.New(slog.NewTextHandler(io.Discard, nil)))
		require.NoError(t, err, "%v", errors.Unwrap(err))
	}
	trip := breakerAcquire(t, clients[0], 3)
	require.NoError(t, clients[0].Complete(breakerCtx, trip, BreakerFailure))
	breakerAdvance(mr2, 30*time.Second)
	start = make(chan struct{})
	results := make(chan *Permit, len(clients))
	errs = make(chan error, len(clients))
	for _, client := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, err := client.Acquire(breakerCtx, 3, time.Hour)
			results <- p
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err, "%v", errors.Unwrap(err))
	}
	winners := 0
	for p := range results {
		if p != nil {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	require.True(t, mr.Exists(breakerKeys(1)[0]))
}

func TestBreakerPermitTokenPolicyAndProbeRetry(t *testing.T) {
	o := breakerDefaults()
	o.MinSamples = 1
	mr, b, c := breakerFixture(t, o)
	p := breakerAcquire(t, b, 1)
	for _, change := range []func(*Permit){func(q *Permit) { q.token = strings.Repeat("a", 32) }, func(q *Permit) { q.epoch = strings.Repeat("b", 32) }, func(q *Permit) { q.until++ }} {
		q := *p
		change(&q)
		before := mr.Dump()
		require.NoError(t, b.Complete(breakerCtx, &q, BreakerFailure))
		require.Equal(t, before, mr.Dump())
	}
	o.ErrorThreshold = 0.75
	other, err := NewBreaker(c, o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	before := mr.Dump()
	_, err = other.Inspect(breakerCtx, 1)
	require.Error(t, err)
	_, err = other.Acquire(breakerCtx, 1, time.Second)
	require.Error(t, err)
	require.Error(t, other.Complete(breakerCtx, p, BreakerFailure))
	require.Equal(t, before, mr.Dump())
	require.NoError(t, b.Complete(breakerCtx, p, BreakerFailure))
	breakerAdvance(mr, 30*time.Second)
	probe, err := b.Acquire(breakerCtx, 1, 24*time.Hour)
	require.NoError(t, err)
	require.NotNil(t, probe)
	before = mr.Dump()
	r, err := b.run(breakerCtx, 1, "acquire", probe.token, strings.Repeat("f", 32), 1)
	require.NoError(t, err)
	require.Equal(t, []interface{}{probe.epoch, "p", fmt.Sprint(probe.until)}, r)
	require.Equal(t, before, mr.Dump())
	require.Equal(t, 24*time.Hour, mr.TTL(breakerKeys(1)[2]))
	breakerAdvance(mr, time.Hour)
	for range 100 {
		breakerView(t, b, 1, BreakerHalfOpen, false)
	}
	require.Equal(t, 23*time.Hour, mr.TTL(breakerKeys(1)[2]))
}

func TestBreakerOneBudgetCauseAndReadOnlyCommands(t *testing.T) {
	_, b, c := breakerFixture(t, breakerDefaults())
	var deadlines []time.Time
	var commands []string
	h := &breakerHook{run: func(ctx context.Context, cmd redis.Cmder) error {
		if strings.HasPrefix(cmd.Name(), "eval") {
			d, ok := ctx.Deadline()
			require.True(t, ok)
			deadlines = append(deadlines, d)
			commands = append(commands, cmd.Name())
		}
		return nil
	}}
	c.AddHook(h)
	breakerView(t, b, 1, BreakerClosed, true)
	require.Equal(t, []string{"evalsha", "eval"}, commands)
	require.Equal(t, deadlines[0], deadlines[1])
	cause := errors.New("private secret")
	parent, cancel := context.WithTimeoutCause(breakerCtx, 20*time.Millisecond, cause)
	defer cancel()
	h.run = func(ctx context.Context, cmd redis.Cmder) error { <-ctx.Done(); return ctx.Err() }
	began := time.Now()
	_, err := b.Inspect(parent, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, cause)
	require.NotContains(t, err.Error(), "secret")
	require.Less(t, time.Since(began), 250*time.Millisecond)
	b.opts.StateTimeout = 20 * time.Millisecond
	began = time.Now()
	_, err = b.Inspect(breakerCtx, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(began), 250*time.Millisecond)
}

func TestBreakerLuaArithmeticAndUnknownHashFields(t *testing.T) {
	mr, b, c := breakerFixture(t, breakerDefaults())
	p := breakerAcquire(t, b, math.MaxInt64)
	keys := breakerKeys(math.MaxInt64)
	require.Equal(t, "gaigate:breaker:{9223372036854775807}", keys[0])
	require.NoError(t, c.HSet(breakerCtx, keys[0], "extension", "keep").Err())
	require.NoError(t, b.Complete(breakerCtx, p, BreakerSuccess))
	require.Equal(t, "keep", c.HGet(breakerCtx, keys[0], "extension").Val())
	before := mr.Dump()
	// Private script substitution only: no exported clock or wall clock in production.
	b.script = redis.NewScript(strings.Replace(breakerLua, "local tm = redis.call('TIME')", "local tm = {'9007199254740','991000'}", 1))
	_, err := b.Acquire(breakerCtx, math.MaxInt64, time.Second)
	require.Error(t, err)
	require.Equal(t, before, mr.Dump())
}
