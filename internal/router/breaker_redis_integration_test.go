//go:build redis_integration

package router

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func breakerRealClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("GAIGATE_TEST_REDIS_ADDR")
	require.NotEmpty(t, addr, "owned Redis fixture required; never SKIP")
	c := redis.NewClient(&redis.Options{Addr: addr, ContextTimeoutEnabled: true})
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(breakerCtx, 3*time.Second)
	defer cancel()
	require.NoError(t, c.Ping(ctx).Err())
	return c
}
func breakerReal(t *testing.T, o BreakerOptions) (*redisBreaker, *redis.Client, int64) {
	t.Helper()
	c := breakerRealClient(t)
	b, err := NewBreaker(c, o, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	id := time.Now().UnixNano()
	keys := breakerKeys(id)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(breakerCtx, 3*time.Second)
		defer cancel()
		require.NoError(t, c.Del(ctx, keys...).Err())
	})
	return b.(*redisBreaker), c, id
}
func breakerServerWait(t *testing.T, c *redis.Client, until int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(breakerCtx, 4*time.Second)
	defer cancel()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		now, err := c.Time(ctx).Result()
		require.NoError(t, err)
		if now.UnixMilli() >= until {
			return
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			t.Fatal("Redis TIME deadline was not reached")
		}
	}
}
func breakerSnapshot(t *testing.T, c *redis.Client, keys []string) interface{} {
	t.Helper()
	r, err := c.Eval(breakerCtx, `local r={} for i=1,3 do r[i]={redis.call('TYPE',KEYS[i]).ok,redis.call('DUMP',KEYS[i]) or '',redis.call('PEXPIRETIME',KEYS[i])} end return r`, keys).Result()
	require.NoError(t, err)
	return r
}

func TestBreakerRedisDefaultBoundaryReadOnlyAndEpochs(t *testing.T) {
	b, c, id := breakerReal(t, breakerDefaults())
	keys := breakerKeys(id)
	before := breakerSnapshot(t, c, keys)
	for range 100 {
		breakerView(t, b, id, BreakerClosed, true)
	}
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	late := breakerAcquire(t, b, id)
	neutral := breakerAcquire(t, b, id)
	require.NoError(t, b.Complete(breakerCtx, neutral, BreakerNeutral))
	for i := range 20 {
		p := breakerAcquire(t, b, id)
		outcome := BreakerSuccess
		if i >= 10 {
			outcome = BreakerFailure
		}
		require.NoError(t, b.Complete(breakerCtx, p, outcome))
		before = breakerSnapshot(t, c, keys)
		require.NoError(t, b.Complete(breakerCtx, p, outcome))
		require.Equal(t, before, breakerSnapshot(t, c, keys))
		if i < 19 {
			breakerView(t, b, id, BreakerClosed, true)
		}
	}
	breakerView(t, b, id, BreakerOpen, false)
	require.EqualValues(t, 20, c.ZCard(breakerCtx, keys[1]).Val())
	before = breakerSnapshot(t, c, keys)
	for range 100 {
		breakerView(t, b, id, BreakerOpen, false)
	}
	p, err := b.Acquire(breakerCtx, id, time.Second)
	require.NoError(t, err)
	require.Nil(t, p)
	require.NoError(t, b.Complete(breakerCtx, late, BreakerSuccess))
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	require.EqualValues(t, -1, c.PTTL(breakerCtx, keys[0]).Val())
	now, err := c.Time(breakerCtx).Result()
	require.NoError(t, err)
	openUntil, err := c.HGet(breakerCtx, keys[0], "open_until_ms").Int64()
	require.NoError(t, err)
	require.InDelta(t, 30000, openUntil-now.UnixMilli(), 1000)
	t.Logf("default channel=%d samples=20 Redis_TIME=%d open_until=%d", id, now.UnixMilli(), openUntil)
}

func TestBreakerRedisLeaseTTLGenerationsAndPolicy(t *testing.T) {
	o := breakerDefaults()
	o.Window = 80 * time.Millisecond
	o.OpenDuration = 10 * time.Millisecond
	o.MinSamples = 1
	b, c, id := breakerReal(t, o)
	keys := breakerKeys(id)
	old := breakerAcquire(t, b, id)
	trip := breakerAcquire(t, b, id)
	require.NoError(t, b.Complete(breakerCtx, trip, BreakerFailure))
	openUntil, err := c.HGet(breakerCtx, keys[0], "open_until_ms").Int64()
	require.NoError(t, err)
	breakerServerWait(t, c, openUntil)
	probe, err := b.Acquire(breakerCtx, id, 100*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, probe)
	before := breakerSnapshot(t, c, keys)
	for range 10 {
		breakerView(t, b, id, BreakerHalfOpen, false)
	}
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	require.GreaterOrEqual(t, c.Do(breakerCtx, "PEXPIRETIME", keys[2]).Val().(int64), old.until)
	breakerServerWait(t, c, probe.until)
	breakerView(t, b, id, BreakerHalfOpen, true)
	next := breakerAcquire(t, b, id)
	before = breakerSnapshot(t, c, keys)
	require.NoError(t, b.Complete(breakerCtx, probe, BreakerSuccess))
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	require.NoError(t, b.Complete(breakerCtx, next, BreakerNeutral))
	breakerView(t, b, id, BreakerOpen, true)
	next = breakerAcquire(t, b, id)
	require.NoError(t, b.Complete(breakerCtx, next, BreakerFailure))
	breakerView(t, b, id, BreakerOpen, false)
	openUntil, err = c.HGet(breakerCtx, keys[0], "open_until_ms").Int64()
	require.NoError(t, err)
	breakerServerWait(t, c, openUntil)
	next = breakerAcquire(t, b, id)
	require.NoError(t, b.Complete(breakerCtx, next, BreakerSuccess))
	breakerView(t, b, id, BreakerClosed, true)
	require.Zero(t, c.Exists(breakerCtx, keys[1]).Val())
	before = breakerSnapshot(t, c, keys)
	require.NoError(t, b.Complete(breakerCtx, old, BreakerFailure))
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	o.MinSamples = 2
	different, err := NewBreaker(c, o, slog.Default())
	require.NoError(t, err)
	_, err = different.Inspect(breakerCtx, id)
	require.Error(t, err)
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	// A short subsequent lease and neutral release cannot shorten the long one.
	long, err := b.Acquire(breakerCtx, id, time.Second)
	require.NoError(t, err)
	short, err := b.Acquire(breakerCtx, id, 20*time.Millisecond)
	require.NoError(t, err)
	require.NoError(t, b.Complete(breakerCtx, short, BreakerNeutral))
	require.GreaterOrEqual(t, c.Do(breakerCtx, "PEXPIRETIME", keys[2]).Val().(int64), long.until)
	require.NoError(t, b.Complete(breakerCtx, long, BreakerSuccess))
	last, err := c.Time(breakerCtx).Result()
	require.NoError(t, err)
	expiry := c.Do(breakerCtx, "PEXPIRETIME", keys[1]).Val().(int64)
	require.InDelta(t, o.Window.Milliseconds(), expiry-last.UnixMilli(), 20)
	breakerServerWait(t, c, expiry+1)
	require.Zero(t, c.Exists(breakerCtx, keys[1]).Val())
	require.EqualValues(t, 1, c.Exists(breakerCtx, keys[0]).Val())
	t.Logf("accelerated window=80ms open=10ms probe=100ms; channel=%d pending_expiry=%v", id, c.Do(breakerCtx, "PEXPIRETIME", keys[2]).Val())
}

func TestBreakerRedisIndependentClientsCompleteAndProbe(t *testing.T) {
	o := breakerDefaults()
	o.MinSamples = 1000000
	o.StateTimeout = 3 * time.Second
	b, c, id := breakerReal(t, o)
	clients := []Breaker{b}
	for range 3 {
		client, err := NewBreaker(breakerRealClient(t), o, slog.Default())
		require.NoError(t, err)
		clients = append(clients, client)
	}
	const n = 64
	permits := make([]*Permit, n)
	for i := range permits {
		permits[i] = breakerAcquire(t, b, id)
	}
	start := make(chan struct{})
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i, p := range permits {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- clients[i%len(clients)].Complete(breakerCtx, p, BreakerFailure)
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.EqualValues(t, n, c.ZCard(breakerCtx, breakerKeys(id)[1]).Val())
	require.Zero(t, c.ZCard(breakerCtx, breakerKeys(id)[2]).Val())
	o.MinSamples = 1
	o.OpenDuration = 10 * time.Millisecond
	b, c, id = breakerReal(t, o)
	clients = nil
	for range 16 {
		client, err := NewBreaker(breakerRealClient(t), o, slog.Default())
		require.NoError(t, err)
		clients = append(clients, client)
	}
	trip := breakerAcquire(t, b, id)
	require.NoError(t, b.Complete(breakerCtx, trip, BreakerFailure))
	until, err := c.HGet(breakerCtx, breakerKeys(id)[0], "open_until_ms").Int64()
	require.NoError(t, err)
	breakerServerWait(t, c, until)
	start = make(chan struct{})
	results := make(chan *Permit, len(clients))
	errs = make(chan error, len(clients))
	for _, client := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			p, err := client.Acquire(breakerCtx, id, time.Second)
			results <- p
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	winners := 0
	for p := range results {
		if p != nil {
			winners++
		}
	}
	require.Equal(t, 1, winners)
	t.Logf("independent_clients=16 probe_winners=%d channel=%d", winners, id)
}

type breakerWirePermit struct {
	ChannelID          int64
	Epoch, Token, Kind string
	Until              int64
}

func breakerWire(p *Permit) breakerWirePermit {
	return breakerWirePermit{p.channelID, p.epoch, p.token, p.kind, p.until}
}
func (w breakerWirePermit) permit() *Permit {
	return &Permit{channelID: w.ChannelID, epoch: w.Epoch, token: w.Token, kind: w.Kind, until: w.Until}
}

type breakerProcessInput struct {
	Opts    BreakerOptions
	ID      int64
	Op      string
	Permits []breakerWirePermit
}
type breakerProcessResult struct {
	Winner    bool
	Completed int
	Error     string
}

func TestBreakerRedisProcessHelper(t *testing.T) {
	if os.Getenv("GAIGATE_BREAKER_CHILD") != "1" {
		return
	}
	var input breakerProcessInput
	decoder := json.NewDecoder(os.Stdin)
	require.NoError(t, decoder.Decode(&input))
	b, err := NewBreaker(breakerRealClient(t), input.Opts, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	fmt.Println("BREAKER_READY")
	var goSignal string
	require.NoError(t, decoder.Decode(&goSignal))
	require.Equal(t, "go", goSignal)
	result := breakerProcessResult{}
	if input.Op == "acquire" {
		p, err := b.Acquire(breakerCtx, input.ID, time.Second)
		result.Winner = p != nil
		if err != nil {
			result.Error = err.Error()
		}
	} else {
		for _, wire := range input.Permits {
			if err := b.Complete(breakerCtx, wire.permit(), BreakerFailure); err != nil {
				result.Error = err.Error()
				break
			}
			result.Completed++
		}
	}
	data, err := json.Marshal(result)
	require.NoError(t, err)
	fmt.Println("BREAKER_RESULT " + string(data))
	require.Empty(t, result.Error)
}

type breakerChild struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	output *bufio.Scanner
	cancel context.CancelFunc
	waited bool
}

func startBreakerChild(t *testing.T, input breakerProcessInput) *breakerChild {
	t.Helper()
	exe, err := os.Executable()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(breakerCtx, 20*time.Second)
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestBreakerRedisProcessHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "GAIGATE_BREAKER_CHILD=1")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	child := &breakerChild{cmd: cmd, input: stdin, output: bufio.NewScanner(stdout), cancel: cancel}
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		cancel()
		_ = stdin.Close()
		if !child.waited {
			_ = cmd.Wait()
		}
	})
	require.NoError(t, json.NewEncoder(stdin).Encode(input))
	ready := false
	for child.output.Scan() {
		if child.output.Text() == "BREAKER_READY" {
			ready = true
			break
		}
	}
	require.True(t, ready, "child did not reach Redis entry barrier")
	return child
}
func (c *breakerChild) goNow(t *testing.T) {
	t.Helper()
	require.NoError(t, json.NewEncoder(c.input).Encode("go"))
}
func (c *breakerChild) finish(t *testing.T) breakerProcessResult {
	t.Helper()
	var result breakerProcessResult
	found := false
	for c.output.Scan() {
		line := c.output.Text()
		if strings.HasPrefix(line, "BREAKER_RESULT ") {
			require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "BREAKER_RESULT ")), &result))
			found = true
		}
	}
	require.NoError(t, c.output.Err())
	err := c.cmd.Wait()
	c.waited = true
	c.cancel()
	_ = c.input.Close()
	require.NoError(t, err)
	require.True(t, found)
	require.Empty(t, result.Error)
	return result
}

func TestBreakerRedisTwoProcessesShareProbeAndWindow(t *testing.T) {
	o := breakerDefaults()
	o.MinSamples = 1000000
	o.StateTimeout = 3 * time.Second
	b, c, id := breakerReal(t, o)
	inputs := []breakerProcessInput{{Opts: o, ID: id, Op: "complete"}, {Opts: o, ID: id, Op: "complete"}}
	for i := range 64 {
		p := breakerAcquire(t, b, id)
		inputs[i%2].Permits = append(inputs[i%2].Permits, breakerWire(p))
	}
	a, d := startBreakerChild(t, inputs[0]), startBreakerChild(t, inputs[1])
	require.NotEqual(t, a.cmd.Process.Pid, d.cmd.Process.Pid)
	a.goNow(t)
	d.goNow(t)
	ra, rd := a.finish(t), d.finish(t)
	require.Equal(t, 64, ra.Completed+rd.Completed)
	require.EqualValues(t, 64, c.ZCard(breakerCtx, breakerKeys(id)[1]).Val())
	t.Logf("complete child_pids=%d,%d samples=64 channel=%d", a.cmd.Process.Pid, d.cmd.Process.Pid, id)
	o.MinSamples = 1
	o.OpenDuration = 10 * time.Millisecond
	b, c, id = breakerReal(t, o)
	trip := breakerAcquire(t, b, id)
	require.NoError(t, b.Complete(breakerCtx, trip, BreakerFailure))
	until, err := c.HGet(breakerCtx, breakerKeys(id)[0], "open_until_ms").Int64()
	require.NoError(t, err)
	breakerServerWait(t, c, until)
	input := breakerProcessInput{Opts: o, ID: id, Op: "acquire"}
	a, d = startBreakerChild(t, input), startBreakerChild(t, input)
	a.goNow(t)
	d.goNow(t)
	ra, rd = a.finish(t), d.finish(t)
	winners := 0
	if ra.Winner {
		winners++
	}
	if rd.Winner {
		winners++
	}
	require.Equal(t, 1, winners)
	t.Logf("probe child_pids=%d,%d winners=%d channel=%d", a.cmd.Process.Pid, d.cmd.Process.Pid, winners, id)
}

func TestBreakerRedisCorruptionZeroPartialWritesAndCapacity(t *testing.T) {
	o := breakerDefaults()
	o.StateTimeout = 3 * time.Second
	for _, field := range []string{"schema", "policy", "state", "epoch", "probe_token", "open_until_ms", "probe_until_ms"} {
		t.Run(field, func(t *testing.T) {
			b, c, id := breakerReal(t, o)
			p := breakerAcquire(t, b, id)
			keys := breakerKeys(id)
			require.NoError(t, c.ZAdd(breakerCtx, keys[2], redis.Z{Score: 1, Member: p.epoch + ":" + strings.Repeat("e", 32) + ":c"}).Err())
			require.NoError(t, c.HDel(breakerCtx, keys[0], field).Err())
			before := breakerSnapshot(t, c, keys)
			_, err := b.Acquire(breakerCtx, id, time.Second)
			require.Error(t, err)
			require.Error(t, b.Complete(breakerCtx, p, BreakerFailure))
			_, err = b.Inspect(breakerCtx, id)
			require.Error(t, err)
			require.Equal(t, before, breakerSnapshot(t, c, keys))
		})
	}
	b, c, id := breakerReal(t, o)
	p := breakerAcquire(t, b, id)
	keys := breakerKeys(id)
	pipe := c.Pipeline()
	for i := 1; i < 4096; i++ {
		pipe.ZAdd(breakerCtx, keys[2], redis.Z{Score: float64(p.until), Member: fmt.Sprintf("%s:%032x:c", p.epoch, i)})
	}
	_, err := pipe.Exec(breakerCtx)
	require.NoError(t, err)
	before := breakerSnapshot(t, c, keys)
	breakerView(t, b, id, BreakerClosed, false)
	denied, err := b.Acquire(breakerCtx, id, time.Second)
	require.NoError(t, err)
	require.Nil(t, denied)
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	require.NoError(t, b.Complete(breakerCtx, p, BreakerNeutral))
	breakerView(t, b, id, BreakerClosed, true)
	breakerAcquire(t, b, id)
	require.EqualValues(t, 4096, c.ZCard(breakerCtx, keys[2]).Val())
}

func TestBreakerRedisMutationWitness(t *testing.T) {
	mode := os.Getenv("GAIGATE_BREAKER_MUTATION")
	for _, name := range []string{"epoch", "token", "atomic"} {
		t.Run(name, func(t *testing.T) {
			o := breakerDefaults()
			o.MinSamples = 1
			o.OpenDuration = time.Millisecond
			b, c, id := breakerReal(t, o)
			keys := breakerKeys(id)
			held := breakerAcquire(t, b, id)
			trip := breakerAcquire(t, b, id)
			require.NoError(t, b.Complete(breakerCtx, trip, BreakerFailure))
			until, err := c.HGet(breakerCtx, keys[0], "open_until_ms").Int64()
			require.NoError(t, err)
			breakerServerWait(t, c, until)
			if name == "epoch" {
				probe := breakerAcquire(t, b, id)
				require.NoError(t, b.Complete(breakerCtx, probe, BreakerSuccess))
				if mode == name {
					b.script = redis.NewScript(strings.Replace(breakerLua, " or permitEpoch ~= epoch", "", 1))
				}
				require.NoError(t, b.Complete(breakerCtx, held, BreakerFailure))
				breakerView(t, b, id, BreakerClosed, true)
			}
			if name == "token" {
				probe := breakerAcquire(t, b, id)
				require.NoError(t, b.Complete(breakerCtx, probe, BreakerSuccess))
				o.MinSamples = 2 /* fresh compatible policy */
				b, _, id = breakerReal(t, o)
				p := breakerAcquire(t, b, id)
				require.NoError(t, b.Complete(breakerCtx, p, BreakerFailure))
				if mode == name {
					b.script = redis.NewScript(strings.Replace(breakerLua, "not matching or ", "", 1))
				}
				require.NoError(t, b.Complete(breakerCtx, p, BreakerFailure))
				breakerView(t, b, id, BreakerClosed, true)
			}
			if name == "atomic" {
				if mode == name {
					b.script = redis.NewScript(strings.Replace(breakerLua, "if writeHash then", "if writeHash and op ~= 'acquire' then", 1))
				}
				a, err := b.Acquire(breakerCtx, id, time.Second)
				require.NoError(t, err)
				d, err := b.Acquire(breakerCtx, id, time.Second)
				require.NoError(t, err)
				winners := 0
				if a != nil {
					winners++
				}
				if d != nil {
					winners++
				}
				require.Equal(t, 1, winners)
			}
		})
	}
	t.Log("mutation=" + strconv.Quote(mode) + "; normal is green, selected mutation must fail")
}

func TestBreakerRedisNoScriptAndArithmeticAreAtomic(t *testing.T) {
	o := breakerDefaults()
	o.MinSamples = 1000000
	b, c, id := breakerReal(t, o)
	p := breakerAcquire(t, b, id)
	keys := breakerKeys(id)
	require.NoError(t, c.ScriptFlush(breakerCtx).Err())
	viaClient := breakerRealClient(t)
	via, err := NewBreaker(viaClient, o, slog.Default())
	require.NoError(t, err)
	var names []string
	var deadlines []time.Time
	viaClient.AddHook(&breakerHook{run: func(ctx context.Context, cmd redis.Cmder) error {
		if strings.HasPrefix(cmd.Name(), "eval") {
			names = append(names, cmd.Name())
			d, ok := ctx.Deadline()
			require.True(t, ok)
			deadlines = append(deadlines, d)
		}
		return nil
	}})
	require.NoError(t, via.Complete(breakerCtx, p, BreakerFailure))
	require.Equal(t, []string{"evalsha", "eval"}, names)
	require.Equal(t, deadlines[0], deadlines[1])
	require.EqualValues(t, 1, c.ZCard(breakerCtx, keys[1]).Val())
	before := breakerSnapshot(t, c, keys)
	require.NoError(t, via.Complete(breakerCtx, p, BreakerFailure))
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	p = breakerAcquire(t, b, id)
	for _, mutation := range []struct{ old, new string }{
		{"local n, failures = 0, 0", "local n, failures = 9007199254, 0"},
		{"local tm = redis.call('TIME')", "local tm = {'9007199254740','991000'}"},
		{"local tm = redis.call('TIME')", "local tm = {'9007199254741','0'}"},
		{"local tm = redis.call('TIME')", "local tm = {'1','1000000'}"},
	} {
		before = breakerSnapshot(t, c, keys)
		b.script = redis.NewScript(strings.Replace(breakerLua, mutation.old, mutation.new, 1))
		require.Error(t, b.Complete(breakerCtx, p, BreakerFailure))
		require.Equal(t, before, breakerSnapshot(t, c, keys))
	}
}

func TestBreakerRedisTypesValuesAndExpiredCapacity(t *testing.T) {
	o := breakerDefaults()
	o.StateTimeout = 3 * time.Second
	cases := map[string]func(*redis.Client, []string, *Permit){
		"hash-type": func(c *redis.Client, k []string, p *Permit) {
			c.Del(breakerCtx, k[0])
			c.Set(breakerCtx, k[0], "bad", 0)
		},
		"events-type": func(c *redis.Client, k []string, p *Permit) { c.Set(breakerCtx, k[1], "bad", 0) },
		"pending-type": func(c *redis.Client, k []string, p *Permit) {
			c.Del(breakerCtx, k[2])
			c.Set(breakerCtx, k[2], "bad", 0)
		},
		"orphan": func(c *redis.Client, k []string, p *Permit) { c.Del(breakerCtx, k[0]) },
		"bad-number": func(c *redis.Client, k []string, p *Permit) {
			c.HSet(breakerCtx, k[0], "open_until_ms", "9007199254740992")
		},
		"fraction":    func(c *redis.Client, k []string, p *Permit) { c.HSet(breakerCtx, k[0], "probe_until_ms", "0.1") },
		"enum":        func(c *redis.Client, k []string, p *Permit) { c.HSet(breakerCtx, k[0], "state", "unknown") },
		"fingerprint": func(c *redis.Client, k []string, p *Permit) { c.HSet(breakerCtx, k[0], "policy", "wrong") },
		"event-fraction": func(c *redis.Client, k []string, p *Permit) {
			c.ZAdd(breakerCtx, k[1], redis.Z{Score: 0.5, Member: p.token + ":s"})
		},
		"event-member": func(c *redis.Client, k []string, p *Permit) {
			c.ZAdd(breakerCtx, k[1], redis.Z{Score: 1, Member: "bad"})
		},
		"pending-number": func(c *redis.Client, k []string, p *Permit) {
			c.ZAdd(breakerCtx, k[2], redis.Z{Score: 9007199254740992, Member: p.epoch + ":" + strings.Repeat("d", 32) + ":c"})
		},
		"probe-orphan": func(c *redis.Client, k []string, p *Permit) {
			c.HSet(breakerCtx, k[0], "state", "half-open", "probe_token", strings.Repeat("d", 32), "probe_until_ms", p.until)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			b, c, id := breakerReal(t, o)
			p := breakerAcquire(t, b, id)
			keys := breakerKeys(id)
			require.NoError(t, c.ZAdd(breakerCtx, keys[2], redis.Z{Score: 1, Member: p.epoch + ":" + strings.Repeat("e", 32) + ":c"}).Err())
			corrupt(c, keys, p)
			before := breakerSnapshot(t, c, keys)
			_, err := b.Inspect(breakerCtx, id)
			require.Error(t, err)
			_, err = b.Acquire(breakerCtx, id, time.Second)
			require.Error(t, err)
			require.Error(t, b.Complete(breakerCtx, p, BreakerFailure))
			require.Equal(t, before, breakerSnapshot(t, c, keys))
		})
	}
	b, c, id := breakerReal(t, o)
	p := breakerAcquire(t, b, id)
	keys := breakerKeys(id)
	now, err := c.Time(breakerCtx).Result()
	require.NoError(t, err)
	until := now.Add(500 * time.Millisecond).UnixMilli()
	pipe := c.Pipeline()
	for i := 1; i < 4096; i++ {
		pipe.ZAdd(breakerCtx, keys[2], redis.Z{Score: float64(until), Member: fmt.Sprintf("%s:%032x:c", p.epoch, i)})
	}
	_, err = pipe.Exec(breakerCtx)
	require.NoError(t, err)
	breakerView(t, b, id, BreakerClosed, false)
	breakerServerWait(t, c, until)
	before := breakerSnapshot(t, c, keys)
	breakerView(t, b, id, BreakerClosed, true)
	require.Equal(t, before, breakerSnapshot(t, c, keys))
	require.EqualValues(t, 4096, c.ZCard(breakerCtx, keys[2]).Val())
	breakerAcquire(t, b, id)
	require.EqualValues(t, 2, c.ZCard(breakerCtx, keys[2]).Val())
	require.NoError(t, c.HSet(breakerCtx, keys[0], "extension", "preserve").Err())
	require.NoError(t, b.Complete(breakerCtx, p, BreakerSuccess))
	require.Equal(t, "preserve", c.HGet(breakerCtx, keys[0], "extension").Val())
}
