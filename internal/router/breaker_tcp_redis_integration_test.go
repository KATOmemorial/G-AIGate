//go:build redis_integration

package router

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/middleware"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func nilLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// A real TCP RESP2 proxy owns and joins its accept loop and every connection.
// Faults stay latched until Close, which tests call AFTER the operation exits.
type breakerRESPProxy struct {
	listener    net.Listener
	target      string
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	conns       map[net.Conn]struct{}
	mode        atomic.Int32 // 0 forward, 1 silent reply, 2 fallback then silent socket read, 3 drop first script reply, 4 delayed NOSCRIPT then silence
	dropped     atomic.Bool
	entered     chan struct{}
	enterOnce   sync.Once
	scriptCalls atomic.Int64
}

func breakerProxy(t *testing.T, target string) *breakerRESPProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(breakerCtx)
	p := &breakerRESPProxy{listener: ln, target: target, ctx: ctx, cancel: cancel, conns: make(map[net.Conn]struct{}), entered: make(chan struct{})}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			if p.ctx.Err() != nil {
				p.mu.Unlock()
				_ = c.Close()
				return
			}
			p.conns[c] = struct{}{}
			p.mu.Unlock()
			p.wg.Add(1)
			go p.serve(c)
		}
	}()
	t.Cleanup(p.close)
	return p
}
func (p *breakerRESPProxy) close() {
	p.cancel()
	_ = p.listener.Close()
	p.mu.Lock()
	for c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
func (p *breakerRESPProxy) signal() { p.enterOnce.Do(func() { close(p.entered) }) }
func (p *breakerRESPProxy) serve(front net.Conn) {
	defer p.wg.Done()
	defer func() { _ = front.Close(); p.mu.Lock(); delete(p.conns, front); p.mu.Unlock() }()
	back, err := (&net.Dialer{}).DialContext(p.ctx, "tcp", p.target)
	if err != nil {
		return
	}
	defer back.Close()
	p.mu.Lock()
	p.conns[back] = struct{}{}
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.conns, back); p.mu.Unlock() }()
	fr, br := bufio.NewReader(front), bufio.NewReader(back)
	blockNext := false
	for {
		if blockNext {
			if _, err := fr.Peek(1); err != nil {
				return
			}
			p.signal()
			<-p.ctx.Done()
			return
		}
		frame, err := breakerRESPFrame(fr)
		if err != nil {
			return
		}
		lower := bytes.ToLower(frame)
		isScript := bytes.Contains(lower, []byte("\r\nevalsha\r\n")) || bytes.Contains(lower, []byte("\r\neval\r\n"))
		if isScript {
			p.scriptCalls.Add(1)
			switch p.mode.Load() {
			case 1:
				p.signal()
				<-p.ctx.Done()
				return
			case 2:
				if _, err = io.WriteString(front, "-NOSCRIPT controlled cache miss\r\n"); err != nil {
					return
				}
				blockNext = true
				continue
			case 4:
				timer := time.NewTimer(30 * time.Millisecond)
				select {
				case <-timer.C:
				case <-p.ctx.Done():
					timer.Stop()
					return
				}
				if _, err = io.WriteString(front, "-NOSCRIPT controlled delayed miss\r\n"); err != nil {
					return
				}
				p.mode.Store(1)
				continue
			}
		}
		if _, err = back.Write(frame); err != nil {
			return
		}
		reply, err := breakerRESPFrame(br)
		if err != nil {
			return
		}
		// Drop only a successful server execution, never the initial NOSCRIPT reply.
		if isScript && p.mode.Load() == 3 && len(reply) > 0 && reply[0] != '-' && p.dropped.CompareAndSwap(false, true) {
			p.signal()
			return
		}
		if _, err = front.Write(reply); err != nil {
			return
		}
	}
}
func breakerRESPFrame(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, io.ErrUnexpectedEOF
	}
	out := append([]byte(nil), line...)
	switch line[0] {
	case '$':
		n, err := strconv.Atoi(strings.TrimSpace(string(line[1:])))
		if err != nil || n > 32*1024*1024 {
			return nil, fmt.Errorf("invalid RESP bulk")
		}
		if n >= 0 {
			body := make([]byte, n+2)
			if _, err = io.ReadFull(r, body); err != nil {
				return nil, err
			}
			out = append(out, body...)
		}
	case '*':
		n, err := strconv.Atoi(strings.TrimSpace(string(line[1:])))
		if err != nil || n > 1000000 {
			return nil, fmt.Errorf("invalid RESP array")
		}
		for range n {
			part, err := breakerRESPFrame(r)
			if err != nil {
				return nil, err
			}
			out = append(out, part...)
		}
	case '+', '-', ':':
	default:
		return nil, fmt.Errorf("unexpected RESP type")
	}
	return out, nil
}
func breakerProxyClient(t *testing.T, p *breakerRESPProxy, retries int) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: p.listener.Addr().String(), Protocol: 2, ContextTimeoutEnabled: true, DisableIdentity: true, MaxRetries: retries, MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond, PoolSize: 1, PoolTimeout: time.Second})
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.Ping(breakerCtx).Err())
	return c
}

func TestBreakerRedisDroppedRepliesRetrySamePermit(t *testing.T) {
	for _, probe := range []bool{false, true} {
		t.Run(fmt.Sprint("probe=", probe), func(t *testing.T) {
			o := breakerDefaults()
			o.MinSamples = 1000000
			o.StateTimeout = 2 * time.Second
			if probe {
				o.MinSamples = 1
				o.OpenDuration = time.Millisecond
			}
			b, direct, id := breakerReal(t, o)
			keys := breakerKeys(id)
			if probe {
				trip := breakerAcquire(t, b, id)
				require.NoError(t, b.Complete(breakerCtx, trip, BreakerFailure))
				until, err := direct.HGet(breakerCtx, keys[0], "open_until_ms").Int64()
				require.NoError(t, err)
				breakerServerWait(t, direct, until)
			}
			p := breakerProxy(t, direct.Options().Addr)
			c := breakerProxyClient(t, p, 1)
			via, err := NewBreaker(c, o, nilLogger())
			require.NoError(t, err)
			p.mode.Store(3)
			permit, err := via.Acquire(breakerCtx, id, time.Second)
			require.NoError(t, err)
			require.NotNil(t, permit)
			require.True(t, p.dropped.Load())
			require.EqualValues(t, 1, direct.ZCard(breakerCtx, keys[2]).Val())
			member := permit.epoch + ":" + permit.token + ":" + permit.kind
			require.EqualValues(t, permit.until, direct.ZScore(breakerCtx, keys[2], member).Val())
			expiry := direct.Do(breakerCtx, "PEXPIRETIME", keys[2]).Val().(int64)
			require.GreaterOrEqual(t, expiry, permit.until)
			p.dropped.Store(false)
			outcome := BreakerFailure
			if probe {
				outcome = BreakerNeutral
			}
			require.NoError(t, via.Complete(breakerCtx, permit, outcome))
			require.True(t, p.dropped.Load())
			if !probe {
				require.EqualValues(t, 1, direct.ZCard(breakerCtx, keys[1]).Val())
			} else {
				breakerView(t, b, id, BreakerOpen, true)
			}
			require.Zero(t, direct.ZCard(breakerCtx, keys[2]).Val())
			t.Logf("dropped acquire+complete replies probe=%v token=%s deadline=%d script_calls=%d", probe, permit.token[:8], permit.until, p.scriptCalls.Load())
		})
	}
}

func TestBreakerRedisAcquireErrorLeavesOnlyOriginalLease(t *testing.T) {
	o := breakerDefaults()
	o.StateTimeout = 100 * time.Millisecond
	b, direct, id := breakerReal(t, o)
	_ = b
	p := breakerProxy(t, direct.Options().Addr)
	c := breakerProxyClient(t, p, -1)
	via, err := NewBreaker(c, o, nilLogger())
	require.NoError(t, err)
	p.mode.Store(3)
	permit, err := via.Acquire(breakerCtx, id, 120*time.Millisecond)
	require.Error(t, err)
	require.Nil(t, permit)
	require.True(t, p.dropped.Load())
	keys := breakerKeys(id)
	rows, err := direct.ZRangeWithScores(breakerCtx, keys[2], 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	until := int64(rows[0].Score)
	expiry := direct.Do(breakerCtx, "PEXPIRETIME", keys[2]).Val().(int64)
	require.Equal(t, until, expiry)
	breakerServerWait(t, direct, until+1)
	require.Zero(t, direct.Exists(breakerCtx, keys[2]).Val())
	breakerView(t, b, id, BreakerClosed, true)
}

type breakerWriteWitness struct {
	net.Conn
	armed       *atomic.Bool
	writeFailed *atomic.Bool
	written     *atomic.Int64
	unwritten   *atomic.Int64
}

func (c *breakerWriteWitness) Write(data []byte) (int, error) {
	n, err := c.Conn.Write(data)
	if c.armed.Load() {
		c.written.Add(int64(n))
		if err != nil {
			c.writeFailed.Store(true)
			c.unwritten.Add(int64(len(data) - n))
		}
	}
	return n, err
}

func TestBreakerRedisTCPBudgetsAndOwners(t *testing.T) {
	for _, op := range []string{"inspect", "acquire", "complete"} {
		for _, fault := range []string{"read", "write", "pool", "noscript"} {
			for _, earlier := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/earlier=%v", op, fault, earlier), func(t *testing.T) {
					o := breakerDefaults()
					o.StateTimeout = 120 * time.Millisecond
					b, direct, id := breakerReal(t, o)
					permit := breakerAcquire(t, b, id)
					p := breakerProxy(t, direct.Options().Addr)
					var armed, writeFailed atomic.Bool
					var written atomic.Int64
					var unwritten atomic.Int64
					c := redis.NewClient(&redis.Options{Addr: p.listener.Addr().String(), Protocol: 2, ContextTimeoutEnabled: true, DisableIdentity: true, MaxRetries: -1, PoolSize: 1, PoolTimeout: time.Second, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
						conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
						if err != nil {
							return nil, err
						}
						return &breakerWriteWitness{Conn: conn, armed: &armed, writeFailed: &writeFailed, written: &written, unwritten: &unwritten}, nil
					}})
					t.Cleanup(func() { _ = c.Close() })
					require.NoError(t, c.Ping(breakerCtx).Err())
					via, err := NewBreaker(c, o, nilLogger())
					require.NoError(t, err)
					var valuesSeen atomic.Bool
					c.AddHook(&breakerHook{run: func(ctx context.Context, cmd redis.Cmder) error {
						requestID, _ := middleware.RequestIDFromContext(ctx)
						valuesSeen.Store(requestID == "breaker-budget-request")
						return nil
					}})
					var held *redis.Conn
					switch fault {
					case "read":
						p.mode.Store(1)
					case "write":
						p.mode.Store(2)
						via.(*redisBreaker).script = redis.NewScript("--" + strings.Repeat("x", 16*1024*1024) + "\n" + breakerLua)
						armed.Store(true)
					case "noscript":
						p.mode.Store(4)
					case "pool":
						held = c.Conn()
						require.NoError(t, held.Ping(breakerCtx).Err())
						defer held.Close()
					}
					ctx := middleware.WithRequestID(breakerCtx, "breaker-budget-request")
					cause := errors.New("private redis://credential cause")
					var cancel context.CancelFunc
					if earlier {
						ctx, cancel = context.WithTimeoutCause(ctx, 70*time.Millisecond, cause)
						defer cancel()
					}
					result := make(chan error, 1)
					started := time.Now()
					go func() {
						var err error
						switch op {
						case "inspect":
							_, err = via.Inspect(ctx, id)
						case "acquire":
							_, err = via.Acquire(ctx, id, time.Second)
						case "complete":
							err = via.Complete(ctx, permit, BreakerNeutral)
						}
						result <- err
					}()
					if fault != "pool" {
						select {
						case <-p.entered:
						case <-time.After(time.Second):
							p.close()
							<-result
							t.Fatal("fault was not entered")
						}
					}
					var opErr error
					select {
					case opErr = <-result:
					case <-time.After(700 * time.Millisecond):
						p.close()
						<-result
						t.Fatal("operation required releasing the fault to exit")
					}
					elapsed := time.Since(started)
					require.Error(t, opErr)
					require.ErrorIs(t, opErr, context.DeadlineExceeded)
					if earlier {
						require.ErrorIs(t, opErr, cause)
					}
					require.NotContains(t, opErr.Error(), "credential")
					require.True(t, valuesSeen.Load(), "operation must inherit request_id")
					require.GreaterOrEqual(t, elapsed, 40*time.Millisecond)
					require.Less(t, elapsed, 600*time.Millisecond)
					if fault == "write" {
						require.True(t, writeFailed.Load(), "must hit a blocked TCP Write, not only response Read")
						require.Positive(t, unwritten.Load(), "TCP Write must return with unsent bytes")
					}
					if fault == "pool" {
						require.Zero(t, p.scriptCalls.Load())
						require.NoError(t, held.Close())
						held = nil
					}
					// No fault release occurs until the bounded result above has been received.
					p.close()
					breakerView(t, b, id, BreakerClosed, true)
					t.Logf("fault=%s earlier=%v elapsed=%s failed_write=%v bytes=%d all_proxy_workers_joined=true", fault, earlier, elapsed, writeFailed.Load(), written.Load())
				})
			}
		}
	}
}

func TestBreakerRedisPreCanceledAllOperationsHaveZeroAccess(t *testing.T) {
	b, c, id := breakerReal(t, breakerDefaults())
	permit := breakerAcquire(t, b, id)
	hook := &breakerHook{}
	c.AddHook(hook)
	cause := errors.New("custom cause")
	ctx, cancel := context.WithCancelCause(middleware.WithRequestID(breakerCtx, "pre-canceled"))
	cancel(cause)
	_, err := b.Inspect(ctx, id)
	require.ErrorIs(t, err, cause)
	_, err = b.Acquire(ctx, id, time.Second)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, b.Complete(ctx, permit, BreakerNeutral), cause)
	require.Zero(t, hook.calls.Load())
}
