package proxy_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type socketWrite struct {
	stage int32
	bytes int
	done  chan struct{}
	err   error // read after done
}

type socketProbe struct {
	stage                          atomic.Int32 // 1 = native response.Write, 2 = native FlushError
	current                        atomic.Pointer[socketWrite]
	failed                         atomic.Pointer[socketWrite]
	written, bodyBytes, sets, late atomic.Int64
	returned                       atomic.Bool
	mu                             sync.Mutex
	deadline                       time.Time
	writeEntered                   chan struct{}
	writeOnce                      sync.Once
}

type probingConn struct {
	net.Conn
	probe *socketProbe
}

func (c *probingConn) Write(p []byte) (int, error) {
	total := 0
	for len(p) > 0 {
		// Keep each real OS write bounded. Windows may otherwise accept one
		// giant overlapped send before TCP flow control blocks a later Flush.
		part := p[:min(len(p), 4096)]
		call := &socketWrite{stage: c.probe.stage.Load(), bytes: len(part), done: make(chan struct{})}
		c.probe.current.Store(call)
		n, err := c.Conn.Write(part) // Real kernel call, no test gate or fake delay.
		c.probe.written.Add(int64(n))
		call.err = err
		close(call.done)
		if err != nil {
			c.probe.failed.Store(call)
		}
		total += n
		if err != nil {
			return total, err
		}
		if n != len(part) {
			return total, io.ErrShortWrite
		}
		p = p[n:]
	}
	return total, nil
}
func (c *probingConn) SetWriteDeadline(deadline time.Time) error {
	if c.probe.returned.Load() {
		c.probe.late.Add(1)
	}
	c.probe.sets.Add(1)
	if !deadline.IsZero() && deadline.After(time.Now()) {
		c.probe.mu.Lock()
		c.probe.deadline = deadline
		c.probe.mu.Unlock()
	}
	return c.Conn.SetWriteDeadline(deadline)
}

type probingListener struct {
	net.Listener
	probe *socketProbe
}

func (l *probingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if err := c.(*net.TCPConn).SetWriteBuffer(4096); err != nil {
		_ = c.Close()
		return nil, err
	}
	return &probingConn{Conn: c, probe: l.probe}, nil
}

// This observer is below Gin/WrapWriter. It delegates every capability and body
// operation to the original HTTP writer; it cannot manufacture a network error.
type stageWriter struct {
	http.ResponseWriter
	probe *socketProbe
}

func (w *stageWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *stageWriter) Write(p []byte) (int, error) {
	w.probe.writeOnce.Do(func() { close(w.probe.writeEntered) })
	w.probe.stage.Store(1)
	defer w.probe.stage.Store(0)
	n, err := w.ResponseWriter.Write(p)
	w.probe.bodyBytes.Add(int64(n))
	return n, err
}
func (w *stageWriter) FlushError() error {
	w.probe.stage.Store(2)
	defer w.probe.stage.Store(0)
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func blockedSocketCall(t *testing.T, probe *socketProbe, stage int32) *socketWrite {
	t.Helper()
	limit := time.NewTimer(600 * time.Millisecond)
	defer limit.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-limit.C:
			t.Fatalf("no blocking kernel Write in stage %d; bytes completed=%d", stage, probe.written.Load())
		case <-tick.C:
			call := probe.current.Load()
			if call == nil || call.stage != stage {
				continue
			}
			stable := time.NewTimer(20 * time.Millisecond)
			select {
			case <-call.done:
				stable.Stop()
			case <-stable.C:
				// Entered actual net.Conn.Write and still has no completion. The
				// client has never read, with bounded send/receive socket buffers.
				if probe.current.Load() == call {
					return call
				}
			}
		}
	}
}

func TestRealTCPBlockedOutputCancelsEntireWrappedProviderChain(t *testing.T) {
	for _, mode := range []string{"stream Write", "stream Flush", "nonstream Write"} {
		for _, reason := range []string{"parent cancel", "TCP client close", "Overall"} {
			t.Run(mode+"/"+reason, func(t *testing.T) {
				streaming := mode != "nonstream Write"
				var upstreamCanceled atomic.Bool
				upstreamExited := make(chan struct{})
				payloadSize := 900 << 10
				if mode == "stream Flush" {
					payloadSize = 1024
				}
				raw := `{"padding":"` + strings.Repeat("x", payloadSize) + `"}`
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(upstreamExited)
					_, _ = io.Copy(io.Discard, r.Body)
					if !streaming {
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"padding":"`+strings.Repeat("x", 8<<20)+`"}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					for {
						select {
						case <-r.Context().Done():
							upstreamCanceled.Store(true)
							return
						default:
						}
						if _, err := io.WriteString(w, "data: "+raw+"\n\n"); err != nil {
							<-r.Context().Done()
							upstreamCanceled.Store(true)
							return
						}
						if err := http.NewResponseController(w).Flush(); err != nil {
							<-r.Context().Done()
							upstreamCanceled.Store(true)
							return
						}
					}
				}))
				defer upstream.Close()
				p, bodyCloses := actualAdapter(t, upstream.Client())
				probe := &socketProbe{writeEntered: make(chan struct{})}
				type outcome struct {
					result          proxy.ForwardResult
					err             error
					stream          *countedStream
					size, callbacks int
					written         bool
					status          int
				}
				finished := make(chan outcome, 1)
				handlerExited := make(chan struct{})
				parentReady := make(chan context.CancelCauseFunc, 1)
				cause := errors.New("fixture client request canceled")
				var logs bytes.Buffer
				logger := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
					if a.Key == slog.TimeKey {
						a.Key = "ts"
					}
					return a
				}}))
				router := gin.New()
				router.Use(middleware.RequestID(), middleware.AccessLog(logger))
				router.GET("/", func(c *gin.Context) {
					ctx, cancel := context.WithCancelCause(c.Request.Context())
					defer cancel(nil)
					parentReady <- cancel
					c.Writer = middleware.WrapWriter(c.Writer)
					got := outcome{}
					opts := proxy.ForwardOptions{TTFBTimeout: 3 * time.Second, OverallTimeout: 2 * time.Second}
					ep := &provider.Endpoint{BaseURL: upstream.URL, Model: "m"}
					if streaming {
						got.result, got.err = proxy.NewForwarder().Forward(ctx, c.Writer, func(attempt context.Context) (provider.Stream, error) {
							s, err := p.ChatStream(attempt, ep, upstreamRequest())
							if err != nil {
								return nil, err
							}
							got.stream = &countedStream{Stream: s}
							return got.stream, nil
						}, opts, func(provider.StreamEvent) { got.callbacks++ })
					} else {
						got.result, got.err = proxy.NewForwarder().ForwardNonStream(ctx, c.Writer, func(attempt context.Context) (*provider.ChatResponse, *provider.Usage, error) {
							return p.Chat(attempt, ep, upstreamRequest())
						}, opts)
					}
					got.size, got.written, got.status = c.Writer.Size(), c.Writer.Written(), c.Writer.Status()
					probe.returned.Store(true)
					finished <- got
					// This fixture deliberately has no production Recover. Final
					// gateway abort wiring belongs to 13.3, not to this test server.
					if got.err != nil && got.result.WroteHeader {
						panic(http.ErrAbortHandler)
					}
				})
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(handlerExited)
					router.ServeHTTP(&stageWriter{ResponseWriter: w, probe: probe}, r)
				}))
				server.Listener = &probingListener{Listener: server.Listener, probe: probe}
				server.Start()
				defer server.Close()
				client, err := net.DialTCP("tcp", nil, server.Listener.Addr().(*net.TCPAddr))
				require.NoError(t, err)
				defer client.Close()
				require.NoError(t, client.SetReadBuffer(4096))
				_, err = io.WriteString(client, "GET / HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n")
				require.NoError(t, err)
				cancel := <-parentReady
				defer cancel(nil)
				select {
				case <-probe.writeEntered:
				case <-time.After(2 * time.Second):
					t.Fatal("forwarder never entered downstream Write after payload validation")
				}
				stage := int32(1)
				if mode == "stream Flush" {
					stage = 2
				}
				blocked := blockedSocketCall(t, probe, stage)
				trigger := time.Now()
				switch reason {
				case "parent cancel":
					cancel(cause)
				case "TCP client close":
					require.NoError(t, client.Close())
				default:
					probe.mu.Lock()
					trigger = probe.deadline
					probe.mu.Unlock()
					require.False(t, trigger.IsZero())
					require.True(t, time.Now().Before(trigger), "must prove blocking before Overall fires")
				}
				var got outcome
				wait := time.Until(trigger) + 500*time.Millisecond
				select {
				case got = <-finished:
				case <-time.After(wait):
					t.Fatal("proxy did not exit within 500ms of trigger")
				}
				select {
				case <-handlerExited:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("handler did not exit")
				}
				select {
				case <-upstreamExited:
				case <-time.After(500 * time.Millisecond):
					t.Fatal("upstream did not exit")
				}
				elapsed := time.Since(trigger)
				require.LessOrEqual(t, elapsed, 500*time.Millisecond)
				<-blocked.done
				failed := probe.failed.Load()
				require.NotNil(t, failed, "a real socket call must report the terminating error")
				require.Equal(t, stage, failed.stage, "termination must occur in the same native Write/Flush phase")
				if reason == "TCP client close" {
					require.ErrorIs(t, got.err, failed.err, "socket close cause survives the full writer chain")
				} else {
					require.ErrorIs(t, got.err, os.ErrDeadlineExceeded, "bottom Write/Flush error survives Gin's void Flush")
				}
				want := proxy.ResultOverallTimeout
				if reason != "Overall" {
					want = proxy.ResultClientCanceled
					if reason == "parent cancel" {
						require.ErrorIs(t, got.err, cause)
					}
				} else {
					require.ErrorIs(t, got.err, context.DeadlineExceeded)
				}
				require.Equal(t, want, got.result.Kind)
				require.True(t, got.result.WroteHeader)
				require.True(t, got.written)
				require.Equal(t, 200, got.status)
				require.GreaterOrEqual(t, got.size, 0)
				require.EqualValues(t, probe.bodyBytes.Load(), got.size, "body writes must retain Gin accounting")
				require.GreaterOrEqual(t, got.result.FirstByteMs, int64(0))
				require.EqualValues(t, 1, bodyCloses.Load())
				if streaming {
					require.True(t, upstreamCanceled.Load())
					require.Equal(t, got.result.Chunks, got.callbacks)
					require.Equal(t, 1, got.stream.closes)
					for event := range got.stream.Events() {
						require.NotEqual(t, provider.Done, event.Kind)
					}
					require.NoError(t, got.stream.Stream.Close())
				} else {
					require.Zero(t, got.result.Chunks)
					require.Zero(t, got.callbacks)
				}
				require.Nil(t, got.result.Usage)
				require.Equal(t, 1, bytes.Count(logs.Bytes(), []byte(`"msg":"request_done"`)))
				require.Contains(t, logs.String(), `"status_code":200`)
				require.Contains(t, logs.String(), `"ts":`)
				require.Zero(t, probe.late.Load(), "no deadline operations after Forward returns")
				t.Logf("mode=%s trigger=%s socket call=%d bytes completed=%d Gin size=%d chunks=%d exit=%s target=100ms limit=500ms", mode, reason, blocked.bytes, probe.written.Load(), got.size, got.result.Chunks, elapsed)
			})
		}
	}
}
