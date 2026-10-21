package proxy_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/provider/openai"
	"gaigate/internal/proxy"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Each Read returns one complete small SSE frame. There is no read-ahead source
// queue: count observes the parser's actual demand rather than TCP send totals.
type demandBody struct {
	count, next   int
	reads, closes atomic.Int64
	closed        chan struct{}
	once          sync.Once
}

func (b *demandBody) Read(p []byte) (int, error) {
	select {
	case <-b.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	if b.next > b.count {
		return 0, io.EOF
	}
	frame := "data: [DONE]\n\n"
	if b.next < b.count {
		frame = fmt.Sprintf("data: {\"n\":%d}\n\n", b.next)
	}
	if len(p) < len(frame) {
		return 0, io.ErrShortBuffer
	}
	b.next++
	b.reads.Add(1)
	return copy(p, frame), nil
}

func (b *demandBody) Close() error {
	b.closes.Add(1)
	b.once.Do(func() { close(b.closed) })
	return nil
}

type gatedOutput struct {
	*deadlineOutput
	permits chan struct{}
	writing chan int
}

func (w *gatedOutput) Write(p []byte) (int, error) {
	w.writing <- w.writes + 1
	for {
		if err := w.outputError(); err != nil {
			return 0, err
		}
		w.mu.Lock()
		deadline, changed := w.deadline, w.changed
		w.mu.Unlock()
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-w.permits:
			timer.Stop()
			return w.outputWriter.Write(p)
		case <-timer.C:
		case <-changed:
			timer.Stop()
		}
	}
}

func TestForwardBackpressureStopsParserAndResumesWithoutLoss(t *testing.T) {
	for _, capacity := range []int{1, 64} {
		for _, stop := range []string{"drain", "parent cancel", "Overall"} {
			t.Run(fmt.Sprintf("C=%d/%s", capacity, stop), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					count := capacity + 6
					body := &demandBody{count: count, closed: make(chan struct{})}
					p, err := openai.New(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: body}, nil
					})}, capacity)
					require.NoError(t, err)
					w := &gatedOutput{deadlineOutput: newDeadlineOutput(""), permits: make(chan struct{}, count+1), writing: make(chan int, count+1)}
					var stream *countedStream
					var result proxy.ForwardResult
					var forwardErr error
					var forwarding errgroup.Group
					forwarding.Go(func() error {
						result, forwardErr = proxy.NewForwarder().Forward(ctx, w, func(attempt context.Context) (provider.Stream, error) {
							s, err := p.ChatStream(attempt, &provider.Endpoint{BaseURL: "http://fixture", Model: "m"}, upstreamRequest())
							if err != nil {
								return nil, err
							}
							stream = &countedStream{Stream: s}
							return stream, nil
						}, proxy.ForwardOptions{TTFBTimeout: time.Second, OverallTimeout: 2 * time.Second}, nil)
						return nil
					})
					require.Equal(t, 1, <-w.writing)
					synctest.Wait() // Forward in Write; producer durably blocked sending C+2.
					require.Equal(t, capacity, cap(stream.Events()))
					require.Equal(t, capacity, len(stream.Events()))
					require.EqualValues(t, capacity+2, body.reads.Load(), "C queued + one Forward payload + one parser send")
					w.permits <- struct{}{}
					require.Equal(t, 2, <-w.writing)
					synctest.Wait()
					require.Equal(t, capacity, len(stream.Events()))
					require.EqualValues(t, capacity+3, body.reads.Load(), "one downstream release permits exactly one more parse/read")
					trigger := time.Now()
					switch stop {
					case "drain":
						for range count {
							w.permits <- struct{}{}
						}
					case "parent cancel":
						cancel()
					case "Overall":
						trigger = trigger.Add(2 * time.Second)
					}
					require.NoError(t, forwarding.Wait())
					if stop == "drain" {
						require.NoError(t, forwardErr)
						require.Equal(t, proxy.ResultSuccess, result.Kind)
						require.Equal(t, count, result.Chunks)
						got := payloads(t, w.body.String())
						require.Len(t, got, count+1)
						for i := range count {
							require.Equal(t, fmt.Sprintf(`{"n":%d}`, i), got[i])
						}
						require.Equal(t, "[DONE]", got[count])
					} else {
						require.Error(t, forwardErr)
						require.Equal(t, 1, result.Chunks)
						want := proxy.ResultClientCanceled
						if stop == "Overall" {
							want = proxy.ResultOverallTimeout
							require.ErrorIs(t, forwardErr, context.DeadlineExceeded)
						} else {
							require.ErrorIs(t, forwardErr, context.Canceled)
						}
						require.Equal(t, want, result.Kind)
						require.LessOrEqual(t, time.Since(trigger), 500*time.Millisecond)
						t.Logf("C=%d stage=full Events + blocked Write trigger=%s exit=%s target=100ms limit=500ms", capacity, stop, time.Since(trigger))
					}
					require.Equal(t, 1, stream.closes)
					require.EqualValues(t, 1, body.closes.Load())
					for range stream.Events() {
					} // Closed channel proves producer and body callback joined.
					require.NoError(t, stream.Stream.Close())
					require.EqualValues(t, 1, body.closes.Load())
					deadlines := w.deadlines()
					cancel()
					time.Sleep(3 * time.Second)
					require.Equal(t, deadlines, w.deadlines(), "no downstream watcher survives Forward")
				})
			})
		}
	}
}
