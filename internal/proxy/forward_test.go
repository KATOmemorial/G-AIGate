package proxy_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/proxy"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

var options = proxy.ForwardOptions{TTFBTimeout: time.Second, OverallTimeout: time.Minute}

type markerKey struct{}

type outputWriter struct {
	header, committed http.Header
	body              bytes.Buffer
	status            int
	writes, flushes   int
	failWrite, short  int
	failFlush         int
	cause             error
	beforeWrite       func()
}

func newOutput() *outputWriter { return &outputWriter{header: make(http.Header)} }

// Mechanism-only fixture: real deadline behavior is tested by deadlineOutput.
func (*outputWriter) SetWriteDeadline(time.Time) error { return nil }
func (w *outputWriter) Header() http.Header            { return w.header }
func (w *outputWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status, w.committed = status, w.header.Clone()
	}
}
func (w *outputWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.beforeWrite != nil {
		w.beforeWrite()
	}
	if w.status == 0 {
		w.WriteHeader(200)
	}
	if w.writes == w.failWrite {
		return 0, w.cause
	}
	if w.writes == w.short {
		return w.body.Write(p[:len(p)-1])
	}
	return w.body.Write(p)
}
func (w *outputWriter) FlushError() error {
	w.flushes++
	if w.flushes == w.failFlush {
		return w.cause
	}
	return nil
}

type eventStream struct {
	events   chan provider.StreamEvent
	closes   int
	onClose  func()
	closeErr error
}

func scripted(events ...provider.StreamEvent) *eventStream {
	s := &eventStream{events: make(chan provider.StreamEvent, len(events))}
	for _, e := range events {
		s.events <- e
	}
	close(s.events)
	return s
}
func (s *eventStream) Events() <-chan provider.StreamEvent { return s.events }
func (s *eventStream) Close() error {
	s.closes++
	if s.onClose != nil {
		s.onClose()
	}
	return s.closeErr
}
func (s *eventStream) open(context.Context) (provider.Stream, error) { return s, nil }
func chunk(raw string) provider.StreamEvent {
	return provider.StreamEvent{Kind: provider.Chunk, Raw: []byte(raw)}
}
func done() provider.StreamEvent {
	return provider.StreamEvent{Kind: provider.Done, Raw: []byte("[DONE]")}
}
func usage(raw string, in, out int64) provider.StreamEvent {
	return provider.StreamEvent{Kind: provider.UsageDone, Raw: []byte(raw), Usage: &provider.Usage{InputTokens: in, OutputTokens: out}}
}

func assertUncommitted(t *testing.T, w *outputWriter, result proxy.ForwardResult) {
	t.Helper()
	require.False(t, result.WroteHeader)
	require.EqualValues(t, -1, result.FirstByteMs)
	require.Zero(t, w.status)
	require.Zero(t, w.writes)
	require.Zero(t, w.flushes)
	require.Empty(t, w.body.String())
	require.Empty(t, w.header.Get("Content-Type"))
}

// Decode data fields independently from the implementation, including the
// optional single space rule and the event's final newline removal.
func payloads(t *testing.T, wire string) []string {
	t.Helper()
	require.True(t, strings.HasSuffix(wire, "\n\n"))
	var result []string
	for _, event := range strings.Split(strings.TrimSuffix(wire, "\n\n"), "\n\n") {
		var data []string
		for _, line := range strings.Split(event, "\n") {
			field, value, found := strings.Cut(line, ":")
			require.True(t, found)
			require.Equal(t, "data", field)
			data = append(data, strings.TrimPrefix(value, " "))
		}
		result = append(result, strings.Join(data, "\n"))
	}
	return result
}

func TestForwardFramesHeadersAndCallbacks(t *testing.T) {
	first := " \n{\n  \"choices\": [{\"delta\": {\"content\": \"汉字  \\\\n\"}}]\n}\n "
	u := usage(`{"choices":[],"usage":{"prompt_tokens":12,"completion_tokens":34}}`, 12, 34)
	s := scripted(chunk(first), chunk(`{"choices":[]}`), u, done(), chunk(`{"must":"remain unread"}`))
	w := newOutput()
	w.header.Set("Content-Length", "999")
	w.header.Set("X-Request-Id", "req_test")
	var callbacks []provider.StreamEventKind
	result, err := proxy.NewForwarder().Forward(t.Context(), w, s.open, options, func(e provider.StreamEvent) {
		require.Equal(t, len(callbacks)+1, w.flushes, "callback follows each successful Flush synchronously")
		callbacks = append(callbacks, e.Kind)
	})
	require.NoError(t, err)
	require.Equal(t, proxy.ResultSuccess, result.Kind)
	require.True(t, result.WroteHeader)
	require.GreaterOrEqual(t, result.FirstByteMs, int64(0))
	require.Equal(t, 2, result.Chunks)
	require.Equal(t, u.Usage, result.Usage)
	require.NotSame(t, u.Usage, result.Usage)
	require.Equal(t, []provider.StreamEventKind{provider.Chunk, provider.Chunk, provider.UsageDone}, callbacks)
	require.Equal(t, []string{first, `{"choices":[]}`, string(u.Raw), "[DONE]"}, payloads(t, w.body.String()))
	require.Equal(t, 4, w.writes)
	require.Equal(t, 4, w.flushes)
	require.Equal(t, 1, len(s.events), "Done stops consumption, not just output")
	require.Equal(t, 1, s.closes)
	require.Equal(t, "text/event-stream; charset=utf-8", w.committed.Get("Content-Type"))
	require.Equal(t, "no-cache", w.committed.Get("Cache-Control"))
	require.Equal(t, "no", w.committed.Get("X-Accel-Buffering"))
	require.Empty(t, w.committed.Get("Content-Length"))
	require.Empty(t, w.committed.Get("Content-Encoding"))
	require.Equal(t, "req_test", w.committed.Get("X-Request-Id"))
}

func TestForwardDoneOnlyAndNilCallback(t *testing.T) {
	s, w := scripted(done(), done()), newOutput()
	result, err := proxy.NewForwarder().Forward(t.Context(), w, s.open, options, nil)
	require.NoError(t, err)
	require.Equal(t, proxy.ResultSuccess, result.Kind)
	require.True(t, result.WroteHeader)
	require.Zero(t, result.Chunks)
	require.Nil(t, result.Usage)
	require.Equal(t, "data: [DONE]\n\n", w.body.String())
	require.Equal(t, 1, len(s.events))
	require.Equal(t, 1, s.closes)
}

func TestForwardUsageCopiesAndDuplicates(t *testing.T) {
	u1 := usage(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2}}`, 1, 2)
	u2 := usage(`{"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4}}`, 3, 4)
	s, w := scripted(u1, u2, done()), newOutput()
	var retained []*provider.Usage
	result, err := proxy.NewForwarder().Forward(t.Context(), w, s.open, options, func(e provider.StreamEvent) {
		retained = append(retained, e.Usage)
		e.Usage.InputTokens = 900 // Must not mutate the saved result.
	})
	require.NoError(t, err)
	require.Equal(t, &provider.Usage{InputTokens: 3, OutputTokens: 4}, result.Usage)
	retained[1].OutputTokens = 999
	require.EqualValues(t, 4, result.Usage.OutputTokens)
	require.Len(t, payloads(t, w.body.String()), 3)
	require.Zero(t, result.Chunks)
}

func TestForwardInterruptionsNeverInventDone(t *testing.T) {
	cause := &url.Error{Op: "read", URL: "https://user:secret@host/?key=sk-private", Err: io.ErrUnexpectedEOF}
	for _, tc := range []struct {
		name       string
		events     []provider.StreamEvent
		wantChunks int
		cause      error
	}{
		{"first Error", []provider.StreamEvent{{Kind: provider.Error, Err: cause}, done()}, 0, cause},
		{"empty channel", nil, 0, io.EOF},
		{"after content Error", []provider.StreamEvent{chunk(`{}`), {Kind: provider.Error, Err: cause}, done()}, 1, cause},
		{"missing Done EOF", []provider.StreamEvent{chunk(`{}`)}, 1, io.EOF},
		{"Error with nil cause", []provider.StreamEvent{{Kind: provider.Error}, done()}, 0, nil},
		{"unknown kind", []provider.StreamEvent{{Kind: 99}, done()}, 0, nil},
		{"empty Chunk", []provider.StreamEvent{chunk(""), done()}, 0, nil},
		{"invalid JSON", []provider.StreamEvent{chunk("bad"), done()}, 0, nil},
		{"JSON array", []provider.StreamEvent{chunk("[]"), done()}, 0, nil},
		{"missing usage", []provider.StreamEvent{{Kind: provider.UsageDone, Raw: []byte(`{}`)}, done()}, 0, nil},
		{"negative usage", []provider.StreamEvent{usage(`{}`, -1, 2), done()}, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, w := scripted(tc.events...), newOutput()
			callbacks := 0
			result, err := proxy.NewForwarder().Forward(t.Context(), w, s.open, options, func(provider.StreamEvent) { callbacks++ })
			require.ErrorIs(t, err, provider.ErrStreamInterrupted)
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
			}
			require.Equal(t, proxy.ResultStreamInterrupted, result.Kind)
			require.Equal(t, tc.wantChunks, result.Chunks)
			require.Equal(t, tc.wantChunks, callbacks)
			require.Nil(t, result.Usage)
			require.NotContains(t, w.body.String(), "[DONE]")
			require.Equal(t, 1, s.closes)
			if tc.wantChunks == 0 {
				assertUncommitted(t, w, result)
			} else {
				require.True(t, result.WroteHeader)
			}
			require.NotContains(t, fmt.Sprint(err), "secret")
			require.NotContains(t, fmt.Sprint(err), "sk-private")
			if tc.cause == cause {
				var original *url.Error
				require.ErrorAs(t, err, &original)
				require.Same(t, cause, original)
			}
		})
	}
}

func TestForwardOutputFailureStopsConsumptionAndCallbacks(t *testing.T) {
	cause := errors.New("downstream failed")
	for _, tc := range []struct {
		name                                                           string
		fail, short, flush, wantWrites, wantFlushes, callbacks, chunks int
		wantUsage                                                      bool
	}{
		{"first Write", 1, 0, 0, 1, 0, 0, 0, false},
		{"short Write", 0, 1, 0, 1, 0, 0, 0, false},
		{"second Write", 2, 0, 0, 2, 1, 1, 1, false},
		{"first Flush", 0, 0, 1, 1, 1, 0, 0, false},
		{"usage Write", 3, 0, 0, 3, 2, 2, 2, false},
		{"usage Flush", 0, 0, 3, 3, 3, 2, 2, false},
		{"Done Flush", 0, 0, 4, 4, 4, 3, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := scripted(chunk(`{"n":1}`), chunk(`{"n":2}`), usage(`{"choices":[],"usage":{"prompt_tokens":8,"completion_tokens":9}}`, 8, 9), done(), chunk(`{"later":true}`))
			w := newOutput()
			w.failWrite, w.short, w.failFlush, w.cause = tc.fail, tc.short, tc.flush, cause
			callbacks := 0
			result, err := proxy.NewForwarder().Forward(t.Context(), w, s.open, options, func(provider.StreamEvent) { callbacks++ })
			if tc.short != 0 {
				require.ErrorIs(t, err, io.ErrShortWrite)
			} else {
				require.ErrorIs(t, err, cause)
			}
			require.Equal(t, proxy.ResultWriteError, result.Kind)
			require.True(t, result.WroteHeader, "failed first attempt cannot roll commitment back")
			require.GreaterOrEqual(t, result.FirstByteMs, int64(0))
			require.Equal(t, tc.wantWrites, w.writes)
			require.Equal(t, tc.wantFlushes, w.flushes)
			require.Equal(t, tc.callbacks, callbacks)
			require.Equal(t, tc.chunks, result.Chunks)
			require.Equal(t, tc.wantUsage, result.Usage != nil)
			require.Equal(t, 5-tc.wantWrites, len(s.events), "no event consumed after failed output")
			require.Equal(t, 1, s.closes)
			if tc.wantWrites < 4 {
				require.NotContains(t, w.body.String(), "[DONE]")
			}
		})
	}
}

type voidFlusher struct {
	http.ResponseWriter
	flushes int
}

func (w *voidFlusher) Flush() { w.flushes++ }

func TestForwardRejectsArgumentsAndUnsupportedWriterBeforeOpen(t *testing.T) {
	for _, name := range []string{"nil writer", "typed nil writer", "nil opener", "nil ctx", "TTFB zero", "Overall negative", "void Flush only"} {
		t.Run(name, func(t *testing.T) {
			w := newOutput()
			var writer http.ResponseWriter = w
			ctx, opts, calls := t.Context(), options, 0
			var open proxy.StreamOpener = func(context.Context) (provider.Stream, error) { calls++; return scripted(done()), nil }
			switch name {
			case "nil writer":
				writer = nil
			case "typed nil writer":
				writer = (*outputWriter)(nil)
			case "nil opener":
				open = nil
			case "nil ctx":
				ctx = nil
			case "TTFB zero":
				opts.TTFBTimeout = 0
			case "Overall negative":
				opts.OverallTimeout = -time.Second
			case "void Flush only":
				writer = &voidFlusher{ResponseWriter: w}
			}
			result, err := proxy.NewForwarder().Forward(ctx, writer, open, opts, nil)
			require.Error(t, err)
			require.Zero(t, calls)
			assertUncommitted(t, w, result)
			if name == "void Flush only" {
				require.ErrorIs(t, err, http.ErrNotSupported)
			}
		})
	}
}

func TestForwardOpenerAndCloseFailures(t *testing.T) {
	opening := &url.Error{Op: "open", URL: "https://user:secret@host", Err: io.ErrClosedPipe}
	closing := errors.New("close sk-private")
	for _, name := range []string{"opener error", "opener stream plus error", "nil stream", "typed nil stream", "success close failure", "event and close failure", "Write and close failure"} {
		t.Run(name, func(t *testing.T) {
			w, s := newOutput(), scripted(done())
			var attempt context.Context
			open := func(ctx context.Context) (provider.Stream, error) {
				attempt = ctx
				s.onClose = func() { require.ErrorIs(t, ctx.Err(), context.Canceled) }
				switch name {
				case "opener error":
					return nil, opening
				case "opener stream plus error":
					return s, opening
				case "nil stream":
					return nil, nil
				case "typed nil stream":
					return (*eventStream)(nil), nil
				}
				s.closeErr = closing
				if name == "event and close failure" {
					s.events = scripted(provider.StreamEvent{Kind: provider.Error, Err: opening}).events
				}
				if name == "Write and close failure" {
					w.failWrite, w.cause = 1, opening
				}
				return s, nil
			}
			result, err := proxy.NewForwarder().Forward(t.Context(), w, open, options, nil)
			require.Error(t, err)
			require.ErrorIs(t, attempt.Err(), context.Canceled)
			if strings.Contains(name, "opener") || name == "event and close failure" || name == "Write and close failure" {
				require.ErrorIs(t, err, opening)
			}
			if strings.Contains(name, "close failure") {
				require.ErrorIs(t, err, closing)
			}
			if strings.Contains(name, "nil stream") || name == "opener error" {
				require.Zero(t, s.closes)
			} else {
				require.Equal(t, 1, s.closes)
			}
			if name == "success close failure" || name == "event and close failure" {
				require.Equal(t, proxy.ResultStreamInterrupted, result.Kind)
			}
			if name == "Write and close failure" {
				require.Equal(t, proxy.ResultWriteError, result.Kind)
			}
			require.NotContains(t, err.Error(), "secret")
			require.NotContains(t, err.Error(), "sk-private")
		})
	}
}

func TestForwardParentCancellationAtBoundaries(t *testing.T) {
	for _, stage := range []string{"before opener", "opener", "ready event", "empty receive", "after Chunk"} {
		t.Run(stage, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancelCause(context.WithValue(t.Context(), markerKey{}, "request marker"))
				defer cancel(nil)
				cause := errors.New("parent reason")
				w, s := newOutput(), scripted(chunk(`{}`), done())
				if stage == "empty receive" {
					s.events = make(chan provider.StreamEvent)
				}
				if stage == "before opener" {
					cancel(cause)
				}
				opened := false
				var group errgroup.Group
				group.Go(func() error {
					result, err := proxy.NewForwarder().Forward(ctx, w, func(attempt context.Context) (provider.Stream, error) {
						opened = true
						require.NotSame(t, ctx, attempt)
						require.Equal(t, "request marker", attempt.Value(markerKey{}))
						if stage == "opener" {
							<-attempt.Done()
							return nil, io.ErrUnexpectedEOF
						}
						if stage == "ready event" {
							cancel(cause)
						}
						return s, nil
					}, options, func(provider.StreamEvent) {
						if stage == "after Chunk" {
							cancel(cause)
						}
					})
					require.ErrorIs(t, err, context.Canceled)
					require.ErrorIs(t, err, cause)
					require.Equal(t, proxy.ResultClientCanceled, result.Kind)
					if stage == "after Chunk" {
						require.True(t, result.WroteHeader)
						require.Equal(t, 1, result.Chunks)
						require.Equal(t, 1, len(s.events))
					} else {
						assertUncommitted(t, w, result)
					}
					return nil
				})
				synctest.Wait()
				cancel(cause)
				require.NoError(t, group.Wait())
				require.Equal(t, stage != "before opener", opened)
				if stage != "before opener" && stage != "opener" {
					require.Equal(t, 1, s.closes)
				}
			})
		})
	}
}

func TestForwardParentDeadlineAndFirstByteTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		w := newOutput()
		result, err := proxy.NewForwarder().Forward(ctx, w, func(attempt context.Context) (provider.Stream, error) { <-attempt.Done(); return nil, io.EOF }, options, nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, proxy.ResultOverallTimeout, result.Kind)
		assertUncommitted(t, w, result)
	})
	synctest.Test(t, func(t *testing.T) {
		s, w := scripted(done()), newOutput()
		s.events = make(chan provider.StreamEvent)
		var producer errgroup.Group
		s.onClose = func() { require.NoError(t, producer.Wait()) }
		w.beforeWrite = func() { time.Sleep(100 * time.Millisecond) }
		result, err := proxy.NewForwarder().Forward(t.Context(), w, func(context.Context) (provider.Stream, error) {
			time.Sleep(40 * time.Millisecond)
			producer.Go(func() error {
				time.Sleep(60 * time.Millisecond)
				s.events <- done()
				close(s.events)
				return nil
			})
			return s, nil
		}, options, nil)
		require.NoError(t, err)
		require.EqualValues(t, 100, result.FirstByteMs, "includes opener and first event wait once, excludes downstream write delay")
	})
}

func TestForwardWaitsForCloseAndWorkerExit(t *testing.T) {
	for _, path := range []string{"Done", "event Error", "EOF", "Write", "Flush", "cancel", "opener stream and error"} {
		t.Run(path, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				s, w := scripted(done()), newOutput()
				if path == "event Error" {
					s = scripted(provider.StreamEvent{Kind: provider.Error, Err: io.ErrUnexpectedEOF})
				}
				if path == "EOF" {
					s = scripted()
				}
				if path == "Write" {
					w.failWrite, w.cause = 1, io.ErrClosedPipe
				}
				if path == "Flush" {
					w.failFlush, w.cause = 1, io.ErrClosedPipe
				}
				release, exited := make(chan struct{}), make(chan struct{})
				var worker, forwarding errgroup.Group
				var returned atomic.Bool
				s.onClose = func() { require.NoError(t, worker.Wait()) }
				forwarding.Go(func() error {
					_, err := proxy.NewForwarder().Forward(ctx, w, func(attempt context.Context) (provider.Stream, error) {
						worker.Go(func() error { <-attempt.Done(); <-release; close(exited); return nil })
						if path == "cancel" {
							cancel()
						}
						if path == "opener stream and error" {
							return s, io.ErrUnexpectedEOF
						}
						return s, nil
					}, options, nil)
					if path == "Done" {
						require.NoError(t, err)
					} else {
						require.Error(t, err)
					}
					returned.Store(true)
					return nil
				})
				synctest.Wait()
				require.False(t, returned.Load(), "Forward must wait at the Close barrier")
				close(release)
				require.NoError(t, forwarding.Wait())
				select {
				case <-exited:
				default:
					t.Fatal("worker still running after return")
				}
				require.Equal(t, 1, s.closes)
			})
		})
	}
}
