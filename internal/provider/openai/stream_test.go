package openai

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/provider"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// One body Read delivers at most one frame. Tests can observe durable blocking
// at an empty Read or a full event queue without sleeps or global goroutine counts.
type frameBody struct {
	parts       chan []byte
	closed      chan struct{}
	readEntered chan struct{}
	once        sync.Once
	closes      atomic.Int64
	reads       atomic.Int64
	remaining   []byte // owned solely by the reader
}

func newFrameBody(capacity int) *frameBody {
	return &frameBody{parts: make(chan []byte, capacity), closed: make(chan struct{}), readEntered: make(chan struct{}, capacity+1)}
}

func (b *frameBody) Read(p []byte) (int, error) {
	if len(b.remaining) == 0 {
		select {
		case b.readEntered <- struct{}{}:
		default:
		}
		select {
		case <-b.closed:
			return 0, io.ErrClosedPipe
		case part, ok := <-b.parts:
			if !ok {
				return 0, io.EOF
			}
			b.remaining = part
			b.reads.Add(1)
		}
	}
	n := copy(p, b.remaining)
	b.remaining = b.remaining[n:]
	return n, nil
}

func (b *frameBody) Close() error {
	b.closes.Add(1)
	b.once.Do(func() { close(b.closed) })
	return nil
}

func startTestStream(t *testing.T, body io.ReadCloser, capacity int) *eventStream {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	s := newStream(ctx, cancel, body, capacity)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func collect(s provider.Stream) []provider.StreamEvent {
	var events []provider.StreamEvent
	for event := range s.Events() {
		events = append(events, event)
	}
	return events
}

func TestStreamOrderDoneAndBodyOwnership(t *testing.T) {
	body := newFrameBody(8)
	body.parts <- []byte("data: {\"n\":1}\n\n")
	body.parts <- []byte("data: {\"n\":2}\n\n")
	body.parts <- []byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":34}}\n\n")
	body.parts <- []byte("data: [DONE]\n\n")
	body.parts <- []byte("data: bad-must-not-be-read\n\n")
	s := startTestStream(t, body, 2)
	events := collect(s)
	require.Len(t, events, 4)
	for i, kind := range []provider.StreamEventKind{provider.Chunk, provider.Chunk, provider.UsageDone, provider.Done} {
		require.Equal(t, kind, events[i].Kind)
		require.Nil(t, events[i].Err)
	}
	require.Equal(t, `{"n":1}`, string(events[0].Raw))
	require.Equal(t, `{"n":2}`, string(events[1].Raw))
	require.Equal(t, &provider.Usage{InputTokens: 12, OutputTokens: 34}, events[2].Usage)
	require.Equal(t, "[DONE]", string(events[3].Raw))
	require.NoError(t, s.Close())
	require.NoError(t, s.Close())
	require.EqualValues(t, 4, body.reads.Load(), "Done stops further body reads")
	require.EqualValues(t, 1, body.closes.Load())
}

func TestStreamFailuresProduceOneErrorWithoutDone(t *testing.T) {
	cause := errors.New("read failed")
	for _, tc := range []struct {
		name   string
		reader io.Reader
		cause  error
	}{
		{"empty EOF", strings.NewReader(""), io.EOF},
		{"missing Done", strings.NewReader("data: {}\n\n"), io.EOF},
		{"partial Done", strings.NewReader("data: [DONE]\n"), io.EOF},
		{"read error", io.MultiReader(strings.NewReader("data: {}\n\n"), failingReader{cause}), cause},
		{"bad JSON", strings.NewReader("data: {}\n\ndata: bad\n\ndata: [DONE]\n\n"), nil},
		{"bad usage", strings.NewReader("data: {\"usage\":{}}\n\ndata: [DONE]\n\n"), nil},
		{"oversize", strings.NewReader("data: " + strings.Repeat("x", maxEventInput) + "\n\n"), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := &observedBody{reader: tc.reader}
			s := startTestStream(t, body, 1)
			events := collect(s)
			require.NoError(t, s.Close())
			errorCount := 0
			for _, event := range events {
				require.NotEqual(t, provider.Done, event.Kind)
				if event.Kind == provider.Error {
					errorCount++
					require.ErrorIs(t, event.Err, provider.ErrStreamInterrupted)
					if tc.cause != nil {
						require.ErrorIs(t, event.Err, tc.cause)
					}
					require.Nil(t, event.Raw)
					require.Nil(t, event.Usage)
				}
			}
			require.Equal(t, 1, errorCount)
			require.Equal(t, provider.Error, events[len(events)-1].Kind)
			require.Equal(t, 1, body.closes)
		})
	}
}

func TestStreamBoundedQueueStopsParsingAndPreservesRaw(t *testing.T) {
	for _, capacity := range []int{1, 64} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				const extra = 5
				count := capacity + extra
				body := newFrameBody(count + 1)
				for i := range count {
					body.parts <- []byte(fmt.Sprintf("data: {\"n\":%d}\n\n", i))
				}
				body.parts <- []byte("data: [DONE]\n\n")
				s := startTestStream(t, body, capacity)
				synctest.Wait() // Worker is durably blocked on the full queue send.
				require.Equal(t, capacity, cap(s.events))
				require.Equal(t, capacity, len(s.events))
				require.EqualValues(t, capacity+1, body.reads.Load(), "one bounded in-flight payload, no further parse/read")
				first := <-s.events
				synctest.Wait()
				require.Equal(t, capacity, len(s.events))
				require.EqualValues(t, capacity+2, body.reads.Load(), "consumption resumes exactly one frame")
				events := append([]provider.StreamEvent{first}, collect(s)...)
				require.Len(t, events, count+1)
				for i := range count {
					require.Equal(t, provider.Chunk, events[i].Kind)
					require.Equal(t, fmt.Sprintf(`{"n":%d}`, i), string(events[i].Raw), "ordered, no loss, no buffer reuse")
				}
				require.Equal(t, provider.Done, events[count].Kind)
				require.NoError(t, s.Close())
				require.EqualValues(t, 1, body.closes.Load())
			})
		})
	}
}

func TestStreamCancelEmptyReadAndFullQueue(t *testing.T) {
	for _, full := range []bool{false, true} {
		for _, explicitClose := range []bool{false, true} {
			t.Run(fmt.Sprintf("full=%v/Close=%v", full, explicitClose), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					body := newFrameBody(3)
					if full {
						for range 3 {
							body.parts <- []byte("data: {}\n\n")
						}
					}
					ctx, cancel := context.WithCancel(t.Context())
					s := newStream(ctx, cancel, body, 1)
					defer func() { _ = s.Close() }()
					synctest.Wait()
					if full {
						require.Len(t, s.events, 1)
						require.EqualValues(t, 2, body.reads.Load())
					} else {
						require.Len(t, body.readEntered, 1, "worker entered empty Read")
					}
					if explicitClose {
						require.NoError(t, s.Close())
					} else {
						cancel()
					}
					synctest.Wait()
					require.NoError(t, s.worker.Wait())
					for event := range s.events {
						require.NotEqual(t, provider.Done, event.Kind)
					}
					require.EqualValues(t, 1, body.closes.Load())
				})
			})
		}
	}
}

type waitingCloseBody struct {
	*frameBody
	entered, release chan struct{}
	err              error
}

func (b *waitingCloseBody) Close() error {
	_ = b.frameBody.Close()
	close(b.entered)
	<-b.release
	return b.err
}

func TestConcurrentCloseWaitsForWorkerAndCallback(t *testing.T) {
	cause := errors.New("body close failure")
	body := &waitingCloseBody{frameBody: newFrameBody(0), entered: make(chan struct{}), release: make(chan struct{}), err: cause}
	s := startTestStream(t, body, 1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(body.release) }) }
	defer release()
	<-body.readEntered
	var closes errgroup.Group
	var returned atomic.Int64
	for range 16 {
		closes.Go(func() error {
			err := s.Close()
			if !errors.Is(err, cause) {
				return fmt.Errorf("wrong close result: %v", err)
			}
			returned.Add(1)
			return nil
		})
	}
	<-body.entered
	// The body is held at a named cleanup barrier. Mutex/Once waiters are
	// not durable blocks in synctest, so this case uses explicit barriers.
	require.Zero(t, returned.Load(), "Close must not return before body/worker/callback cleanup")
	started := time.Now()
	release()
	require.NoError(t, closes.Wait())
	require.Less(t, time.Since(started), 500*time.Millisecond)
	require.EqualValues(t, 16, returned.Load())
	_, open := <-s.Events()
	require.False(t, open)
	require.EqualValues(t, 1, body.closes.Load())
}

func TestStreamCloseBeforeReadingAndAfterCompletion(t *testing.T) {
	for range 32 {
		body := newFrameBody(0)
		ctx, cancel := context.WithCancel(t.Context())
		s := newStream(ctx, cancel, body, 1)
		require.NoError(t, s.Close())
		_, open := <-s.Events()
		require.False(t, open)
		require.EqualValues(t, 1, body.closes.Load())
	}
}
