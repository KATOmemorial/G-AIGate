package openai

import (
	"context"
	"io"
	"sync"

	"gaigate/internal/provider"

	"golang.org/x/sync/errgroup"
)

// The cause may contain credentials or upstream payload fragments. Keep the
// public text fixed; classification may still traverse both underlying errors.
type streamInterruptedError struct{ cause error }

func (*streamInterruptedError) Error() string { return "openai: upstream stream interrupted" }

func (e *streamInterruptedError) Unwrap() []error {
	return []error{provider.ErrStreamInterrupted, e.cause}
}

type eventStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	body   io.ReadCloser
	events chan provider.StreamEvent
	worker errgroup.Group

	closeOnce sync.Once
	closeErr  error
	stopWake  func() bool
	wakeDone  chan struct{}
}

func newStream(ctx context.Context, cancel context.CancelFunc, body io.ReadCloser, capacity int) *eventStream {
	s := &eventStream{ctx: ctx, cancel: cancel, body: body,
		events: make(chan provider.StreamEvent, capacity), wakeDone: make(chan struct{})}
	// A canceled custom transport body may not observe the HTTP request context.
	// Own this callback as well: the worker stops it or waits for its completion.
	s.stopWake = context.AfterFunc(ctx, func() {
		s.closeBody()
		close(s.wakeDone)
	})
	s.worker.Go(s.run)
	return s
}

func (s *eventStream) Events() <-chan provider.StreamEvent { return s.events }

func (s *eventStream) closeBody() {
	s.closeOnce.Do(func() { s.closeErr = s.body.Close() })
}

// Close is safe concurrently, including before the worker enters its read.
// The producer never calls Close: it performs cleanup without waiting on itself.
func (s *eventStream) Close() error {
	s.cancel()
	s.closeBody()
	_ = s.worker.Wait() // Worker failures are delivered as Error events.
	return s.closeErr
}

func (s *eventStream) run() error {
	defer func() {
		s.closeBody()
		s.cancel()
		if !s.stopWake() {
			<-s.wakeDone
		}
		close(s.events) // Only this worker owns closing Events.
	}()
	parser := newSSEParser(s.body)
	for s.ctx.Err() == nil {
		raw, err := parser.next()
		var event provider.StreamEvent
		if err == nil {
			event, err = parseEvent(raw)
		}
		if err != nil {
			event = provider.StreamEvent{Kind: provider.Error,
				Err: &streamInterruptedError{cause: err}}
		}
		if !s.send(event) || event.Kind == provider.Done || event.Kind == provider.Error {
			return nil
		}
	}
	return nil
}

func (s *eventStream) send(event provider.StreamEvent) bool {
	if s.ctx.Err() != nil {
		return false
	}
	// At most capacity queued events plus this one bounded in-flight event.
	// A blocked send prevents all further parsing; no dropping or extra workers.
	select {
	case s.events <- event:
		return true
	case <-s.ctx.Done():
		return false
	}
}
