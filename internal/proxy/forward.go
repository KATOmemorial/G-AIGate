package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"time"

	"gaigate/internal/provider"
)

// Never format causes: transport/read/close errors may contain credentials.
// All original causes remain available through errors.Is/As.
type forwardError struct {
	message string
	causes  []error
}

func (e *forwardError) Error() string   { return e.message }
func (e *forwardError) Unwrap() []error { return e.causes }

func failure(message string, causes ...error) error {
	return &forwardError{message: message, causes: causes}
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflect.ValueOf(value).IsNil()
	default:
		return false
	}
}

// Resolve an error-returning Flush without calling Flush during preflight.
// ResponseController.Flush stops at Gin's void Flush before reaching Unwrap.
// Body writes must still go through the original writer for Gin's accounting.
func errorFlusher(w http.ResponseWriter) (func() error, error) {
	for range 64 { // Malformed/cyclic wrapper chains fail without hanging.
		if nilValue(w) {
			break
		}
		if f, ok := w.(interface{ FlushError() error }); ok {
			return f.FlushError, nil
		}
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		w = unwrapper.Unwrap()
	}
	return nil, failure("proxy: writer cannot expose Flush errors", http.ErrNotSupported)
}

func canceled(ctx context.Context) (ResultKind, error) {
	if err := ctx.Err(); err != nil {
		kind := ResultClientCanceled
		if errors.Is(err, context.DeadlineExceeded) {
			kind = ResultOverallTimeout
		}
		return kind, failure("proxy: request context ended", err, context.Cause(ctx))
	}
	return "", nil
}

func (*forwarder) Forward(ctx context.Context, w http.ResponseWriter, open StreamOpener,
	opts ForwardOptions, onChunk func(provider.StreamEvent)) (result ForwardResult, err error) {
	started := time.Now()
	result = ForwardResult{Kind: ResultUpstreamError, FirstByteMs: -1}
	if ctx == nil || nilValue(w) || open == nil || opts.TTFBTimeout <= 0 || opts.OverallTimeout <= 0 {
		return result, failure("proxy: invalid forwarding arguments")
	}
	if kind, cause := canceled(ctx); cause != nil {
		result.Kind = kind
		return result, cause
	}
	flush, err := errorFlusher(w)
	if err != nil {
		return result, err
	}
	// TODO(15.2): Add the opener-inclusive TTFB/Overall budgets and interruptible
	// Write/Flush deadlines. This lifecycle currently inherits parent cancellation.
	attempt, cancel := context.WithCancel(ctx)
	var stream provider.Stream
	defer func() {
		cancel()
		var closeErr error
		if !nilValue(stream) {
			closeErr = stream.Close() // Sole owner: Close waits for the producer.
		}
		if kind, cause := canceled(ctx); cause != nil {
			result.Kind = kind
			err = failure("proxy: request context ended", cause, err)
		}
		if closeErr != nil {
			if err == nil {
				result.Kind = ResultStreamInterrupted
				err = failure("proxy: upstream cleanup failed", provider.ErrStreamInterrupted, closeErr)
			} else {
				err = failure("proxy: forwarding failed; upstream cleanup also failed", err, closeErr)
			}
		}
	}()
	stream, err = open(attempt)
	if kind, cause := canceled(ctx); cause != nil {
		result.Kind = kind
		return result, cause
	}
	if err != nil {
		return result, failure("proxy: upstream opening failed", err)
	}
	if nilValue(stream) {
		return result, failure("proxy: opener returned no stream", provider.ErrUpstreamUnavailable)
	}
	events := stream.Events()
	for {
		if kind, cause := canceled(ctx); cause != nil {
			result.Kind = kind
			return result, cause
		}
		var event provider.StreamEvent
		var ok bool
		select {
		case <-attempt.Done():
			result.Kind, err = canceled(ctx)
			return result, err
		case event, ok = <-events:
		}
		// Do not let select choose a queued event over an already canceled parent.
		if kind, cause := canceled(ctx); cause != nil {
			result.Kind = kind
			return result, cause
		}
		if !ok {
			result.Kind = ResultStreamInterrupted
			return result, failure("proxy: stream ended before Done", provider.ErrStreamInterrupted, io.EOF)
		}
		if event.Kind == provider.Error {
			result.Kind = ResultStreamInterrupted
			return result, failure("proxy: upstream stream interrupted", provider.ErrStreamInterrupted, event.Err)
		}
		if !validEvent(event) {
			result.Kind = ResultStreamInterrupted
			return result, failure("proxy: invalid upstream event", provider.ErrStreamInterrupted)
		}
		if result.FirstByteMs < 0 {
			result.FirstByteMs = time.Since(started).Milliseconds()
		}
		var frame []byte
		if event.Kind == provider.Done {
			frame = []byte("data: [DONE]\n\n")
		} else {
			frame = dataFrame(event.Raw)
		}
		if kind, cause := canceled(ctx); cause != nil {
			result.Kind = kind
			return result, cause
		}
		if !result.WroteHeader {
			h := w.Header()
			h.Set("Content-Type", "text/event-stream; charset=utf-8")
			h.Set("Cache-Control", "no-cache")
			h.Set("X-Accel-Buffering", "no")
			h.Del("Content-Length")
			// Native WriteHeader commits; Gin only stages its status. Mark before
			// either operation can commit, and never roll back after a failure.
			result.WroteHeader = true
			w.WriteHeader(http.StatusOK)
		}
		n, writeErr := w.Write(frame)
		if writeErr == nil && n != len(frame) {
			writeErr = io.ErrShortWrite
		}
		if writeErr == nil {
			writeErr = flush()
		}
		if writeErr != nil {
			result.Kind = ResultWriteError
			return result, failure("proxy: downstream output failed", writeErr)
		}
		if event.Kind == provider.Done {
			result.Kind = ResultSuccess
			return result, nil
		}
		if event.Kind == provider.Chunk {
			result.Chunks++
		} else {
			usage := *event.Usage
			result.Usage = &usage // Isolated from the provider and the callback.
		}
		if onChunk != nil {
			onChunk(event)
		}
	}
}

func validEvent(event provider.StreamEvent) bool {
	if event.Err != nil {
		return false
	}
	switch event.Kind {
	case provider.Done:
		return event.Usage == nil
	case provider.Chunk, provider.UsageDone:
		raw := bytes.TrimSpace(event.Raw)
		if len(raw) == 0 || raw[0] != '{' || !json.Valid(raw) {
			return false
		}
		if event.Kind == provider.Chunk {
			return event.Usage == nil
		}
		return event.Usage != nil && event.Usage.InputTokens >= 0 && event.Usage.OutputTokens >= 0
	default:
		return false
	}
}

func dataFrame(raw []byte) []byte {
	frame := make([]byte, 0, len(raw)+8)
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		frame = append(frame, "data: "...)
		frame = append(frame, line...)
		frame = append(frame, '\n')
	}
	return append(frame, '\n')
}
