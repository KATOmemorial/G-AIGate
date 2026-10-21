package proxy

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// The budget context outlives upstream cancellation during Close. In particular,
// successful cleanup must not wake the downstream as if the client canceled.
type lifetime struct {
	parent, ctx            context.Context
	cancel                 context.CancelFunc
	upstream               context.Context
	closeUpstream          context.CancelFunc
	started, overall, ttfb time.Time
	mu                     sync.Mutex
	first                  bool
	timer                  *time.Timer
	timerStopped           bool // only the forwarding owner writes this
	timerDone              chan struct{}
	decidedKind            ResultKind
	decidedErr             error
}

func newLifetime(parent context.Context, started time.Time, opts ForwardOptions) *lifetime {
	l := &lifetime{parent: parent, started: started, overall: started.Add(opts.OverallTimeout),
		ttfb: started.Add(opts.TTFBTimeout), timerDone: make(chan struct{})}
	l.ctx, l.cancel = context.WithDeadline(parent, l.overall)
	l.upstream, l.closeUpstream = context.WithCancel(l.ctx)
	l.timer = time.AfterFunc(time.Until(l.ttfb), func() {
		defer close(l.timerDone)
		l.mu.Lock()
		defer l.mu.Unlock()
		if !l.first {
			l.cancel()
		}
	})
	return l
}

// Check wall-clock deadlines as well as ctx.Err: timer dispatch and a ready
// Events receive may race. Never let select's random choice bypass a deadline.
func (l *lifetime) checkLocked(now time.Time) (ResultKind, error) {
	if l.decidedKind != "" {
		return l.decidedKind, failure("proxy: attempt cause determined", l.decidedErr, l.parent.Err(), context.Cause(l.parent))
	}
	if deadline, ok := l.parent.Deadline(); ok && !now.Before(deadline) {
		// The network deadline may wake Write before the parent's timer callback
		// is scheduled. Join that signal so WithDeadlineCause is not lost.
		<-l.parent.Done()
		return l.decideLocked(ResultOverallTimeout, failure("proxy: parent deadline reached", context.DeadlineExceeded, l.parent.Err(), context.Cause(l.parent)))
	}
	if kind, err := canceled(l.parent); err != nil {
		return l.decideLocked(kind, err)
	}
	if !now.Before(l.overall) {
		return l.decideLocked(ResultOverallTimeout, failure("proxy: overall budget exceeded", context.DeadlineExceeded))
	}
	if !l.first && !now.Before(l.ttfb) {
		return ResultTTFBTimeout, failure("proxy: first payload budget exceeded", context.DeadlineExceeded)
	}
	return "", nil
}

func (l *lifetime) decideLocked(kind ResultKind, err error) (ResultKind, error) {
	l.decidedKind, l.decidedErr = kind, err
	return kind, err
}

// A custom parent cause is independent evidence. Ordinary Canceled after an
// Overall socket timeout is ambiguous because net/http cancels on write errors.
func (l *lifetime) independentParent() bool {
	return l.parent.Err() != nil && context.Cause(l.parent) != context.Canceled
}

func (l *lifetime) parentDeadlineFirst() bool {
	d, ok := l.parent.Deadline()
	return ok && !l.overall.Before(d)
}

func outputTimeout(err error) bool {
	var timeout net.Error
	return errors.Is(err, os.ErrDeadlineExceeded) || errors.As(err, &timeout) && timeout.Timeout()
}

// Called before cleanup can sample net/http's derived cancellation (15.2 §3.4).
// Neither elapsed wall time alone nor an unrelated Timeout grants this override.
func (l *lifetime) outputFailed(d *downstream, outputErr error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.decidedKind != "" {
		return
	}
	if d.installed && d.fromOverall && !d.shortened && !l.parentDeadlineFirst() &&
		!time.Now().Before(l.overall) && outputTimeout(outputErr) && !l.independentParent() {
		_, _ = l.decideLocked(ResultOverallTimeout, failure("proxy: overall write deadline exceeded", context.DeadlineExceeded))
		return
	}
	_, _ = l.checkLocked(time.Now())
}

// The owned cancellation callback arbitrates before touching the socket. A
// parent observed before expiry is fixed; a late plain Canceled is left for the
// output-error evidence check, unless the budget context itself timed out first.
func (l *lifetime) wakeDeadline(d *downstream) time.Time {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if l.decidedKind == "" {
		switch {
		case now.Before(l.overall), l.independentParent(), l.parentDeadlineFirst():
			_, _ = l.checkLocked(now)
		case l.ctx.Err() == context.DeadlineExceeded:
			_, _ = l.decideLocked(ResultOverallTimeout, failure("proxy: overall budget exceeded", context.DeadlineExceeded))
		}
	}
	if now.Before(d.deadline) {
		d.shortened = true // Under the same lock as outputFailed's evidence check.
		return now
	}
	return d.deadline // Never extend an already elapsed write deadline.
}

func (l *lifetime) check() (ResultKind, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.checkLocked(time.Now())
}

func (l *lifetime) ready(result *ForwardResult) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if kind, err := l.checkLocked(now); err != nil {
		result.Kind = kind
		return err
	}
	if !l.first {
		l.first = true
		l.timerStopped = l.timer.Stop()
		result.FirstByteMs = now.Sub(l.started).Milliseconds()
	}
	return nil
}

func (l *lifetime) finish(result *ForwardResult, err *error) {
	if kind, cause := l.check(); cause != nil {
		result.Kind = kind
		*err = failure("proxy: attempt budget or request ended", cause, *err)
	}
	l.closeUpstream()
	l.cancel()
	if !l.timerStopped && !l.timer.Stop() {
		<-l.timerDone
	}
}

type downstream struct {
	setDeadline                       func(time.Time) error
	deadline                          time.Time
	stop                              func() bool
	done                              chan struct{}
	wakeErr                           error // read only after joining done
	installed, fromOverall, shortened bool  // provenance; callback/evidence use lifetime.mu
}

func writeDeadline(w http.ResponseWriter) (func(time.Time) error, error) {
	for range 64 {
		if nilValue(w) {
			break
		}
		if d, ok := w.(interface{ SetWriteDeadline(time.Time) error }); ok {
			return d.SetWriteDeadline, nil
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			break
		}
		w = u.Unwrap()
	}
	return nil, failure("proxy: writer cannot set write deadlines", http.ErrNotSupported)
}

func newDownstream(w http.ResponseWriter) (*downstream, error) {
	set, err := writeDeadline(w)
	if err != nil {
		return nil, err
	}
	// Probe without committing or installing an expiring deadline while waiting
	// for the opener. A precommit timeout must leave the handler able to reply.
	if err := set(time.Time{}); err != nil {
		return nil, failure("proxy: write deadline probe failed", err)
	}
	return &downstream{setDeadline: set}, nil
}

func (d *downstream) prepare(l *lifetime) error {
	d.deadline = l.overall
	d.fromOverall = true
	if parent, ok := l.parent.Deadline(); ok && !d.deadline.Before(parent) {
		d.deadline = parent
		d.fromOverall = false
	}
	if err := d.setDeadline(d.deadline); err != nil {
		return failure("proxy: cannot set downstream write deadline", err)
	}
	d.installed = true
	return nil
}

// Start immediately at the conservative commitment boundary. The callback only
// calls the discovered deadline setter (net.Conn in HTTP/1.1), never Gin state.
func (d *downstream) watch(l *lifetime) {
	d.done = make(chan struct{})
	d.stop = context.AfterFunc(l.ctx, func() {
		defer close(d.done)
		d.wakeErr = d.setDeadline(l.wakeDeadline(d))
	})
}

func (d *downstream) finish(l *lifetime, committed bool) error {
	if d.stop != nil && !d.stop() {
		<-d.done
	}
	if d.wakeErr != nil {
		return failure("proxy: cannot interrupt downstream output", d.wakeErr)
	}
	if d.deadline.IsZero() {
		return nil
	}
	if !committed {
		// Arming can yield until after expiry, before the commitment check.
		// No Write/Flush or writer watcher has run on this path, so there is
		// no sticky output error to repair: remove the installed connection
		// deadline so the handler can send its one error response.
		if err := d.setDeadline(time.Time{}); err != nil {
			return failure("proxy: cannot clear precommit write deadline", err)
		}
		return nil
	}
	// An expired response is not reusable; clearing cannot repair a sticky HTTP
	// write error. Never clear after a wake or an elapsed deadline.
	if l.ctx.Err() != nil || !time.Now().Before(d.deadline) {
		return nil
	}
	if err := d.setDeadline(time.Time{}); err != nil {
		return failure("proxy: cannot clear downstream write deadline", err)
	}
	// Cancellation can cross the clear call. Restore interruption before return,
	// without allowing any callback to touch the writer after this owner exits.
	if d.stop != nil && (l.ctx.Err() != nil || !time.Now().Before(d.deadline)) {
		if err := d.setDeadline(time.Now()); err != nil {
			return failure("proxy: cannot interrupt downstream output", err)
		}
	}
	return nil
}
