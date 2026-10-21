package proxy

import (
	"context"
	"net/http"
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
	if kind, err := canceled(l.parent); err != nil {
		return kind, err
	}
	if deadline, ok := l.parent.Deadline(); ok && !now.Before(deadline) {
		// The network deadline may wake Write before the parent's timer callback
		// is scheduled. Join that signal so WithDeadlineCause is not lost.
		<-l.parent.Done()
		return canceled(l.parent)
	}
	if !now.Before(l.overall) {
		return ResultOverallTimeout, failure("proxy: overall budget exceeded", context.DeadlineExceeded)
	}
	if !l.first && !now.Before(l.ttfb) {
		return ResultTTFBTimeout, failure("proxy: first payload budget exceeded", context.DeadlineExceeded)
	}
	return "", nil
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
	setDeadline func(time.Time) error
	deadline    time.Time
	stop        func() bool
	done        chan struct{}
	wakeErr     error // read only after joining done
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
	if parent, ok := l.parent.Deadline(); ok && parent.Before(d.deadline) {
		d.deadline = parent
	}
	if err := d.setDeadline(d.deadline); err != nil {
		return failure("proxy: cannot set downstream write deadline", err)
	}
	return nil
}

// Start immediately at the conservative commitment boundary. The callback only
// calls the discovered deadline setter (net.Conn in HTTP/1.1), never Gin state.
func (d *downstream) watch(l *lifetime) {
	d.done = make(chan struct{})
	d.stop = context.AfterFunc(l.ctx, func() {
		defer close(d.done)
		d.wakeErr = d.setDeadline(time.Now())
	})
}

func (d *downstream) finish(l *lifetime) error {
	if d.stop != nil && !d.stop() {
		<-d.done
	}
	if d.wakeErr != nil {
		return failure("proxy: cannot interrupt downstream output", d.wakeErr)
	}
	if d.deadline.IsZero() {
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
