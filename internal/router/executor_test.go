package router

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gaigate/internal/access"
	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/proxy"
	"gaigate/internal/store"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// All hooks are fixture-local and synchronous. They model specific boundaries,
// not TCP, Redis, or the production gateway assembly.
type executionFixture struct {
	t                        *testing.T
	trace                    []string
	results                  []proxy.ForwardResult
	errors                   []error
	opens, forwards, samples int
	outcomes                 []BreakerOutcome
	reports                  []bool
	leases                   []time.Duration
	contexts                 []context.Context
	durations                []time.Duration
	available                func(context.Context, int64) bool
	acquire                  func(context.Context, int64) (*Permit, error)
	pick                     func(context.Context, *store.Channel) (*provider.Endpoint, error)
	complete                 func(context.Context)
	report                   func(context.Context)
	chat                     func(context.Context, *provider.Endpoint, *provider.ChatRequest)
	forward                  func(context.Context)
	beforeOpen               func(context.Context)
	close                    func()
	observe                  func(context.Context)
	logs                     bytes.Buffer
}

func (f *executionFixture) IsAvailable(ctx context.Context, id int64) bool {
	f.trace = append(f.trace, fmt.Sprint("health", id))
	if f.available != nil {
		return f.available(ctx, id)
	}
	return true
}
func (f *executionFixture) ReportRequestResult(ctx context.Context, id int64, ok bool) {
	f.trace = append(f.trace, fmt.Sprint("report", id))
	f.reports = append(f.reports, ok)
	f.contexts = append(f.contexts, ctx)
	if f.report != nil {
		f.report(ctx)
	}
}
func (f *executionFixture) Inspect(context.Context, int64) (BreakerView, error) {
	panic("executor must not inspect")
}
func (f *executionFixture) Acquire(ctx context.Context, id int64, lease time.Duration) (*Permit, error) {
	f.trace = append(f.trace, fmt.Sprint("acquire", id))
	f.leases = append(f.leases, lease)
	if f.acquire != nil {
		return f.acquire(ctx, id)
	}
	return &Permit{channelID: id}, nil
}
func (f *executionFixture) Complete(ctx context.Context, p *Permit, outcome BreakerOutcome) error {
	f.trace = append(f.trace, fmt.Sprint("complete", p.channelID))
	f.outcomes = append(f.outcomes, outcome)
	f.contexts = append(f.contexts, ctx)
	if f.complete != nil {
		f.complete(ctx)
	}
	return errors.New("private feedback secret")
}
func endpointFor(c *store.Channel) *provider.Endpoint {
	return &provider.Endpoint{ChannelID: int64(c.ID), Provider: c.Provider, BaseURL: c.BaseURL, APIKey: "private-key", Model: "mapped"}
}
func (f *executionFixture) Pick(ctx context.Context, c *store.Channel, _ string) (*provider.Endpoint, error) {
	f.trace = append(f.trace, fmt.Sprint("pick", c.ID))
	if f.pick != nil {
		return f.pick(ctx, c)
	}
	return endpointFor(c), nil
}
func (f *executionFixture) Name() string { return "openai" }
func (f *executionFixture) Chat(ctx context.Context, ep *provider.Endpoint, req *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
	f.trace = append(f.trace, fmt.Sprint("open", ep.ChannelID))
	f.opens++
	require.Equal(f.t, ep.ChannelID, middleware.ChannelIDFromContext(ctx))
	require.NoError(f.t, ctx.Err())
	if f.chat != nil {
		f.chat(ctx, ep, req)
	}
	return &provider.ChatResponse{Body: json.RawMessage(`{}`)}, nil, nil
}
func (f *executionFixture) ChatStream(ctx context.Context, ep *provider.Endpoint, req *provider.ChatRequest) (provider.Stream, error) {
	_, _, err := f.Chat(ctx, ep, req)
	return &executionStream{f: f}, err
}

type executionStream struct{ f *executionFixture }

func (*executionStream) Events() <-chan provider.StreamEvent { return nil }
func (s *executionStream) Close() error {
	s.f.trace = append(s.f.trace, "close")
	if s.f.close != nil {
		s.f.close()
	}
	return nil
}
func (f *executionFixture) next(ctx context.Context) (proxy.ForwardResult, error) {
	i := f.forwards
	f.forwards++
	if f.forward != nil {
		f.forward(ctx)
	}
	if i < len(f.results) {
		return f.results[i], f.errors[i]
	}
	return proxy.ForwardResult{Kind: proxy.ResultSuccess, FirstByteMs: 7, Chunks: 2}, nil
}
func (f *executionFixture) Forward(ctx context.Context, _ http.ResponseWriter, open proxy.StreamOpener, _ proxy.ForwardOptions, _ func(provider.StreamEvent)) (proxy.ForwardResult, error) {
	if f.beforeOpen != nil {
		f.beforeOpen(ctx)
	}
	stream, err := open(ctx)
	if err != nil {
		return proxy.ForwardResult{Kind: proxy.ResultClientCanceled}, err
	}
	defer stream.Close()
	return f.next(ctx)
}
func (f *executionFixture) ForwardNonStream(ctx context.Context, _ http.ResponseWriter, open proxy.ResponseOpener, _ proxy.ForwardOptions) (proxy.ForwardResult, error) {
	if f.beforeOpen != nil {
		f.beforeOpen(ctx)
	}
	_, _, err := open(ctx)
	if err != nil {
		return proxy.ForwardResult{Kind: proxy.ResultClientCanceled}, err
	}
	return f.next(ctx)
}
func (*executionFixture) Order(context.Context, string, RouteStrategy, []Candidate) ([]Candidate, error) {
	panic("executor must not order")
}
func (f *executionFixture) ObserveSuccess(ctx context.Context, _ int64, _ string, d time.Duration) {
	f.samples++
	f.durations = append(f.durations, d)
	if f.observe != nil {
		f.observe(ctx)
	}
}
func executionPlan(n int) *Plan {
	p := &Plan{Model: "public", Strategy: FallbackChain}
	for i := 1; i <= n; i++ {
		p.Candidates = append(p.Candidates, Candidate{Channel: store.Channel{ID: uint64(i), Name: "channel", Provider: "openai", BaseURL: "http://private.example", Weight: 1, Enabled: true}, UpstreamModel: "mapped"})
	}
	return p
}
func executionOptions() ExecutorOptions {
	return ExecutorOptions{Forward: proxy.ForwardOptions{TTFBTimeout: time.Second, OverallTimeout: 10 * time.Second}, StateTimeout: time.Second, PreparationTimeout: 2 * time.Second, MaxAttempts: 3}
}
func (f *executionFixture) executor(opts ExecutorOptions) Executor {
	e, err := NewExecutor(f, f, f, f, f, f, opts, slog.New(slog.NewJSONHandler(&f.logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
		if a.Key == slog.TimeKey {
			a.Key = "ts"
		}
		return a
	}})))
	require.NoError(f.t, err)
	return e
}
func (f *executionFixture) run(ctx context.Context, p *Plan, stream bool, opts ExecutorOptions) error {
	return f.executor(opts).Execute(ctx, httptest.NewRecorder(), &access.Principal{UserID: 1}, p, &provider.ChatRequest{Model: "public", Stream: stream})
}

func TestExecutorMatrixAndCausePreservation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, committed := range []bool{false, true} {
			for _, kind := range []proxy.ResultKind{proxy.ResultSuccess, proxy.ResultTTFBTimeout, proxy.ResultOverallTimeout, proxy.ResultStreamInterrupted, proxy.ResultUpstreamError, proxy.ResultClientCanceled, proxy.ResultWriteError} {
				statuses := []int{0}
				if kind == proxy.ResultUpstreamError {
					statuses = []int{0, 400, 404, 422, 401, 403, 429, 500, 503}
				}
				for _, status := range statuses {
					for _, retryable := range []bool{false, true} {
						t.Run(fmt.Sprintf("stream=%v/committed=%v/%s/%d/retry=%v", stream, committed, kind, status, retryable), func(t *testing.T) {
							cause := error(errors.Join(errors.New("secret cause"), context.Canceled))
							if status != 0 {
								cause = &provider.UpstreamError{StatusCode: status, Retryable: retryable, Cause: cause, Body: []byte("private-body")}
							}
							if kind == proxy.ResultSuccess {
								cause = nil
							}
							f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: kind, WroteHeader: committed, FirstByteMs: 11, Chunks: 3}}, errors: []error{cause}}
							err := f.run(context.Background(), executionPlan(2), stream, executionOptions())
							wantOutcome, wantReport, wantRetry := BreakerNeutral, false, false
							switch kind {
							case proxy.ResultSuccess:
								wantOutcome, wantReport = BreakerSuccess, true
							case proxy.ResultTTFBTimeout, proxy.ResultOverallTimeout, proxy.ResultStreamInterrupted:
								wantOutcome, wantReport, wantRetry = BreakerFailure, true, true
							case proxy.ResultUpstreamError:
								if status != 400 && status != 404 && status != 422 {
									wantOutcome, wantReport = BreakerFailure, true
									wantRetry = status != 0 && status != 401 && status != 403 && retryable
								}
							}
							wantRetry = wantRetry && !committed
							wantOpens := 1
							if wantRetry {
								wantOpens = 2
							}
							require.Equal(t, wantOpens, f.opens)
							require.Len(t, f.outcomes, wantOpens)
							require.Equal(t, wantOutcome, f.outcomes[0])
							wantReports := 0
							if wantReport {
								wantReports++
							}
							if wantRetry {
								wantReports++
							}
							require.Len(t, f.reports, wantReports)
							if kind == proxy.ResultSuccess || wantRetry {
								require.NoError(t, err)
								require.Equal(t, 1, f.samples)
							} else {
								var ex *access.ExecutionError
								require.ErrorAs(t, err, &ex)
								require.Same(t, cause, ex.Cause)
								require.Equal(t, committed, ex.WroteHeader)
								require.Equal(t, kind == proxy.ResultTTFBTimeout || kind == proxy.ResultOverallTimeout, ex.TimedOut)
								require.Zero(t, f.samples)
							}
							require.NotContains(t, f.logs.String(), "secret cause")
							require.NotContains(t, f.logs.String(), "private-body")
							require.NotContains(t, f.logs.String(), "private-key")
							require.NotContains(t, f.logs.String(), "private.example")
						})
					}
				}
			}
		}
	}
}

func TestExecutorPreparationSkipsAndInfrastructureStops(t *testing.T) {
	for _, mode := range []string{"unhealthy", "denied", "no-key", "missing-mapping", "changed-mapping", "sql", "cipher", "nil-endpoint", "channel", "provider", "url", "empty-key", "acquire-error"} {
		t.Run(mode, func(t *testing.T) {
			f := &executionFixture{t: t}
			sentinel := errors.New("private infrastructure")
			f.available = func(_ context.Context, id int64) bool { return mode != "unhealthy" || id != 1 }
			f.acquire = func(_ context.Context, id int64) (*Permit, error) {
				if id == 1 {
					if mode == "denied" {
						return nil, nil
					}
					if mode == "acquire-error" {
						return nil, sentinel
					}
				}
				return &Permit{channelID: id}, nil
			}
			f.pick = func(_ context.Context, c *store.Channel) (*provider.Endpoint, error) {
				ep := endpointFor(c)
				if c.ID != 1 {
					return ep, nil
				}
				switch mode {
				case "no-key":
					return nil, provider.ErrNoCandidate
				case "missing-mapping":
					return nil, store.ErrNotFound
				case "changed-mapping":
					ep.Model = "new"
				case "sql", "cipher":
					return nil, sentinel
				case "nil-endpoint":
					return nil, nil
				case "channel":
					ep.ChannelID = 99
				case "provider":
					ep.Provider = "wrong"
				case "url":
					ep.BaseURL = "http://wrong"
				case "empty-key":
					ep.APIKey = ""
				}
				return ep, nil
			}
			err := f.run(context.Background(), executionPlan(2), false, executionOptions())
			skip := mode == "unhealthy" || mode == "denied" || mode == "no-key" || mode == "missing-mapping" || mode == "changed-mapping"
			if skip {
				require.NoError(t, err)
				require.Equal(t, 1, f.opens)
				require.Equal(t, []bool{true}, f.reports)
			} else {
				require.Error(t, err)
				require.Zero(t, f.opens)
				require.Empty(t, f.reports)
				require.NotContains(t, f.trace, "health2")
				if mode == "sql" || mode == "cipher" || mode == "acquire-error" {
					require.ErrorIs(t, err, sentinel)
				}
			}
			if mode != "unhealthy" && mode != "denied" && mode != "acquire-error" {
				require.Equal(t, BreakerNeutral, f.outcomes[0])
			}
		})
	}
}

func TestExecutorExhaustionAttemptsAndLastFailure(t *testing.T) {
	for _, limit := range []int{1, 3, 16} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			f := &executionFixture{t: t}
			cause := errors.New("last failure")
			for range 20 {
				f.results = append(f.results, proxy.ForwardResult{Kind: proxy.ResultOverallTimeout})
				f.errors = append(f.errors, cause)
			}
			opts := executionOptions()
			opts.MaxAttempts = limit
			err := f.run(context.Background(), executionPlan(20), false, opts)
			want := limit
			if want == 0 {
				want = 3
			}
			require.Equal(t, want, f.opens)
			require.Len(t, f.outcomes, want)
			require.ErrorIs(t, err, cause)
			require.NotErrorIs(t, err, provider.ErrNoCandidate)
		})
	}
	for _, failureFirst := range []bool{false, true} {
		t.Run(fmt.Sprint(failureFirst), func(t *testing.T) {
			cause := errors.New("actual failure")
			f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: proxy.ResultTTFBTimeout}}, errors: []error{cause}}
			f.pick = func(_ context.Context, c *store.Channel) (*provider.Endpoint, error) {
				if failureFirst && c.ID == 1 {
					return endpointFor(c), nil
				}
				return nil, provider.ErrNoCandidate
			}
			err := f.run(context.Background(), executionPlan(4), false, executionOptions())
			if failureFirst {
				require.ErrorIs(t, err, cause)
				require.NotErrorIs(t, err, provider.ErrNoCandidate)
				require.Equal(t, 1, f.opens)
			} else {
				require.ErrorIs(t, err, provider.ErrNoCandidate)
				require.Zero(t, f.opens)
			}
			require.Len(t, f.outcomes, 4)
		})
	}
}

func TestExecutorSharedPreparationAndFeedbackBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &executionFixture{t: t}
		var deadline time.Time
		f.available = func(ctx context.Context, _ int64) bool {
			deadline, _ = ctx.Deadline()
			time.Sleep(700 * time.Millisecond)
			return true
		}
		f.acquire = func(ctx context.Context, id int64) (*Permit, error) {
			d, _ := ctx.Deadline()
			require.Equal(t, deadline, d)
			time.Sleep(700 * time.Millisecond)
			return &Permit{channelID: id}, nil
		}
		f.pick = func(ctx context.Context, _ *store.Channel) (*provider.Endpoint, error) {
			d, _ := ctx.Deadline()
			require.Equal(t, deadline, d)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		start := time.Now()
		err := f.run(context.Background(), executionPlan(2), false, executionOptions())
		require.Equal(t, 2*time.Second, time.Since(start))
		require.ErrorIs(t, err, context.DeadlineExceeded)
		var ex *access.ExecutionError
		require.False(t, errors.As(err, &ex))
		require.Zero(t, f.opens)
		require.Equal(t, []BreakerOutcome{BreakerNeutral}, f.outcomes)
		require.Empty(t, f.reports)
	})
	synctest.Test(t, func(t *testing.T) {
		f := &executionFixture{t: t}
		ctx, cancel := context.WithCancel(middleware.WithRequestID(context.Background(), "request-164"))
		defer cancel()
		f.complete = func(clean context.Context) {
			cancel()
			require.NoError(t, clean.Err())
			time.Sleep(700 * time.Millisecond)
		}
		f.report = func(clean context.Context) {
			require.Same(t, f.contexts[0], clean)
			id, _ := middleware.RequestIDFromContext(clean)
			require.Equal(t, "request-164", id)
			<-clean.Done()
		}
		start := time.Now()
		require.NoError(t, f.run(ctx, executionPlan(2), false, executionOptions()))
		require.Equal(t, time.Second, time.Since(start))
		require.Equal(t, 1, f.opens)
		require.Equal(t, []BreakerOutcome{BreakerSuccess}, f.outcomes)
		require.Equal(t, []bool{true}, f.reports)
	})
}

func TestExecutorLogicalBudgetAndEarlierParentCause(t *testing.T) {
	for _, earlier := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			f := &executionFixture{t: t}
			opts := executionOptions()
			opts.Forward.OverallTimeout = 3 * time.Second
			parent := context.Background()
			custom := errors.New("custom deadline")
			if earlier {
				var cancel context.CancelFunc
				parent, cancel = context.WithTimeoutCause(parent, time.Second, custom)
				defer cancel()
			}
			f.chat = func(ctx context.Context, _ *provider.Endpoint, _ *provider.ChatRequest) { <-ctx.Done() }
			f.results = []proxy.ForwardResult{{Kind: proxy.ResultOverallTimeout}}
			f.errors = []error{context.DeadlineExceeded}
			start := time.Now()
			err := f.run(parent, executionPlan(8), false, opts)
			want := 3 * time.Second
			if earlier {
				want = time.Second
			}
			require.Equal(t, want, time.Since(start))
			require.Equal(t, 1, f.opens)
			var ex *access.ExecutionError
			require.ErrorAs(t, err, &ex)
			require.True(t, ex.TimedOut)
			if earlier {
				require.ErrorIs(t, err, custom)
			}
			require.Equal(t, time.Duration(0), f.leases[0]%time.Millisecond)
			require.LessOrEqual(t, f.leases[0], want+opts.StateTimeout+500*time.Millisecond)
		})
	}
}

func TestExecutorCancellationBoundariesAndPanicRelease(t *testing.T) {
	for _, point := range []string{"health", "acquire", "pick", "before-open", "close", "complete", "report"} {
		t.Run(point, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			custom := errors.New("cancel cause")
			f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: proxy.ResultTTFBTimeout}}, errors: []error{context.DeadlineExceeded}}
			switch point {
			case "health":
				f.available = func(context.Context, int64) bool { cancel(custom); return true }
			case "acquire":
				f.acquire = func(_ context.Context, id int64) (*Permit, error) { cancel(custom); return &Permit{channelID: id}, nil }
			case "pick":
				f.pick = func(_ context.Context, c *store.Channel) (*provider.Endpoint, error) {
					cancel(custom)
					return endpointFor(c), nil
				}
			case "before-open":
				f.beforeOpen = func(context.Context) { cancel(custom) }
			case "close":
				f.close = func() { cancel(custom) }
			case "complete":
				f.complete = func(context.Context) { cancel(custom) }
			case "report":
				f.report = func(context.Context) { cancel(custom) }
			}
			err := f.run(ctx, executionPlan(2), true, executionOptions())
			require.ErrorIs(t, err, custom)
			require.NotContains(t, f.trace, "health2")
			if point == "close" || point == "complete" || point == "report" {
				require.Equal(t, 1, f.opens)
			} else {
				require.Zero(t, f.opens)
			}
			if point != "health" {
				require.Len(t, f.outcomes, 1)
			}
		})
	}
	for _, point := range []string{"pick", "provider", "forward", "observe"} {
		t.Run("panic/"+point, func(t *testing.T) {
			f := &executionFixture{t: t}
			boom := http.ErrAbortHandler
			switch point {
			case "pick":
				f.pick = func(context.Context, *store.Channel) (*provider.Endpoint, error) { panic(boom) }
			case "provider":
				f.chat = func(context.Context, *provider.Endpoint, *provider.ChatRequest) { panic(boom) }
			case "forward":
				f.forward = func(context.Context) { panic(boom) }
			case "observe":
				f.observe = func(context.Context) { panic(boom) }
			}
			require.PanicsWithValue(t, boom, func() { _ = f.run(context.Background(), executionPlan(2), true, executionOptions()) })
			require.Len(t, f.outcomes, 1)
			require.NotContains(t, f.trace, "health2")
			if point != "observe" {
				require.Equal(t, BreakerNeutral, f.outcomes[0])
				require.Empty(t, f.reports)
			}
		})
	}
}

func TestExecutorSerialCloseTimingReadonlyAndSafeLogs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: proxy.ResultTTFBTimeout, FirstByteMs: 11, Chunks: 3}}, errors: []error{context.DeadlineExceeded}}
		f.pick = func(_ context.Context, c *store.Channel) (*provider.Endpoint, error) {
			time.Sleep(200 * time.Millisecond)
			return endpointFor(c), nil
		}
		f.forward = func(context.Context) { time.Sleep(50 * time.Millisecond) }
		f.close = func() { time.Sleep(20 * time.Millisecond) }
		f.complete = func(context.Context) { time.Sleep(100 * time.Millisecond) }
		req := &provider.ChatRequest{Model: "public", Stream: true, Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"private-message"}`)}, Extra: map[string]json.RawMessage{"secret": json.RawMessage(`"private-extra"`)}}
		before, _ := json.Marshal(req)
		plan := executionPlan(2)
		beforePlan, _ := json.Marshal(plan)
		f.chat = func(_ context.Context, ep *provider.Endpoint, r *provider.ChatRequest) {
			require.Same(t, req, r)
			require.Equal(t, "public", r.Model)
			require.Equal(t, "mapped", ep.Model)
		}
		ctx := middleware.WithRequestID(context.Background(), "request-164")
		require.NoError(t, f.executor(executionOptions()).Execute(ctx, httptest.NewRecorder(), &access.Principal{}, plan, req))
		require.Equal(t, []string{"health1", "acquire1", "pick1", "open1", "close", "complete1", "report1", "health2", "acquire2", "pick2", "open2", "close", "complete2", "report2"}, f.trace)
		require.Equal(t, []time.Duration{70 * time.Millisecond}, f.durations)
		after, _ := json.Marshal(req)
		afterPlan, _ := json.Marshal(plan)
		require.Equal(t, before, after)
		require.Equal(t, beforePlan, afterPlan)
		require.Contains(t, f.logs.String(), `"first_byte_ms":11`)
		require.Contains(t, f.logs.String(), `"attempt_elapsed_ms":70`)
		require.Contains(t, f.logs.String(), `"feedback_elapsed_ms":100`)
		for _, s := range []string{"private-key", "private-message", "private-extra", "private.example", "private feedback secret", "request_done"} {
			require.NotContains(t, f.logs.String(), s)
		}
	})
}

func TestExecutorConstructorAndInvalidPlansNeverDispatch(t *testing.T) {
	f := &executionFixture{t: t}
	opts := executionOptions()
	for _, change := range []func(*ExecutorOptions){func(o *ExecutorOptions) { o.MaxAttempts = 0 }, func(o *ExecutorOptions) { o.MaxAttempts = -1 }, func(o *ExecutorOptions) { o.MaxAttempts = 17 }, func(o *ExecutorOptions) { o.StateTimeout = 0 }, func(o *ExecutorOptions) { o.PreparationTimeout = 0 }, func(o *ExecutorOptions) { o.Forward.TTFBTimeout = 0 }, func(o *ExecutorOptions) { o.Forward.OverallTimeout = 0 }, func(o *ExecutorOptions) { o.StateTimeout = time.Duration(math.MaxInt64) }, func(o *ExecutorOptions) { o.Forward.OverallTimeout = time.Duration(math.MaxInt64) }, func(o *ExecutorOptions) { o.StateTimeout = 24 * time.Hour }, func(o *ExecutorOptions) {
		o.Forward.OverallTimeout = 24*time.Hour - o.StateTimeout - 500*time.Millisecond + time.Nanosecond
	}} {
		o := opts
		change(&o)
		_, err := NewExecutor(f, f, f, f, f, f, o, testLogger())
		require.Error(t, err)
	}
	var typedNil *executionFixture
	for _, dep := range []*executionFixture{nil, typedNil} {
		_, err := NewExecutor(dep, f, f, f, f, f, opts, testLogger())
		require.Error(t, err)
		_, err = NewExecutor(f, dep, f, f, f, f, opts, testLogger())
		require.Error(t, err)
		_, err = NewExecutor(f, f, dep, f, f, f, opts, testLogger())
		require.Error(t, err)
		_, err = NewExecutor(f, f, f, dep, f, f, opts, testLogger())
		require.Error(t, err)
		_, err = NewExecutor(f, f, f, f, dep, f, opts, testLogger())
		require.Error(t, err)
		_, err = NewExecutor(f, f, f, f, f, dep, opts, testLogger())
		require.Error(t, err)
	}
	_, err := NewExecutor(f, f, f, f, f, f, opts, nil)
	require.Error(t, err)
	for _, mutate := range []func(*Plan){func(p *Plan) { p.Model = "wrong" }, func(p *Plan) { p.Strategy = Auto }, func(p *Plan) { p.Candidates = executionPlan(65).Candidates }, func(p *Plan) { p.Candidates = append(p.Candidates, p.Candidates[0]) }, func(p *Plan) { p.Candidates[0].Channel.ID = 0 }, func(p *Plan) { p.Candidates[0].Channel.Enabled = false }, func(p *Plan) { p.Candidates[0].UpstreamModel = "" }} {
		p := executionPlan(1)
		mutate(p)
		require.Error(t, f.run(context.Background(), p, false, opts))
	}
	e := f.executor(opts)
	ctx := context.Background()
	w := httptest.NewRecorder()
	p := &access.Principal{}
	plan := executionPlan(1)
	req := &provider.ChatRequest{Model: "public"}
	var missingContext context.Context // Deliberately exercise the public invalid-input guard.
	for _, err := range []error{e.Execute(missingContext, w, p, plan, req), e.Execute(ctx, nil, p, plan, req), e.Execute(ctx, w, nil, plan, req), e.Execute(ctx, w, p, nil, req), e.Execute(ctx, w, p, plan, nil)} {
		require.Error(t, err)
	}
	require.Empty(t, f.trace)
}

func TestExecutorConcurrentRequestIsolation(t *testing.T) {
	// One shared executor and shared dependencies dispatch into request-local
	// records by the propagated request ID. The map is immutable before launch.
	deps := executionConcurrent{records: make(map[string]*executionFixture)}
	for i := 0; i < 24; i++ {
		deps.records[fmt.Sprint("request", i)] = &executionFixture{t: t}
	}
	e, err := NewExecutor(deps, deps, deps, deps, deps, deps, executionOptions(), testLogger())
	require.NoError(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f := deps.records[fmt.Sprint("request", i)]
			ctx := middleware.WithRequestID(context.Background(), fmt.Sprint("request", i))
			plan := executionPlan(1)
			plan.Model = fmt.Sprint("public", i)
			plan.Candidates[0].Channel.ID = uint64(i + 1)
			req := &provider.ChatRequest{Model: plan.Model, Stream: i%2 == 0, Extra: map[string]json.RawMessage{"private": json.RawMessage(fmt.Sprintf(`"value%d"`, i))}}
			f.chat = func(_ context.Context, ep *provider.Endpoint, r *provider.ChatRequest) {
				require.Same(t, req, r)
				require.Equal(t, int64(i+1), ep.ChannelID)
				require.Equal(t, plan.Model, r.Model)
			}
			require.NoError(t, e.Execute(ctx, httptest.NewRecorder(), &access.Principal{UserID: int64(i + 1)}, plan, req))
			require.Equal(t, 1, f.opens)
			require.Equal(t, 1, f.samples)
		}(i)
	}
	wg.Wait()
}

type executionConcurrent struct{ records map[string]*executionFixture }

func (d executionConcurrent) get(ctx context.Context) *executionFixture {
	id, _ := middleware.RequestIDFromContext(ctx)
	return d.records[id]
}
func (d executionConcurrent) IsAvailable(ctx context.Context, id int64) bool {
	return d.get(ctx).IsAvailable(ctx, id)
}
func (d executionConcurrent) ReportRequestResult(ctx context.Context, id int64, ok bool) {
	d.get(ctx).ReportRequestResult(ctx, id, ok)
}
func (d executionConcurrent) Inspect(context.Context, int64) (BreakerView, error) { panic("inspect") }
func (d executionConcurrent) Acquire(ctx context.Context, id int64, lease time.Duration) (*Permit, error) {
	return d.get(ctx).Acquire(ctx, id, lease)
}
func (d executionConcurrent) Complete(ctx context.Context, p *Permit, o BreakerOutcome) error {
	return d.get(ctx).Complete(ctx, p, o)
}
func (d executionConcurrent) Pick(ctx context.Context, c *store.Channel, model string) (*provider.Endpoint, error) {
	return d.get(ctx).Pick(ctx, c, model)
}
func (d executionConcurrent) Name() string { return "openai" }
func (d executionConcurrent) Chat(ctx context.Context, ep *provider.Endpoint, r *provider.ChatRequest) (*provider.ChatResponse, *provider.Usage, error) {
	return d.get(ctx).Chat(ctx, ep, r)
}
func (d executionConcurrent) ChatStream(ctx context.Context, ep *provider.Endpoint, r *provider.ChatRequest) (provider.Stream, error) {
	return d.get(ctx).ChatStream(ctx, ep, r)
}
func (d executionConcurrent) Forward(ctx context.Context, w http.ResponseWriter, o proxy.StreamOpener, p proxy.ForwardOptions, on func(provider.StreamEvent)) (proxy.ForwardResult, error) {
	return d.get(ctx).Forward(ctx, w, o, p, on)
}
func (d executionConcurrent) ForwardNonStream(ctx context.Context, w http.ResponseWriter, o proxy.ResponseOpener, p proxy.ForwardOptions) (proxy.ForwardResult, error) {
	return d.get(ctx).ForwardNonStream(ctx, w, o, p)
}
func (executionConcurrent) Order(context.Context, string, RouteStrategy, []Candidate) ([]Candidate, error) {
	panic("order")
}
func (d executionConcurrent) ObserveSuccess(ctx context.Context, id int64, model string, elapsed time.Duration) {
	d.get(ctx).ObserveSuccess(ctx, id, model, elapsed)
}

func TestExecutorBudgetDecreasesAcrossSkipsAndAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: proxy.ResultTTFBTimeout}}, errors: []error{context.DeadlineExceeded}}
		f.pick = func(_ context.Context, c *store.Channel) (*provider.Endpoint, error) {
			time.Sleep(100 * time.Millisecond)
			if c.ID == 1 {
				return nil, provider.ErrNoCandidate
			}
			return endpointFor(c), nil
		}
		f.forward = func(context.Context) { time.Sleep(200 * time.Millisecond) }
		f.complete = func(context.Context) { time.Sleep(100 * time.Millisecond) }
		opts := executionOptions()
		opts.MaxAttempts = 2
		require.NoError(t, f.run(context.Background(), executionPlan(4), true, opts))
		require.Equal(t, 2, f.opens)
		require.Len(t, f.outcomes, 3)
		require.Len(t, f.leases, 3)
		require.Greater(t, f.leases[0], f.leases[1])
		require.Greater(t, f.leases[1], f.leases[2])
		require.NotContains(t, f.trace, "health4")
	})
}

func TestExecutorLatchedOverallSurvivesParentCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cause := errors.Join(context.Canceled, context.DeadlineExceeded)
	f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: proxy.ResultOverallTimeout}}, errors: []error{cause}}
	f.forward = func(context.Context) { cancel() }
	err := f.run(ctx, executionPlan(2), false, executionOptions())
	var ex *access.ExecutionError
	require.ErrorAs(t, err, &ex)
	require.True(t, ex.TimedOut)
	require.ErrorIs(t, err, cause)
	require.Equal(t, 1, f.opens)
	require.Equal(t, []BreakerOutcome{BreakerFailure}, f.outcomes)
	require.Equal(t, []bool{false}, f.reports)
}

type executionCommittedWriter struct{ *httptest.ResponseRecorder }

func (*executionCommittedWriter) Written() bool { return true }
func TestExecutorPreexistingCommitAndCanceledInputHaveNoEffects(t *testing.T) {
	f := &executionFixture{t: t}
	e := f.executor(executionOptions())
	req := &provider.ChatRequest{Model: "public"}
	err := e.Execute(context.Background(), &executionCommittedWriter{httptest.NewRecorder()}, &access.Principal{}, executionPlan(2), req)
	var ex *access.ExecutionError
	require.ErrorAs(t, err, &ex)
	require.True(t, ex.WroteHeader)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, e.Execute(ctx, httptest.NewRecorder(), &access.Principal{}, executionPlan(2), req), context.Canceled)
	require.Empty(t, f.trace)
}

type executionNoOpen struct{ *executionFixture }

func (executionNoOpen) Forward(context.Context, http.ResponseWriter, proxy.StreamOpener, proxy.ForwardOptions, func(provider.StreamEvent)) (proxy.ForwardResult, error) {
	return proxy.ForwardResult{Kind: proxy.ResultUpstreamError}, http.ErrNotSupported
}
func (executionNoOpen) ForwardNonStream(context.Context, http.ResponseWriter, proxy.ResponseOpener, proxy.ForwardOptions) (proxy.ForwardResult, error) {
	return proxy.ForwardResult{Kind: proxy.ResultUpstreamError}, http.ErrNotSupported
}
func TestExecutorForwarderPreflightNeutralAndInfrastructureAfterFailure(t *testing.T) {
	for _, stream := range []bool{false, true} {
		f := &executionFixture{t: t}
		e, err := NewExecutor(f, f, executionNoOpen{f}, f, f, f, executionOptions(), testLogger())
		require.NoError(t, err)
		err = e.Execute(context.Background(), httptest.NewRecorder(), &access.Principal{}, executionPlan(2), &provider.ChatRequest{Model: "public", Stream: stream})
		require.ErrorIs(t, err, http.ErrNotSupported)
		require.Zero(t, f.opens)
		require.Equal(t, []BreakerOutcome{BreakerNeutral}, f.outcomes)
		require.Empty(t, f.reports)
		require.Zero(t, f.samples)
		require.NotContains(t, f.trace, "health2")
	}
	f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: proxy.ResultTTFBTimeout}}, errors: []error{context.DeadlineExceeded}}
	infra := errors.New("private sql")
	f.pick = func(_ context.Context, c *store.Channel) (*provider.Endpoint, error) {
		if c.ID == 2 {
			return nil, infra
		}
		return endpointFor(c), nil
	}
	err := f.run(context.Background(), executionPlan(3), false, executionOptions())
	require.ErrorIs(t, err, infra)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, provider.ErrNoCandidate)
	require.Equal(t, 1, f.opens)
	require.Equal(t, []BreakerOutcome{BreakerFailure, BreakerNeutral}, f.outcomes)
	require.Equal(t, []bool{false}, f.reports)
}

func TestExecutorSummaryTracksOnlyActualOpener(t *testing.T) {
	for _, success := range []bool{false, true} {
		t.Run(fmt.Sprint(success), func(t *testing.T) {
			f := &executionFixture{t: t, results: []proxy.ForwardResult{{Kind: proxy.ResultTTFBTimeout}}, errors: []error{context.DeadlineExceeded}}
			f.pick = func(_ context.Context, c *store.Channel) (*provider.Endpoint, error) {
				if c.ID == 1 || success && c.ID == 3 {
					return endpointFor(c), nil
				}
				return nil, provider.ErrNoCandidate
			}
			logger := slog.New(slog.NewJSONHandler(&f.logs, &slog.HandlerOptions{ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
				if a.Key == slog.TimeKey {
					a.Key = "ts"
				}
				return a
			}}))
			e, err := NewExecutor(f, f, f, f, f, f, executionOptions(), logger)
			require.NoError(t, err)
			r := gin.New()
			r.Use(middleware.RequestID(), middleware.AccessLog(logger))
			r.POST("/", func(c *gin.Context) {
				ctx := middleware.WithModel(c.Request.Context(), "public")
				_ = e.Execute(ctx, c.Writer, &access.Principal{}, executionPlan(3), &provider.ChatRequest{Model: "public"})
			})
			r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
			var done map[string]any
			count := 0
			for _, line := range bytes.Split(bytes.TrimSpace(f.logs.Bytes()), []byte("\n")) {
				var row map[string]any
				require.NoError(t, json.Unmarshal(line, &row))
				require.Contains(t, row, "ts")
				require.NotEmpty(t, row["request_id"])
				if row["msg"] == "request_done" {
					done = row
					count++
				}
			}
			require.Equal(t, 1, count)
			want := float64(1)
			if success {
				want = 3
			}
			require.Equal(t, want, done["channel_id"])
		})
	}
}
