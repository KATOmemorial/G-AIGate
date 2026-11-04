package router

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Breaker issues attempt permits shared across gateway replicas.
type Breaker interface {
	BreakerInspector
	Acquire(context.Context, int64, time.Duration) (*Permit, error)
	Complete(context.Context, *Permit, BreakerOutcome) error
}

// BreakerOutcome classifies only the actual attempt result, including release.
type BreakerOutcome string

const (
	BreakerSuccess BreakerOutcome = "success"
	BreakerFailure BreakerOutcome = "failure"
	BreakerNeutral BreakerOutcome = "neutral"
)

// Permit has no exported state or mutation methods. Copies remain the same permit.
type Permit struct {
	channelID          int64
	epoch, token, kind string
	until              int64
}

// BreakerOptions requires explicit nonzero values; configuration owns defaults.
type BreakerOptions struct {
	Window, OpenDuration, StateTimeout time.Duration
	MinSamples                         int
	ErrorThreshold                     float64
}

//go:embed breaker.lua
var breakerLua string

type redisBreaker struct {
	client *redis.Client
	script *redis.Script
	opts   BreakerOptions
	policy string
	ppm    int64
}

// NewBreaker validates explicit options without contacting Redis. The injected
// client must honor context deadlines, as store.NewRedis does.
func NewBreaker(client *redis.Client, opts BreakerOptions, logger *slog.Logger) (Breaker, error) {
	ppm, ok := breakerThreshold(opts.ErrorThreshold)
	if client == nil || logger == nil || !ok || !breakerDuration(opts.Window, time.Hour) ||
		!breakerDuration(opts.OpenDuration, time.Hour) || !breakerDuration(opts.StateTimeout, time.Minute) ||
		opts.MinSamples < 1 || opts.MinSamples > 1000000 {
		return nil, failure("invalid breaker dependencies or options", nil)
	}
	if !client.Options().ContextTimeoutEnabled {
		return nil, failure("breaker client must honor context deadlines", nil)
	}
	policy := fmt.Sprintf("1:%d:%d:%d:%d:4096", opts.Window.Milliseconds(), opts.MinSamples, ppm, opts.OpenDuration.Milliseconds())
	return &redisBreaker{client: client, opts: opts, ppm: ppm, policy: policy, script: redis.NewScript(breakerLua)}, nil
}

func breakerDuration(d, max time.Duration) bool {
	return d >= time.Millisecond && d <= max && d%time.Millisecond == 0
}

// Accept a value only within two binary64 ULPs of the exact decimal PPM
// representation. This admits decimal parsing/multiplication noise, not a
// fixed decimal rounding tolerance. Bounds are checked before normalization.
func breakerThreshold(v float64) (int64, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0.000001 || v > 1 {
		return 0, false
	}
	p := math.Round(v * 1000000)
	canonical := p / 1000000
	ulp := math.Nextafter(canonical, math.Inf(1)) - canonical
	return int64(p), p >= 1 && p <= 1000000 && math.Abs(v-canonical) <= 2*ulp
}

func breakerID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", failure("breaker token generation failed", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func breakerKeys(id int64) []string {
	tag := "{" + strconv.FormatInt(id, 10) + "}"
	return []string{"gaigate:breaker:" + tag, "gaigate:breaker:events:" + tag, "gaigate:breaker:pending:" + tag}
}

func validBreakerID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func (b *redisBreaker) run(parent context.Context, id int64, op string, args ...any) ([]interface{}, error) {
	if parent == nil || id <= 0 {
		return nil, failure("invalid breaker input", nil)
	}
	ctx, cancel := context.WithTimeout(parent, b.opts.StateTimeout)
	defer cancel()
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	argv := []any{op, b.policy, b.opts.Window.Milliseconds(), b.opts.MinSamples, b.ppm, b.opts.OpenDuration.Milliseconds()}
	argv = append(argv, args...)
	// The inspect branch returns before the commit section and performs reads
	// only. Use the common Script cache fallback supported by miniredis too.
	cmd := b.script.Run(ctx, b.client, breakerKeys(id), argv...)
	if err := ctx.Err(); err != nil {
		return nil, failure("breaker state canceled", errors.Join(err, context.Cause(ctx), cmd.Err()))
	}
	result, err := cmd.Slice()
	if err != nil {
		return nil, failure("breaker state operation failed", err)
	}
	return result, nil
}

func (b *redisBreaker) Inspect(ctx context.Context, id int64) (BreakerView, error) {
	r, err := b.run(ctx, id, "inspect")
	if err != nil {
		return BreakerView{}, err
	}
	if len(r) != 2 {
		return BreakerView{}, failure("invalid breaker response", nil)
	}
	s, ok := r[0].(string)
	eligible, ok2 := r[1].(int64)
	if !ok || !ok2 || eligible < 0 || eligible > 1 || (BreakerState(s) != BreakerClosed && BreakerState(s) != BreakerOpen && BreakerState(s) != BreakerHalfOpen) {
		return BreakerView{}, failure("invalid breaker response", nil)
	}
	return BreakerView{State: BreakerState(s), Eligible: eligible == 1}, nil
}

func (b *redisBreaker) Acquire(ctx context.Context, id int64, lease time.Duration) (*Permit, error) {
	if id <= 0 || !breakerDuration(lease, 24*time.Hour) {
		return nil, failure("invalid breaker acquire input", nil)
	}
	token, err := breakerID()
	if err != nil {
		return nil, err
	}
	epoch, err := breakerID()
	if err != nil {
		return nil, err
	}
	r, err := b.run(ctx, id, "acquire", token, epoch, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	if len(r) == 0 {
		return nil, nil
	}
	if len(r) != 3 {
		return nil, failure("invalid breaker permit response", nil)
	}
	e, ok := r[0].(string)
	kind, ok2 := r[1].(string)
	until, ok3 := r[2].(string)
	n, parseErr := strconv.ParseInt(until, 10, 64)
	if !ok || !ok2 || !ok3 || parseErr != nil || !validBreakerID(e) || (kind != "c" && kind != "p") || n <= 0 || n > 9007199254740991 {
		return nil, failure("invalid breaker permit response", nil)
	}
	return &Permit{channelID: id, epoch: e, token: token, kind: kind, until: n}, nil
}

func (b *redisBreaker) Complete(ctx context.Context, p *Permit, outcome BreakerOutcome) error {
	if p == nil || p.channelID <= 0 || !validBreakerID(p.epoch) || !validBreakerID(p.token) || (p.kind != "c" && p.kind != "p") || p.until <= 0 || p.until > 9007199254740991 || (outcome != BreakerSuccess && outcome != BreakerFailure && outcome != BreakerNeutral) {
		return failure("invalid breaker completion input", nil)
	}
	epoch, err := breakerID()
	if err != nil {
		return err
	}
	_, err = b.run(ctx, p.channelID, "complete", p.token, epoch, 0, p.epoch, p.kind, p.until, string(outcome))
	return err
}
