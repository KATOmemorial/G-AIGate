package channel

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"time"

	"gaigate/internal/middleware"
	"gaigate/internal/store"
)

const healthConflictLimit = 16

var errHealthConflicts = errors.New("channel: health conflict limit exhausted")

// M1 supplies the state engine only. The lifecycle, public service constructor,
// scheduling and production assembly are delivered in M2, not stubbed here.
type healthPolicy struct {
	failureThreshold int
	disableDuration  time.Duration
	stateTimeout     time.Duration
}

type healthCore struct {
	state  store.VersionedHealthState
	policy healthPolicy
	logger *slog.Logger
	now    func() time.Time // instance-local; production uses UTC, tests control it
}

var _ HealthRegistry = (*healthCore)(nil)

func newHealthCore(state store.VersionedHealthState, policy healthPolicy, logger *slog.Logger, now func() time.Time) (*healthCore, error) {
	if policy.failureThreshold == 0 {
		policy.failureThreshold = 5
	}
	if policy.disableDuration == 0 {
		policy.disableDuration = 60 * time.Second
	}
	if policy.stateTimeout == 0 {
		policy.stateTimeout = 500 * time.Millisecond
	}
	if nilDependency(state) || logger == nil || policy.failureThreshold < 1 || policy.failureThreshold > math.MaxInt32 ||
		policy.disableDuration <= 0 || policy.stateTimeout < time.Millisecond {
		return nil, failure("invalid health state dependencies or policy", nil)
	}
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return &healthCore{state: state, policy: policy, logger: logger, now: now}, nil
}

func (h *healthCore) Read(parent context.Context, channelID int64) (store.HealthSnapshot, error) {
	ctx, cancel := context.WithTimeout(parent, h.policy.stateTimeout)
	defer cancel()
	record, err := h.load(ctx, channelID)
	if err != nil {
		return store.HealthSnapshot{}, err
	}
	return record.Snapshot, nil
}

func (h *healthCore) load(ctx context.Context, channelID int64) (store.HealthRecord, error) {
	if channelID <= 0 {
		return store.HealthRecord{}, failure("invalid health channel ID", nil)
	}
	if err := ctx.Err(); err != nil {
		return store.HealthRecord{}, failure("health state canceled", err)
	}
	record, err := h.state.LoadVersioned(ctx, channelID)
	if err != nil {
		return store.HealthRecord{}, failure("health state read failed", err)
	}
	return record, nil
}

func (h *healthCore) IsAvailable(ctx context.Context, channelID int64) bool {
	snapshot, err := h.Read(ctx, channelID)
	if err != nil {
		h.logFailure(ctx, channelID, "availability", err)
		return false
	}
	return snapshot.DisabledUntil == 0
}

func (h *healthCore) ReportRequestResult(ctx context.Context, channelID int64, ok bool) {
	if _, err := h.report(ctx, channelID, ok); err != nil {
		h.logFailure(ctx, channelID, "request_result", err)
	}
}

// report returns whether a CAS committed so M2 can enqueue a display refresh.
// The same context/deadline covers every read and CAS, with no retry goroutine.
func (h *healthCore) report(parent context.Context, channelID int64, ok bool) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, h.policy.stateTimeout)
	defer cancel()
	for attempt := 0; attempt < healthConflictLimit; attempt++ {
		record, err := h.load(ctx, channelID)
		if err != nil {
			return false, err
		}
		if record.Snapshot.DisabledUntil != 0 {
			return false, nil
		}
		next, err := h.transition(record.Snapshot, ok, false, h.now().UTC())
		if err != nil {
			return false, err
		}
		if err := ctx.Err(); err != nil {
			return false, failure("health state canceled", err)
		}
		committed, err := h.state.CompareAndSwap(ctx, channelID, record, next)
		if err != nil {
			return false, failure("health state commit failed", err)
		}
		if committed {
			return true, nil
		}
	}
	return false, errHealthConflicts
}

// probeFingerprint contains only the configuration that determines the actual
// probe path. Ciphertext is privately copied, never formatted or logged.
type probeFingerprint struct {
	enabled           bool
	provider, baseURL string
	mapping           store.ChannelModel
	keyID             uint64
	keyStatus         int8
	keyCiphertext     string
}

func newProbeFingerprint(ch store.Channel, mapping store.ChannelModel, key store.ChannelKey) probeFingerprint {
	return probeFingerprint{ch.Enabled, ch.Provider, ch.BaseURL, mapping, key.ID, key.Status, string(key.ProviderKeyEnc)}
}

// probeGeneration is captured before issuing a provider call. It owns a value
// copy; completion may only CAS this exact record once, never replay on reread.
type probeGeneration struct {
	record      store.HealthRecord
	fingerprint probeFingerprint
}

func (h *healthCore) beginProbe(parent context.Context, channelID int64, fingerprint probeFingerprint) (probeGeneration, bool, error) {
	ctx, cancel := context.WithTimeout(parent, h.policy.stateTimeout)
	defer cancel()
	record, err := h.load(ctx, channelID)
	if err != nil {
		return probeGeneration{}, false, err
	}
	if !fingerprint.enabled || (record.Snapshot.DisabledUntil != 0 && h.now().UTC().Unix() < record.Snapshot.DisabledUntil) {
		return probeGeneration{}, false, nil
	}
	return probeGeneration{record, fingerprint}, true, nil
}

// finishProbe consumes the ConnectivityTester contract: a nil error means an
// actual provider result, including its own timeout; preparation errors and
// parent cancellation are not signals. M2 owns calling the tester and passes
// its completion time here. reload MUST bypass the configuration cache.
func (h *healthCore) finishProbe(parent context.Context, channelID int64, generation probeGeneration,
	result ProbeResult, probeErr error, completedAt time.Time,
	reload func(context.Context) (probeFingerprint, error)) (bool, error) {
	if parent.Err() != nil {
		return false, nil
	}
	if probeErr != nil {
		h.logFailure(parent, channelID, "probe_preparation", probeErr)
		return false, failure("probe did not yield a health signal", probeErr)
	}
	ctx, cancel := context.WithTimeout(parent, h.policy.stateTimeout)
	defer cancel()
	current, err := reload(ctx)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, failure("probe configuration reload failed", err)
	}
	if current != generation.fingerprint || !current.enabled {
		return false, nil
	}
	next, err := h.transition(generation.record.Snapshot, result.OK, true, completedAt.UTC())
	if err != nil {
		return false, err
	}
	if err := ctx.Err(); err != nil {
		return false, failure("probe completion canceled", err)
	}
	committed, err := h.state.CompareAndSwap(ctx, channelID, generation.record, next)
	if err != nil {
		return false, failure("probe health commit failed", err)
	}
	return committed, nil
}

func (h *healthCore) transition(snapshot store.HealthSnapshot, ok, probe bool, now time.Time) (store.HealthSnapshot, error) {
	if probe {
		if now.Unix() < 0 || !time.Unix(now.Unix(), int64(now.Nanosecond())).Equal(now) {
			return store.HealthSnapshot{}, failure("probe time out of range", nil)
		}
		snapshot.LastProbeAt, snapshot.LastProbeOk = now.Unix(), ok
	}
	if ok {
		snapshot.ConsecutiveFailures = 0
		if probe {
			snapshot.DisabledUntil = 0
		}
		return snapshot, nil
	}
	threshold := int32(h.policy.failureThreshold)
	if snapshot.ConsecutiveFailures < threshold {
		snapshot.ConsecutiveFailures++
	} else {
		snapshot.ConsecutiveFailures = threshold
	}
	if snapshot.DisabledUntil != 0 {
		snapshot.ConsecutiveFailures = threshold
	}
	if snapshot.ConsecutiveFailures == threshold {
		until, err := cooldownUntil(now, h.policy.disableDuration)
		if err != nil {
			return store.HealthSnapshot{}, err
		}
		snapshot.DisabledUntil = until
	}
	return snapshot, nil
}

// Work in seconds/remainder rather than UnixNano or time.Add, which can wrap
// at extreme dates. ceil preserves even a positive one-nanosecond cooldown.
func cooldownUntil(now time.Time, duration time.Duration) (int64, error) {
	seconds := now.Unix()
	if duration <= 0 || seconds < 0 || !time.Unix(seconds, int64(now.Nanosecond())).Equal(now) {
		return 0, failure("health cooldown time out of range", nil)
	}
	delta := int64(duration / time.Second)
	remainder := int64(now.Nanosecond()) + int64(duration%time.Second)
	delta += remainder / int64(time.Second)
	if remainder%int64(time.Second) != 0 {
		delta++
	}
	if seconds > math.MaxInt64-delta {
		return 0, failure("health cooldown overflow", nil)
	}
	return seconds + delta, nil
}

func (h *healthCore) logFailure(ctx context.Context, channelID int64, operation string, err error) {
	reason := "state_error"
	switch {
	case errors.Is(err, context.Canceled):
		reason = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		reason = "deadline"
	case errors.Is(err, errHealthConflicts):
		reason = "conflicts_exhausted"
	}
	requestID, _ := middleware.RequestIDFromContext(ctx)
	h.logger.ErrorContext(ctx, "health_state_failed", "request_id", requestID,
		"channel_id", channelID, "operation", operation, "reason", reason)
}
