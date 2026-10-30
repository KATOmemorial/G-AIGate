package channel

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/store"

	"golang.org/x/sync/errgroup"
)

type HealthOptions struct {
	ProbeInterval, DisableDuration, ProbeTimeout, StateTimeout time.Duration
	FailureThreshold, ProbeConcurrency                         int
}

type HealthService interface {
	HealthRegistry
	Start(ctx context.Context) error
	Close(ctx context.Context) error
	Read(ctx context.Context, channelID int64) (store.HealthSnapshot, error)
}

const healthViewCapacity = 1024

type healthService struct {
	core     *healthCore
	channels store.ChannelRepo
	keys     store.KeyRepo
	tester   ConnectivityTester
	opts     HealthOptions

	mu         sync.Mutex
	started    bool
	closed     bool
	stopLogged bool
	cancel     context.CancelFunc
	done       chan struct{}
	ops        sync.WaitGroup // Add guarded by mu; Close prevents new operations
	operations map[*healthOperation]struct{}
	owner      errgroup.Group

	viewMu  sync.Mutex
	pending map[int64]struct{}
	views   chan int64 // never closed; the service context owns worker exit
	jobs    chan int64
}

type healthOperation struct{ cancel context.CancelFunc }

func NewHealthService(channels store.ChannelRepo, keys store.KeyRepo, state store.VersionedHealthState,
	tester ConnectivityTester, opts HealthOptions, logger *slog.Logger) (HealthService, error) {
	for _, dependency := range []any{channels, keys, state, tester, logger} {
		if nilDependency(dependency) {
			return nil, failure("missing health service dependency", nil)
		}
	}
	if opts.ProbeInterval <= 0 || opts.DisableDuration <= 0 || opts.ProbeTimeout < time.Millisecond ||
		opts.StateTimeout < time.Millisecond || opts.FailureThreshold < 1 || opts.FailureThreshold > math.MaxInt32 ||
		opts.ProbeConcurrency < 1 || opts.ProbeConcurrency > 64 {
		return nil, failure("invalid health options", nil)
	}
	core, err := newHealthCore(state, healthPolicy{opts.FailureThreshold, opts.DisableDuration, opts.StateTimeout}, logger, nil)
	if err != nil {
		return nil, err
	}
	if _, err := cooldownUntil(core.now(), opts.DisableDuration); err != nil {
		return nil, err
	}
	return &healthService{core: core, channels: channels, keys: keys, tester: tester, opts: opts,
		done: make(chan struct{}), pending: make(map[int64]struct{}), views: make(chan int64, healthViewCapacity),
		jobs: make(chan int64, opts.ProbeConcurrency), operations: make(map[*healthOperation]struct{})}, nil
}

func (h *healthService) Start(parent context.Context) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started || h.closed {
		return failure("health service already started or closed", nil)
	}
	if err := parent.Err(); err != nil {
		h.started = true
		close(h.done)
		return failure("health service start canceled", err)
	}
	h.started = true
	ctx, cancel := context.WithCancel(parent)
	h.cancel = cancel
	h.owner.Go(func() error {
		defer close(h.done)
		return h.run(ctx)
	})
	return nil
}

func (h *healthService) Close(ctx context.Context) error {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		for operation := range h.operations {
			operation.cancel()
		}
		if h.cancel != nil {
			h.cancel()
		}
		if !h.started {
			close(h.done)
		}
	}
	h.mu.Unlock()
	// All operations already have StateTimeout/ProbeTimeout and cancellation.
	// Joining remains mandatory even when the caller's close deadline expires;
	// there is no replacement timeout and no returned worker using closed stores.
	h.ops.Wait()
	<-h.done
	err := h.owner.Wait()
	h.mu.Lock()
	if !h.stopLogged {
		h.stopLogged = true
		h.core.logger.InfoContext(ctx, "health_service_stopped")
	}
	h.mu.Unlock()
	if err != nil {
		return failure("health owner failed", err)
	}
	return ctx.Err()
}

func (h *healthService) acquire(parent context.Context) (context.Context, func(), bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(parent)
	operation := &healthOperation{cancel: cancel}
	h.operations[operation] = struct{}{}
	h.ops.Add(1)
	return ctx, func() {
		cancel()
		h.mu.Lock()
		delete(h.operations, operation)
		h.mu.Unlock()
		h.ops.Done()
	}, true
}

func (h *healthService) Read(ctx context.Context, channelID int64) (store.HealthSnapshot, error) {
	operationCtx, release, ok := h.acquire(ctx)
	if !ok {
		return store.HealthSnapshot{}, failure("health service closed", nil)
	}
	defer release()
	return h.core.Read(operationCtx, channelID)
}

func (h *healthService) IsAvailable(ctx context.Context, channelID int64) bool {
	snapshot, err := h.Read(ctx, channelID)
	if err != nil {
		h.core.logFailure(ctx, channelID, "availability", err)
		return false
	}
	return snapshot.DisabledUntil == 0
}

func (h *healthService) ReportRequestResult(ctx context.Context, channelID int64, ok bool) {
	operationCtx, release, acquired := h.acquire(ctx)
	if !acquired {
		h.core.logFailure(ctx, channelID, "request_result", failure("health service closed", nil))
		return
	}
	defer release()
	committed, err := h.core.report(operationCtx, channelID, ok)
	if err != nil {
		h.core.logFailure(ctx, channelID, "request_result", err)
	} else if committed {
		h.enqueueView(ctx, channelID)
	}
}

func (h *healthService) enqueueView(ctx context.Context, channelID int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.viewMu.Lock()
	defer h.viewMu.Unlock()
	if _, ok := h.pending[channelID]; ok {
		return
	}
	if len(h.pending) == healthViewCapacity {
		h.core.logFailure(ctx, channelID, "display_queue_full", errors.New("health display queue full"))
		return
	}
	h.pending[channelID] = struct{}{}
	h.views <- channelID // map/channel are updated under the same lock
}

func (h *healthService) run(ctx context.Context) error {
	group, workerCtx := errgroup.WithContext(ctx)
	jobs := h.jobs
	completed := make(chan struct{}, h.opts.ProbeConcurrency)
	for i := 0; i < h.opts.ProbeConcurrency; i++ {
		group.Go(func() error {
			for {
				select {
				case <-workerCtx.Done():
					return nil
				case id := <-jobs:
					if workerCtx.Err() != nil {
						return nil
					}
					h.probeChannel(workerCtx, id)
					select {
					case completed <- struct{}{}:
					case <-workerCtx.Done():
						return nil
					}
				}
			}
		})
	}
	group.Go(func() error { h.displayWorker(workerCtx); return nil })
	group.Go(func() error {
		for workerCtx.Err() == nil {
			h.round(workerCtx, jobs, completed)
			if workerCtx.Err() == nil {
				h.core.logger.InfoContext(workerCtx, "health_probe_round_finished")
			}
			// A fresh timer after each completed round cannot accumulate ticks.
			timer := time.NewTimer(h.opts.ProbeInterval)
			select {
			case <-workerCtx.Done():
				timer.Stop()
				return nil
			case <-timer.C:
			}
		}
		return nil
	})
	return group.Wait()
}

func (h *healthService) round(ctx context.Context, jobs chan<- int64, completed <-chan struct{}) {
	queryCtx, cancel := context.WithTimeout(ctx, h.opts.ProbeTimeout)
	channels, err := h.channels.List(queryCtx)
	cancel()
	if err != nil {
		h.core.logFailure(ctx, 0, "probe_enumeration", err)
		return
	}
	index, outstanding := 0, 0
	for index < len(channels) || outstanding > 0 {
		for index < len(channels) && (!channels[index].Enabled || !validID(channels[index].ID)) {
			index++
		}
		var send chan<- int64
		var id int64
		if index < len(channels) {
			send, id = jobs, int64(channels[index].ID)
		}
		if send == nil && outstanding == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case send <- id:
			index++
			outstanding++
		case <-completed:
			outstanding--
		}
	}
}

func (h *healthService) probeChannel(owner context.Context, channelID int64) {
	ctx, cancel := context.WithTimeout(owner, h.opts.ProbeTimeout)
	defer cancel()
	config, err := h.loadProbeConfiguration(ctx, channelID)
	if err != nil {
		h.core.logFailure(owner, channelID, "probe_configuration", err)
		return
	}
	if !config.channel.Enabled {
		return
	}
	generation, eligible, err := h.core.beginProbe(ctx, channelID, config.fingerprint())
	if err != nil {
		h.core.logFailure(owner, channelID, "probe_state", err)
		return
	}
	if !eligible {
		return
	}
	pinned := context.WithValue(ctx, healthProbeContextKey{}, &healthProbeCall{owner: owner, config: config})
	result, probeErr := h.tester.Test(pinned, &config.channel, config.mapping.Model)
	completedAt := h.core.now().UTC()
	// Completion is a separate StateTimeout operation, even for an issued probe
	// whose own network budget expired. The task owner still cancels it on Close.
	committed, err := h.core.finishProbe(owner, channelID, generation, result, probeErr, completedAt,
		func(reloadCtx context.Context) (probeFingerprint, error) {
			latest, err := h.loadProbeConfiguration(reloadCtx, channelID)
			return latest.fingerprint(), err
		})
	if err != nil {
		h.core.logFailure(owner, channelID, "probe_result", err)
	} else if committed {
		h.enqueueView(owner, channelID)
	}
}

type healthProbeContextKey struct{}
type healthProbeCall struct {
	owner  context.Context
	config probeConfiguration
}
type probeConfiguration struct {
	channel store.Channel
	mapping store.ChannelModel
	key     store.ChannelKey
}

func (c probeConfiguration) fingerprint() probeFingerprint {
	return newProbeFingerprint(c.channel, c.mapping, c.key)
}

func (h *healthService) loadProbeConfiguration(ctx context.Context, id int64) (probeConfiguration, error) {
	var config probeConfiguration
	ch, err := h.channels.Get(ctx, id)
	if err != nil {
		return config, err
	}
	if ch == nil || !validID(ch.ID) || int64(ch.ID) != id {
		return config, failure("invalid probe channel", nil)
	}
	config.channel = *ch
	if !ch.Enabled {
		return config, nil
	}
	mappings, err := h.channels.ListModels(ctx, id)
	if err != nil {
		return config, err
	}
	if len(mappings) == 0 {
		return config, failure("missing probe mapping", nil)
	}
	config.mapping = mappings[0]
	for _, mapping := range mappings[1:] {
		if mapping.Model < config.mapping.Model {
			config.mapping = mapping
		}
	}
	if err := validateEndpointChannel(ch, config.mapping.Model); err != nil {
		return config, err
	}
	if config.mapping.ChannelID != ch.ID || !validName(config.mapping.UpstreamModel) {
		return config, failure("invalid probe mapping", nil)
	}
	keys, err := h.keys.ListByChannel(ctx, id)
	if err != nil {
		return config, err
	}
	var selected *store.ChannelKey
	for i := range keys {
		if keys[i].Status == store.ChannelKeyStatusActive && (selected == nil || keys[i].ID < selected.ID) {
			selected = &keys[i]
		}
	}
	if selected == nil {
		return config, failure("missing active probe Key", provider.ErrNoCandidate)
	}
	if !validID(selected.ID) || selected.ChannelID != ch.ID {
		return config, failure("invalid probe Key", nil)
	}
	config.key = *selected
	config.key.ProviderKeyEnc = append([]byte(nil), selected.ProviderKeyEnc...)
	return config, nil
}

func (h *healthService) displayWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-h.views:
			h.viewMu.Lock()
			delete(h.pending, id)
			h.viewMu.Unlock()
			operationCtx, cancel := context.WithTimeout(ctx, h.opts.StateTimeout)
			record, err := h.core.load(operationCtx, id)
			if err == nil {
				status, checked := healthView(record.Snapshot, h.core.now().UTC())
				err = h.channels.UpdateHealthView(operationCtx, id, status, checked)
			}
			cancel()
			if err != nil {
				h.core.logFailure(ctx, id, "health_display", err)
			}
		}
	}
}

func healthView(snapshot store.HealthSnapshot, now time.Time) (string, *time.Time) {
	status := "unknown"
	var checked *time.Time
	if snapshot.LastProbeAt != 0 {
		at := time.Unix(snapshot.LastProbeAt, 0).UTC()
		checked = &at
		status = "healthy"
	}
	if snapshot.DisabledUntil != 0 {
		status = "disabled"
		if now.Unix() >= snapshot.DisabledUntil {
			status = "cooling"
		}
	}
	return status, checked
}
