package router

import (
	"context"
	"sort"
	"sync"
	"time"

	"gaigate/internal/middleware"
)

const (
	latencyWindow   = time.Minute
	latencyCapacity = 4096
	latencyLimit    = 256
	latencyMinimum  = 5
)

type latencyKey struct {
	channelID int64
	model     string
}

type latencySample struct {
	at      time.Time
	elapsed time.Duration
}

type latencyEntry struct {
	samples  [latencyLimit]latencySample
	count    int
	observed time.Time
}

type latencySamples struct {
	mu      sync.Mutex
	entries map[latencyKey]*latencyEntry
}

// prune preserves arrival order, including across wall-clock adjustments.
func (e *latencyEntry) prune(now time.Time) {
	cutoff := now.Add(-latencyWindow)
	n := 0
	for i := 0; i < e.count; i++ {
		if e.samples[i].at.After(cutoff) {
			e.samples[n] = e.samples[i]
			n++
		}
	}
	clear(e.samples[n:e.count])
	e.count = n
}

func (s *strategyEngine) ObserveSuccess(ctx context.Context, channelID int64, model string, elapsed time.Duration) {
	if ctx == nil || channelID <= 0 || !validName(model) || elapsed < 0 {
		requestID := ""
		if ctx != nil {
			requestID, _ = middleware.RequestIDFromContext(ctx)
		}
		// Never log the rejected input, candidate or cause; logging is outside the lock.
		s.logger.Warn("router observation discarded", "stage", "invalid success observation", "request_id", requestID)
		return
	}
	key := latencyKey{channelID: channelID, model: model}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Capture under the table lock so concurrent writes have a consistent observation order.
	now := s.now()
	e := s.entries[key]
	if e == nil {
		if len(s.entries) >= latencyCapacity {
			var oldest latencyKey
			var entry *latencyEntry
			for k, v := range s.entries {
				if entry == nil || v.observed.Before(entry.observed) || v.observed.Equal(entry.observed) && latencyKeyLess(k, oldest) {
					oldest, entry = k, v
				}
			}
			delete(s.entries, oldest)
		}
		e = &latencyEntry{}
		s.entries[key] = e
	}
	e.prune(now)
	if e.count == latencyLimit {
		copy(e.samples[:], e.samples[1:])
		e.count--
	}
	e.samples[e.count] = latencySample{at: now, elapsed: elapsed}
	e.count++
	e.observed = now
}

func latencyKeyLess(a, b latencyKey) bool {
	return a.channelID < b.channelID || a.channelID == b.channelID && a.model < b.model
}

func (s *strategyEngine) latencies(model string, candidates []Candidate, now time.Time) map[uint64]strategyRank {
	// Copy only bounded durations while locked. Percentile sorting stays outside.
	values := make(map[uint64][]time.Duration, len(candidates))
	s.mu.Lock()
	for _, c := range candidates {
		e := s.entries[latencyKey{channelID: int64(c.Channel.ID), model: model}]
		if e == nil {
			continue
		}
		e.prune(now)
		v := make([]time.Duration, 0, e.count)
		for i := 0; i < e.count; i++ {
			if !e.samples[i].at.After(now) {
				v = append(v, e.samples[i].elapsed)
			}
		}
		values[c.Channel.ID] = v
	}
	s.mu.Unlock()
	ranks := make(map[uint64]strategyRank, len(values))
	for id, v := range values {
		if len(v) >= latencyMinimum {
			sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
			ranks[id] = strategyRank{known: true, value: uint64(v[(99*len(v)+99)/100-1])}
		}
	}
	return ranks
}
