package channel

import (
	"bytes"
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"gaigate/internal/provider"
	"gaigate/internal/store"
)

const pickerIdleTTL = 10 * time.Minute

type endpointPicker struct {
	channels  store.ChannelRepo
	keys      store.KeyRepo
	masterKey []byte

	// This lock only protects gate lifetime, references and the private clock.
	// Neither repository calls nor gate waits run under it.
	mu     sync.Mutex
	states map[uint64]*pickerState
	now    func() time.Time
}

type pickerState struct {
	gate     chan struct{}
	refs     int // includes operations waiting for the gate
	lastUsed time.Time

	// The numerical lock never covers SQL, decryption or gate waits.
	mu       sync.Mutex
	snapshot []store.ChannelKey
	current  []int64
}

// NewEndpointPicker constructs an instance-local, configuration-only SWRR picker.
// ProbePicker remains independent and never consumes this picker's positions.
func NewEndpointPicker(channels store.ChannelRepo, keys store.KeyRepo, masterKey []byte) (EndpointPicker, error) {
	if nilDependency(channels) || nilDependency(keys) || len(masterKey) != 32 {
		return nil, failure("invalid endpoint picker dependencies", nil)
	}
	return &endpointPicker{channels: channels, keys: keys,
		masterKey: append([]byte(nil), masterKey...), states: make(map[uint64]*pickerState), now: time.Now}, nil
}

func (p *endpointPicker) retain(id uint64) *pickerState {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	for channelID, state := range p.states {
		if state.refs == 0 && now.Sub(state.lastUsed) >= pickerIdleTTL {
			delete(p.states, channelID)
		}
	}
	state := p.states[id]
	if state == nil {
		state = &pickerState{gate: make(chan struct{}, 1)}
		p.states[id] = state
	}
	state.refs++
	return state
}

func (p *endpointPicker) release(state *pickerState) {
	p.mu.Lock()
	defer p.mu.Unlock()
	state.refs--
	state.lastUsed = p.now()
}

func (p *endpointPicker) Pick(ctx context.Context, ch *store.Channel, model string) (*provider.Endpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, failure("endpoint selection canceled", err)
	}
	if err := validateEndpointChannel(ch, model); err != nil {
		return nil, err
	}
	if !ch.Enabled {
		return nil, failure("no enabled channel", provider.ErrNoCandidate)
	}
	state := p.retain(ch.ID)
	defer p.release(state)
	select {
	case <-ctx.Done():
		return nil, failure("endpoint selection canceled", ctx.Err())
	case state.gate <- struct{}{}:
	}
	defer func() { <-state.gate }()
	if err := ctx.Err(); err != nil {
		return nil, failure("endpoint selection canceled", err)
	}
	// Read only after taking the channel gate: an older SQL result must never
	// reinstall an obsolete snapshot after a newer selection has committed.
	rows, err := p.keys.ListByChannel(ctx, int64(ch.ID))
	if err != nil {
		return nil, failure("endpoint Key lookup failed", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, failure("endpoint selection canceled", err)
	}
	snapshot, total, err := endpointSnapshot(rows, ch.ID)
	if err != nil {
		return nil, err
	}
	if len(snapshot) == 0 {
		state.mu.Lock()
		state.snapshot, state.current = nil, nil
		state.mu.Unlock()
		return nil, failure("no active endpoint Key", provider.ErrNoCandidate)
	}
	mapping, err := p.channels.GetModel(ctx, int64(ch.ID), model)
	if err != nil {
		return nil, failure("endpoint mapping lookup failed", err)
	}
	if mapping == nil || mapping.Model != model {
		return nil, failure("invalid endpoint mapping", nil)
	}
	selected, err := state.choose(ctx, snapshot, total)
	if err != nil {
		return nil, err
	}
	// A committed position is consumed even if assembly/decryption fails.
	// Only the selected ciphertext is decrypted; there is no retry or rollback.
	return assembleEndpoint(ctx, ch, mapping, &selected, p.masterKey)
}

func endpointSnapshot(rows []store.ChannelKey, channelID uint64) ([]store.ChannelKey, int64, error) {
	active := make([]store.ChannelKey, 0, len(rows))
	var total int64
	for _, row := range rows {
		if row.Status != store.ChannelKeyStatusActive {
			continue
		}
		if !validID(row.ID) || row.ChannelID != channelID || row.Weight < 1 || row.Weight > 10000 {
			return nil, 0, failure("invalid active endpoint Key", nil)
		}
		var ok bool
		total, ok = checkedWeightAdd(total, int64(row.Weight))
		if !ok {
			return nil, 0, failure("endpoint weight total overflow", nil)
		}
		row.ProviderKeyEnc = append([]byte(nil), row.ProviderKeyEnc...)
		active = append(active, row)
	}
	sort.Slice(active, func(i, j int) bool { return active[i].ID < active[j].ID })
	for i := 1; i < len(active); i++ {
		if active[i-1].ID == active[i].ID {
			return nil, 0, failure("duplicate active endpoint Key", nil)
		}
	}
	return active, total, nil
}

func sameEndpointSnapshot(a, b []store.ChannelKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].Weight != b[i].Weight || a[i].Status != b[i].Status ||
			!bytes.Equal(a[i].ProviderKeyEnc, b[i].ProviderKeyEnc) {
			return false
		}
	}
	return true
}

func checkedWeightAdd(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func (s *pickerState) choose(ctx context.Context, snapshot []store.ChannelKey, total int64) (store.ChannelKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return store.ChannelKey{}, failure("endpoint selection canceled", err)
	}
	current := s.current
	if !sameEndpointSnapshot(s.snapshot, snapshot) {
		current = make([]int64, len(snapshot))
	}
	// Prepare the complete update before committing anything, including a reset.
	next := make([]int64, len(snapshot))
	selected := 0
	for i, key := range snapshot {
		var ok bool
		next[i], ok = checkedWeightAdd(current[i], int64(key.Weight))
		if !ok {
			return store.ChannelKey{}, failure("endpoint current weight overflow", nil)
		}
		if next[i] > next[selected] {
			selected = i
		}
	}
	weight, ok := checkedWeightAdd(next[selected], -total)
	if !ok {
		return store.ChannelKey{}, failure("endpoint selected weight overflow", nil)
	}
	next[selected] = weight
	if err := ctx.Err(); err != nil {
		return store.ChannelKey{}, failure("endpoint selection canceled", err)
	}
	s.snapshot, s.current = snapshot, next
	return snapshot[selected], nil
}
