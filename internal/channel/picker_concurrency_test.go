package channel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"gaigate/internal/provider"
	"gaigate/internal/store"
)

// Done observation is a deterministic entry barrier for the gate select. At
// this point retain has already counted the operation, including a waiter.
type endpointGateContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *endpointGateContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}

func endpointAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("endpoint barrier timed out")
		var zero T
		return zero
	}
}

type endpointPickResult struct {
	label string
	err   error
}

func endpointAsyncPick(p EndpointPicker, ctx context.Context, ch *store.Channel) <-chan endpointPickResult {
	result := make(chan endpointPickResult, 1)
	go func() {
		ep, err := p.Pick(ctx, ch, "a")
		result <- endpointPickResult{label: endpointLabel(ep), err: err}
	}()
	return result
}

func TestEndpointPickerConcurrentPeriodCountsAndInstances(t *testing.T) {
	master := make([]byte, 32)
	rows := endpointRows(t, 1, master, 5, 1, 1)
	keys := &keyFake{list: func(context.Context, int64) ([]store.ChannelKey, error) { return rows, nil }}
	channels := &mappingRepoFake{get: func(_ context.Context, id int64, m string) (*store.ChannelModel, error) {
		return &store.ChannelModel{ChannelID: uint64(id), Model: m, UpstreamModel: "mapped"}, nil
	}}
	p, err := NewEndpointPicker(channels, keys, master)
	require.NoError(t, err)
	ch := &store.Channel{ID: 1, Enabled: true, Provider: "openai", BaseURL: "http://localhost"}
	var counts sync.Map
	for _, label := range []string{"A", "B", "C"} {
		counts.Store(label, &atomic.Int64{})
	}
	var group errgroup.Group
	for i := 0; i < 28; i++ {
		group.Go(func() error {
			for j := 0; j < 25; j++ {
				ep, err := p.Pick(context.Background(), ch, "a")
				if err != nil {
					return err
				}
				counter, ok := counts.Load(endpointLabel(ep))
				if !ok {
					t.Error("unexpected endpoint credential")
					return nil
				}
				counter.(*atomic.Int64).Add(1)
			}
			return nil
		})
	}
	require.NoError(t, group.Wait())
	for label, expected := range map[string]int64{"A": 500, "B": 100, "C": 100} {
		counter, _ := counts.Load(label)
		assert.Equal(t, expected, counter.(*atomic.Int64).Load())
	}
	// Two instances share repositories but have independent numerical state.
	for i := 0; i < 2; i++ {
		other, err := NewEndpointPicker(channels, keys, master)
		require.NoError(t, err)
		assert.Equal(t, "A", pickEndpointLabel(t, other, ch))
		assert.Equal(t, "A", pickEndpointLabel(t, other, ch))
		assert.Equal(t, "B", pickEndpointLabel(t, other, ch))
	}
}

func TestEndpointPickerGateCancellationAndCrossChannelIsolation(t *testing.T) {
	f := newEndpointFixture(t, 1, 1)
	rows := f.rows
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	var reads atomic.Int64
	f.keys.list = func(ctx context.Context, id int64) ([]store.ChannelKey, error) {
		if id == 2 {
			copyRows := append([]store.ChannelKey(nil), rows...)
			for i := range copyRows {
				copyRows[i].ChannelID = 2
			}
			return copyRows, nil
		}
		if reads.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return rows, nil
	}
	f.channels.get = func(_ context.Context, id int64, m string) (*store.ChannelModel, error) {
		return &store.ChannelModel{ChannelID: uint64(id), Model: m, UpstreamModel: "mapped"}, nil
	}
	first := endpointAsyncPick(f.p, context.Background(), f.ch)
	endpointAwait(t, entered)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitCtx := &endpointGateContext{Context: parent, entered: make(chan struct{})}
	waiting := endpointAsyncPick(f.p, waitCtx, f.ch)
	endpointAwait(t, waitCtx.entered)
	assert.Equal(t, int64(1), reads.Load(), "waiter must not query outside the gate")
	f.p.mu.Lock()
	state := f.p.states[1]
	assert.Equal(t, 2, state.refs)
	f.p.mu.Unlock()
	// No numerical or registry lock may be held across the blocked SQL call.
	require.True(t, state.mu.TryLock())
	state.mu.Unlock()
	otherChannel := *f.ch
	otherChannel.ID = 2
	other := endpointAwait(t, endpointAsyncPick(f.p, context.Background(), &otherChannel))
	require.NoError(t, other.err)
	assert.Equal(t, "A", other.label)
	cancel()
	got := endpointAwait(t, waiting)
	assert.ErrorIs(t, got.err, context.Canceled)
	assert.Equal(t, int64(1), reads.Load(), "canceled gate waiter must not query SQL")
	f.p.mu.Lock()
	assert.Equal(t, 1, state.refs)
	f.p.mu.Unlock()
	unblock.Do(func() { close(release) })
	got = endpointAwait(t, first)
	require.NoError(t, got.err)
	assert.Equal(t, "A", got.label)
	assert.Equal(t, "B", pickEndpointLabel(t, f.p, f.ch), "wait cancellation did not consume a slot")
	f.p.mu.Lock()
	assert.Zero(t, state.refs)
	f.p.mu.Unlock()
}

func TestEndpointPickerOldSnapshotCannotReinstall(t *testing.T) {
	f := newEndpointFixture(t, 1, 1)
	oldRows := f.rows
	newRows := append([]store.ChannelKey(nil), oldRows...)
	newRows[0].Weight = 3
	var committed atomic.Bool
	var queries atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	f.keys.list = func(ctx context.Context, _ int64) ([]store.ChannelKey, error) {
		query := queries.Add(1)
		if query == 1 {
			// The first operation already obtained the old SQL snapshot.
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return oldRows, nil
		}
		if committed.Load() {
			return newRows, nil
		}
		return oldRows, nil
	}
	first := endpointAsyncPick(f.p, context.Background(), f.ch)
	endpointAwait(t, entered)
	ctx := &endpointGateContext{Context: context.Background(), entered: make(chan struct{})}
	second := endpointAsyncPick(f.p, ctx, f.ch)
	endpointAwait(t, ctx.entered)
	assert.Equal(t, int64(1), queries.Load(), "second SQL read has not started")
	committed.Store(true)
	unblock.Do(func() { close(release) })
	a, b := endpointAwait(t, first), endpointAwait(t, second)
	require.NoError(t, a.err)
	require.NoError(t, b.err)
	assert.Equal(t, "A", a.label, "old in-flight snapshot may finish")
	assert.Equal(t, "A", b.label, "next operation installs the committed 3:1 snapshot")
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	assert.Equal(t, "B", pickEndpointLabel(t, f.p, f.ch))
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	assert.Equal(t, int64(5), queries.Load())
	assert.Equal(t, int32(3), f.p.states[1].snapshot[0].Weight)
}

func TestEndpointPickerIdleReclamationProtectsOwnersAndWaiters(t *testing.T) {
	for _, cancelWaiter := range []bool{false, true} {
		name := "waiting completes"
		if cancelWaiter {
			name = "waiting canceled"
		}
		t.Run(name, func(t *testing.T) {
			f := newEndpointFixture(t, 1, 1)
			var clock atomic.Int64
			f.p.now = func() time.Time { return time.Unix(0, clock.Load()) }
			assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
			initial := f.p.states[1]
			rows := f.rows
			entered, release := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			t.Cleanup(func() { unblock.Do(func() { close(release) }) })
			var calls atomic.Int64
			f.keys.list = func(ctx context.Context, _ int64) ([]store.ChannelKey, error) {
				if calls.Add(1) == 1 {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return rows, nil
			}
			first := endpointAsyncPick(f.p, context.Background(), f.ch)
			endpointAwait(t, entered)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			ctx := &endpointGateContext{Context: parent, entered: make(chan struct{})}
			second := endpointAsyncPick(f.p, ctx, f.ch)
			endpointAwait(t, ctx.entered)
			clock.Store(int64(2 * pickerIdleTTL))
			// Trigger private opportunity cleanup without a second SQL operation.
			other := f.p.retain(2)
			f.p.release(other)
			f.p.mu.Lock()
			assert.Same(t, initial, f.p.states[1], "active/waiting gates cannot be reclaimed")
			assert.Equal(t, 2, initial.refs)
			f.p.mu.Unlock()
			if cancelWaiter {
				cancel()
				assert.ErrorIs(t, endpointAwait(t, second).err, context.Canceled)
			}
			// A new reference while the old owner is alive must use that same gate.
			same := f.p.retain(1)
			assert.Same(t, initial, same)
			f.p.release(same)
			unblock.Do(func() { close(release) })
			got := endpointAwait(t, first)
			require.NoError(t, got.err)
			assert.Equal(t, "B", got.label, "cleanup must not reset an in-flight sequence")
			if !cancelWaiter {
				got = endpointAwait(t, second)
				require.NoError(t, got.err)
				assert.Equal(t, "A", got.label)
			}
			clock.Add(int64(pickerIdleTTL))
			replacement := f.p.retain(1)
			assert.NotSame(t, initial, replacement, "fully idle gate can be reclaimed at ten minutes")
			f.p.release(replacement)
			assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch), "a reclaimed sequence restarts from zero")
		})
	}
}

func TestEndpointPickerEmptySetReclaimAndProbeIndependence(t *testing.T) {
	f := newEndpointFixture(t, 1, 1)
	var now atomic.Int64
	f.p.now = func() time.Time { return time.Unix(0, now.Load()) }
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	probe, err := NewProbePicker(f.channels, f.keys, f.master)
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		assert.Equal(t, "A", pickEndpointLabel(t, probe, f.ch))
	}
	assert.Equal(t, "B", pickEndpointLabel(t, f.p, f.ch), "manual probes do not advance business sequence")
	rows := f.rows
	f.rows = nil
	_, err = f.p.Pick(context.Background(), f.ch, "a")
	assert.ErrorIs(t, err, provider.ErrNoCandidate)
	emptyState := f.p.states[1]
	assert.Empty(t, emptyState.current)
	assert.Zero(t, emptyState.refs)
	now.Store(int64(pickerIdleTTL))
	other := f.p.retain(2)
	f.p.release(other)
	assert.NotContains(t, f.p.states, uint64(1), "empty state is also reclaimed")
	f.rows = rows
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	assert.NotSame(t, emptyState, f.p.states[1])
}
