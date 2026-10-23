package channel

import (
	"context"
	"errors"
	"math"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/provider"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

type endpointFixture struct {
	p        *endpointPicker
	ch       *store.Channel
	channels *mappingRepoFake
	keys     *keyFake
	rows     []store.ChannelKey
	mapping  *store.ChannelModel
	master   []byte
	reads    int
	mappings int
}

func endpointRows(t *testing.T, channelID uint64, master []byte, weights ...int32) []store.ChannelKey {
	t.Helper()
	rows := make([]store.ChannelKey, len(weights))
	for i, weight := range weights {
		enc, err := secret.Encrypt([]byte("endpoint-"+string(rune('A'+i))+"-private"), master)
		require.NoError(t, err)
		rows[i] = store.ChannelKey{ID: uint64(i + 1), ChannelID: channelID,
			Weight: weight, Status: 1, ProviderKeyEnc: enc}
	}
	return rows
}

func newEndpointFixture(t *testing.T, weights ...int32) *endpointFixture {
	t.Helper()
	f := &endpointFixture{master: make([]byte, 32),
		ch:      &store.Channel{ID: 1, Enabled: true, Provider: "openai", BaseURL: "http://localhost/prefix", Weight: 9999},
		mapping: &store.ChannelModel{ChannelID: 1, Model: "a", UpstreamModel: "mapped-a"}}
	f.rows = endpointRows(t, 1, f.master, weights...)
	f.keys = &keyFake{list: func(context.Context, int64) ([]store.ChannelKey, error) {
		f.reads++
		return f.rows, nil
	}}
	f.channels = &mappingRepoFake{get: func(context.Context, int64, string) (*store.ChannelModel, error) {
		f.mappings++
		return f.mapping, nil
	}}
	p, err := NewEndpointPicker(f.channels, f.keys, f.master)
	require.NoError(t, err)
	f.p = p.(*endpointPicker)
	return f
}

func endpointLabel(ep *provider.Endpoint) string {
	if ep == nil {
		return "nil"
	}
	for _, label := range []string{"A", "B", "C"} {
		if ep.APIKey == "endpoint-"+label+"-private" {
			return label
		}
	}
	return "unexpected"
}

func pickEndpointLabel(t *testing.T, p EndpointPicker, ch *store.Channel) string {
	t.Helper()
	ep, err := p.Pick(context.Background(), ch, "a")
	require.NoError(t, err)
	return endpointLabel(ep)
}

func TestEndpointPickerExactSequencesAndPeriods(t *testing.T) {
	for _, tc := range []struct {
		name    string
		weights []int32
		first   []string
		counts  map[string]int
	}{
		{"5:1:1", []int32{5, 1, 1}, []string{"A", "A", "B", "A", "C", "A", "A"}, map[string]int{"A": 50, "B": 10, "C": 10}},
		{"tie", []int32{1, 1}, []string{"A", "B"}, map[string]int{"A": 10, "B": 10}},
		{"single", []int32{1}, []string{"A"}, map[string]int{"A": 10}},
		{"max weight single", []int32{10000}, []string{"A"}, map[string]int{"A": 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEndpointFixture(t, tc.weights...)
			// Repository order and channel weight must not control Key ordering.
			for i, j := 0, len(f.rows)-1; i < j; i, j = i+1, j-1 {
				f.rows[i], f.rows[j] = f.rows[j], f.rows[i]
			}
			got := make([]string, len(tc.first))
			for i := range got {
				got[i] = pickEndpointLabel(t, f.p, f.ch)
			}
			assert.Equal(t, tc.first, got)
			counts := map[string]int{}
			for i := 0; i < len(tc.first)*10; i++ {
				f.ch.Weight = int32(i)
				counts[pickEndpointLabel(t, f.p, f.ch)]++
			}
			assert.Equal(t, tc.counts, counts)
			assert.Equal(t, len(tc.first)*11, f.reads, "every selection must read a fresh snapshot")
		})
	}
}

func TestEndpointPickerConstructorsAndInputValidation(t *testing.T) {
	var nilChannels *mappingRepoFake
	var nilKeys *keyFake
	for _, tc := range []struct {
		c store.ChannelRepo
		k store.KeyRepo
		n int
	}{{nil, &keyFake{}, 32}, {nilChannels, &keyFake{}, 32}, {&mappingRepoFake{}, nil, 32},
		{&mappingRepoFake{}, nilKeys, 32}, {&mappingRepoFake{}, &keyFake{}, 0},
		{&mappingRepoFake{}, &keyFake{}, 31}, {&mappingRepoFake{}, &keyFake{}, 33}} {
		p, err := NewEndpointPicker(tc.c, tc.k, make([]byte, tc.n))
		assert.Error(t, err)
		assert.Nil(t, p)
	}
	for name, mutate := range map[string]func(*store.Channel){
		"id zero":     func(ch *store.Channel) { ch.ID = 0 },
		"id overflow": func(ch *store.Channel) { ch.ID = math.MaxInt64 + 1 },
		"provider":    func(ch *store.Channel) { ch.Provider = "mock" },
		"userinfo":    func(ch *store.Channel) { ch.BaseURL = "http://private:credential@host" },
		"query":       func(ch *store.Channel) { ch.BaseURL = "http://host?private=credential" },
		"fragment":    func(ch *store.Channel) { ch.BaseURL = "http://host#private" },
		"port":        func(ch *store.Channel) { ch.BaseURL = "http://host:65536" },
		"scheme":      func(ch *store.Channel) { ch.BaseURL = "file:///private" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newEndpointFixture(t, 1)
			mutate(f.ch)
			ep, err := f.p.Pick(context.Background(), f.ch, "a")
			require.Error(t, err)
			assert.NotErrorIs(t, err, provider.ErrNoCandidate)
			assert.NotContains(t, err.Error(), "private")
			assert.Nil(t, ep)
			assert.Zero(t, f.reads)
		})
	}
	f := newEndpointFixture(t, 1)
	_, err := f.p.Pick(context.Background(), nil, "a")
	assert.Error(t, err)
	_, err = f.p.Pick(context.Background(), f.ch, "")
	assert.Error(t, err)
	f.ch.Enabled = false
	_, err = f.p.Pick(context.Background(), f.ch, "a")
	assert.ErrorIs(t, err, provider.ErrNoCandidate)
	assert.Zero(t, f.reads)
	f.ch.Enabled = true
	f.ch.ID = math.MaxInt64
	f.rows[0].ChannelID = f.ch.ID
	f.mapping.ChannelID = f.ch.ID
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch), "MaxInt64 channel ID is valid")
}

func TestEndpointPickerActiveValidation(t *testing.T) {
	for name, mutate := range map[string]func(*endpointFixture){
		"zero ID":          func(f *endpointFixture) { f.rows[0].ID = 0 },
		"overflow ID":      func(f *endpointFixture) { f.rows[0].ID = math.MaxInt64 + 1 },
		"duplicate ID":     func(f *endpointFixture) { f.rows = append(f.rows, f.rows[0]) },
		"wrong owner":      func(f *endpointFixture) { f.rows[0].ChannelID = 2 },
		"zero weight":      func(f *endpointFixture) { f.rows[0].Weight = 0 },
		"negative weight":  func(f *endpointFixture) { f.rows[0].Weight = -1 },
		"weight too large": func(f *endpointFixture) { f.rows[0].Weight = 10001 },
	} {
		t.Run(name, func(t *testing.T) {
			f := newEndpointFixture(t, 1)
			mutate(f)
			_, err := f.p.Pick(context.Background(), f.ch, "a")
			require.Error(t, err)
			assert.NotErrorIs(t, err, provider.ErrNoCandidate)
			assert.Zero(t, f.mappings)
		})
	}
	f := newEndpointFixture(t, 1)
	f.rows[0].ID = math.MaxInt64
	f.rows = append(f.rows, store.ChannelKey{ID: 0, Weight: -100, Status: 0},
		store.ChannelKey{ID: math.MaxUint64, Weight: 10001, Status: -1},
		store.ChannelKey{ID: math.MaxInt64, Weight: 0, Status: 2})
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch), "all nonactive records are ignored")
}

func TestEndpointPickerSnapshotChangesResetSequence(t *testing.T) {
	for name, mutate := range map[string]func(*testing.T, *endpointFixture){
		"add": func(t *testing.T, f *endpointFixture) {
			f.rows = append(f.rows, endpointRows(t, 1, f.master, 1, 1, 1)[2])
		},
		"delete":  func(_ *testing.T, f *endpointFixture) { f.rows = f.rows[:1] },
		"disable": func(_ *testing.T, f *endpointFixture) { f.rows[1].Status = 0 },
		"weight":  func(_ *testing.T, f *endpointFixture) { f.rows[0].Weight = 2 },
		"ciphertext": func(t *testing.T, f *endpointFixture) {
			enc, err := secret.Encrypt([]byte("endpoint-A-private"), f.master)
			require.NoError(t, err)
			copy(f.rows[0].ProviderKeyEnc, enc) // same backing array, different nonce
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newEndpointFixture(t, 1, 1)
			assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
			mutate(t, f)
			assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch), "a changed participating set restarts from zero")
			assert.Len(t, f.p.states[1].snapshot, len(func() []store.ChannelKey {
				s, _, err := endpointSnapshot(f.rows, 1)
				require.NoError(t, err)
				return s
			}()))
		})
	}
	f := newEndpointFixture(t, 1, 1)
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	f.rows[1].Status = 0
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	f.rows[1].Status = 1
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch), "reenabling resets")
	f.rows[0].Status, f.rows[1].Status = 0, 0
	_, err := f.p.Pick(context.Background(), f.ch, "a")
	assert.ErrorIs(t, err, provider.ErrNoCandidate)
	assert.Empty(t, f.p.states[1].snapshot)
	assert.Empty(t, f.p.states[1].current)
	f.rows[0].Status, f.rows[1].Status = 1, 1
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch), "empty set discarded the previous position")
}

func TestEndpointPickerLiveMappingAndCopies(t *testing.T) {
	f := newEndpointFixture(t, 1, 1)
	before := *f.ch
	ctx := context.WithValue(context.Background(), testContextKey{}, "endpoint")
	f.keys.list = func(got context.Context, id int64) ([]store.ChannelKey, error) {
		assert.Same(t, ctx, got)
		assert.Equal(t, int64(1), id)
		return f.rows, nil
	}
	f.channels.get = func(got context.Context, id int64, model string) (*store.ChannelModel, error) {
		assert.Same(t, ctx, got)
		assert.Equal(t, f.mapping.Model, model)
		return f.mapping, nil
	}
	f.master[0] = 1 // constructor must have copied the input
	ep, err := f.p.Pick(ctx, f.ch, "a")
	require.NoError(t, err)
	assert.Equal(t, "A", endpointLabel(ep))
	ep.APIKey, ep.Model, ep.BaseURL, ep.Provider, ep.ChannelID = "caller", "caller", "caller", "caller", 99
	assert.Equal(t, before, *f.ch)
	assert.Equal(t, "mapped-a", f.mapping.UpstreamModel)
	f.mapping.Model, f.mapping.UpstreamModel = "b", "mapped-b"
	f.ch.BaseURL = "https://localhost/new/v1"
	next, err := f.p.Pick(ctx, f.ch, "b")
	require.NoError(t, err)
	assert.NotSame(t, ep, next)
	assert.Equal(t, "B", endpointLabel(next), "switching models must not reset the Key sequence")
	assert.Equal(t, "mapped-b", next.Model)
	assert.Equal(t, f.ch.BaseURL, next.BaseURL)
	assert.Equal(t, "openai", next.Provider)
	assert.Equal(t, int64(1), next.ChannelID)
	// The installed signature owns its ciphertext bytes.
	copyOfSnapshot := append([]byte(nil), f.p.states[1].snapshot[0].ProviderKeyEnc...)
	f.rows[0].ProviderKeyEnc[0] ^= 1
	assert.Equal(t, copyOfSnapshot, f.p.states[1].snapshot[0].ProviderKeyEnc)
}

func TestEndpointPickerNonparticipatingChangesPreserveSequence(t *testing.T) {
	f := newEndpointFixture(t, 1, 1)
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	f.rows[0], f.rows[1] = f.rows[1], f.rows[0]
	f.rows = append(f.rows, store.ChannelKey{ID: 0, Status: 0, Weight: -1, ProviderKeyEnc: []byte("inactive")})
	assert.Equal(t, "B", pickEndpointLabel(t, f.p, f.ch), "repository order and inactive rows are outside the signature")
	assert.Equal(t, uint64(2), f.rows[0].ID, "sorting must not mutate repository-owned rows")
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	f.rows[2].Weight, f.rows[2].Status, f.rows[2].ProviderKeyEnc = 10001, -1, []byte("changed-inactive")
	assert.Equal(t, "B", pickEndpointLabel(t, f.p, f.ch), "inactive changes must not reset an active sequence")
}

func TestEndpointPickerFailureClassificationAndConsumedSlots(t *testing.T) {
	for _, stage := range []string{"keys", "mapping"} {
		t.Run(stage, func(t *testing.T) {
			f := newEndpointFixture(t, 1, 1)
			cause := &mysqldriver.MySQLError{Number: 1234, Message: "private SQL credential"}
			if stage == "keys" {
				f.keys.list = func(context.Context, int64) ([]store.ChannelKey, error) { return nil, cause }
			} else {
				f.channels.get = func(context.Context, int64, string) (*store.ChannelModel, error) { return nil, cause }
			}
			_, err := f.p.Pick(context.Background(), f.ch, "a")
			var classified *mysqldriver.MySQLError
			assert.ErrorAs(t, err, &classified)
			assert.ErrorIs(t, err, cause)
			assert.NotErrorIs(t, err, provider.ErrNoCandidate)
			assert.NotContains(t, err.Error(), "private")
		})
	}
	f := newEndpointFixture(t, 1, 1)
	f.channels.get = func(context.Context, int64, string) (*store.ChannelModel, error) { return nil, store.ErrNotFound }
	_, err := f.p.Pick(context.Background(), f.ch, "a")
	assert.ErrorIs(t, err, store.ErrNotFound)
	assert.NotErrorIs(t, err, provider.ErrNoCandidate)
	assert.NotErrorIs(t, err, errProbeMapping, "business mapping errors are not manual-test request errors")
	for _, mapping := range []*store.ChannelModel{nil, {ChannelID: 1, Model: "wrong", UpstreamModel: "up"}} {
		f.mapping = mapping
		f.channels.get = func(context.Context, int64, string) (*store.ChannelModel, error) { return f.mapping, nil }
		_, err = f.p.Pick(context.Background(), f.ch, "a")
		assert.Error(t, err)
	}
	for _, stage := range []string{"decrypt", "empty Key", "assembly"} {
		t.Run(stage, func(t *testing.T) {
			f := newEndpointFixture(t, 1, 1)
			switch stage {
			case "decrypt":
				f.rows[0].ProviderKeyEnc = []byte("private-corrupt-ciphertext")
			case "empty Key":
				f.rows[0].ProviderKeyEnc, err = secret.Encrypt(nil, f.master)
				require.NoError(t, err)
			default:
				f.mapping.UpstreamModel = ""
			}
			_, err := f.p.Pick(context.Background(), f.ch, "a")
			require.Error(t, err)
			assert.NotErrorIs(t, err, provider.ErrNoCandidate)
			assert.NotContains(t, err.Error(), "private")
			assert.Equal(t, 1, f.reads, "no retry on failure")
			assert.Equal(t, []int64{-1, 1}, f.p.states[1].current, "failed assembly consumed A exactly once")
			f.mapping.UpstreamModel = "mapped-a"
			assert.Equal(t, "B", pickEndpointLabel(t, f.p, f.ch))
			assert.Equal(t, 2, f.reads)
		})
	}
	f = newEndpointFixture(t, 5, 1, 1)
	f.rows[1].ProviderKeyEnc, f.rows[2].ProviderKeyEnc = []byte("bad-backup"), []byte("bad-backup")
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch), "unselected ciphertext must never be decrypted")
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
	_, err = f.p.Pick(context.Background(), f.ch, "a")
	assert.Error(t, err, "the third slot must fail on B rather than retry A")
	assert.Equal(t, 3, f.reads)
	assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
}

func TestEndpointPickerCanceledBeforeSelectionDoesNotAdvance(t *testing.T) {
	for _, stage := range []string{"entry", "keys", "mapping"} {
		t.Run(stage, func(t *testing.T) {
			f := newEndpointFixture(t, 1, 1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch stage {
			case "entry":
				cancel()
			case "keys":
				f.keys.list = func(context.Context, int64) ([]store.ChannelKey, error) { cancel(); return f.rows, nil }
			case "mapping":
				f.channels.get = func(context.Context, int64, string) (*store.ChannelModel, error) { cancel(); return f.mapping, nil }
			}
			_, err := f.p.Pick(ctx, f.ch, "a")
			assert.ErrorIs(t, err, context.Canceled)
			if state := f.p.states[1]; state != nil {
				assert.Empty(t, state.current)
				assert.Zero(t, state.refs)
			}
			f.keys.list = func(context.Context, int64) ([]store.ChannelKey, error) { return f.rows, nil }
			f.channels.get = func(context.Context, int64, string) (*store.ChannelModel, error) { return f.mapping, nil }
			assert.Equal(t, "A", pickEndpointLabel(t, f.p, f.ch))
		})
	}
}

func TestEndpointPickerCheckedArithmeticAndAtomicOverflow(t *testing.T) {
	for _, tc := range []struct {
		a, b, want int64
		ok         bool
	}{
		{math.MaxInt64, 1, 0, false}, {math.MinInt64, -1, 0, false},
		{math.MaxInt64 - 1, 1, math.MaxInt64, true}, {math.MinInt64 + 1, -1, math.MinInt64, true},
		{0, math.MinInt64, math.MinInt64, true}, {0, math.MaxInt64, math.MaxInt64, true},
	} {
		got, ok := checkedWeightAdd(tc.a, tc.b)
		assert.Equal(t, tc.ok, ok)
		assert.Equal(t, tc.want, got)
	}
	for name, initial := range map[string][]int64{
		"addition": {math.MaxInt64, 0}, "subtraction": {math.MinInt64, math.MinInt64},
	} {
		t.Run(name, func(t *testing.T) {
			f := newEndpointFixture(t, 1, 1)
			snapshot, total, err := endpointSnapshot(f.rows, 1)
			require.NoError(t, err)
			state := &pickerState{snapshot: snapshot, current: append([]int64(nil), initial...)}
			_, err = state.choose(context.Background(), snapshot, total)
			require.Error(t, err)
			assert.False(t, errors.Is(err, provider.ErrNoCandidate))
			assert.Equal(t, initial, state.current, "overflow must not partially commit")
			assert.Equal(t, snapshot, state.snapshot)
		})
	}
}
