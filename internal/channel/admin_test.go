package channel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/secret"
	"gaigate/internal/store"
)

type channelFake struct {
	store.ChannelRepo
	list   func(context.Context) ([]store.Channel, error)
	get    func(context.Context, int64) (*store.Channel, error)
	models func(context.Context, int64) ([]store.ChannelModel, error)
	create func(context.Context, *store.Channel, []store.ChannelModel, [][]byte) error
	update func(context.Context, *store.Channel, []store.ChannelModel) error
	remove func(context.Context, int64) error
}

type testContextKey struct{}

func (f *channelFake) List(ctx context.Context) ([]store.Channel, error) { return f.list(ctx) }
func (f *channelFake) Get(ctx context.Context, id int64) (*store.Channel, error) {
	return f.get(ctx, id)
}
func (f *channelFake) ListModels(ctx context.Context, id int64) ([]store.ChannelModel, error) {
	return f.models(ctx, id)
}
func (f *channelFake) Create(ctx context.Context, ch *store.Channel, models []store.ChannelModel, enc [][]byte) error {
	return f.create(ctx, ch, models, enc)
}
func (f *channelFake) UpdateConfig(ctx context.Context, ch *store.Channel, models []store.ChannelModel) error {
	return f.update(ctx, ch, models)
}
func (f *channelFake) Delete(ctx context.Context, id int64) error { return f.remove(ctx, id) }

type modelFake struct {
	store.ModelRepo
	list func(context.Context) ([]store.ModelAlias, error)
}

func (f *modelFake) List(ctx context.Context) ([]store.ModelAlias, error) { return f.list(ctx) }

type keyFake struct {
	store.KeyRepo
	list   func(context.Context, int64) ([]store.ChannelKey, error)
	create func(context.Context, int64, []byte) (*store.ChannelKey, error)
	update func(context.Context, int64, int64, int32, int8) (*store.ChannelKey, error)
	remove func(context.Context, int64, int64) error
}

func (f *keyFake) ListByChannel(ctx context.Context, id int64) ([]store.ChannelKey, error) {
	return f.list(ctx, id)
}
func (f *keyFake) CreateOwned(ctx context.Context, id int64, enc []byte) (*store.ChannelKey, error) {
	return f.create(ctx, id, enc)
}
func (f *keyFake) UpdateOwned(ctx context.Context, id, key int64, w int32, status int8) (*store.ChannelKey, error) {
	return f.update(ctx, id, key, w, status)
}
func (f *keyFake) RemoveOwned(ctx context.Context, id, key int64) error {
	return f.remove(ctx, id, key)
}

type invalidationSpy struct {
	store.Invalidator
	calls int
}

func (f *invalidationSpy) InvalidateAll() { f.calls++ }

func validInput() CreateChannelInput {
	return CreateChannelInput{Name: "  渠道  ", Provider: "openai", BaseURL: "http://127.0.0.1:8080/prefix/v1/",
		Priority: 0, Weight: 1, Enabled: false, Models: []ModelMappingInput{{Model: "a", UpstreamModel: "up"}}, Keys: []string{"raw-private"}}
}

func newFakeService(t *testing.T) (*adminService, *channelFake, *keyFake, *modelFake, *invalidationSpy) {
	t.Helper()
	c, k, m, inv := &channelFake{}, &keyFake{}, &modelFake{}, &invalidationSpy{}
	m.list = func(context.Context) ([]store.ModelAlias, error) { return []store.ModelAlias{{Name: "a"}}, nil }
	s, err := NewAdminService(c, k, m, inv, []byte(strings.Repeat("m", 32)))
	require.NoError(t, err)
	return s.(*adminService), c, k, m, inv
}

func TestAdminConstructor(t *testing.T) {
	s, c, k, m, inv := newFakeService(t)
	var nc *channelFake
	var nk *keyFake
	var nm *modelFake
	var ni *invalidationSpy
	for i, deps := range []struct {
		c store.ChannelRepo
		k store.KeyRepo
		m store.ModelRepo
		i store.Invalidator
	}{{nil, k, m, inv}, {nc, k, m, inv}, {c, nil, m, inv}, {c, nk, m, inv},
		{c, k, nil, inv}, {c, k, nm, inv}, {c, k, m, nil}, {c, k, m, ni}} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			got, err := NewAdminService(deps.c, deps.k, deps.m, deps.i, s.masterKey)
			require.Error(t, err)
			assert.Nil(t, got)
		})
	}
	for _, n := range []int{0, 31, 33} {
		got, err := NewAdminService(c, k, m, inv, make([]byte, n))
		require.Error(t, err)
		assert.Nil(t, got)
	}
	master := []byte(strings.Repeat("x", 32))
	got, err := NewAdminService(c, k, m, inv, master)
	require.NoError(t, err)
	master[0] = 'y'
	assert.Equal(t, byte('x'), got.(*adminService).masterKey[0])
}

func TestAdminValidationBeforeEncryptionOrWrite(t *testing.T) {
	cases := map[string]func(*CreateChannelInput){
		"empty name":       func(i *CreateChannelInput) { i.Name = "" },
		"129 runes":        func(i *CreateChannelInput) { i.Name = strings.Repeat("中", 129) },
		"invalid utf8":     func(i *CreateChannelInput) { i.Name = "\xff" },
		"provider":         func(i *CreateChannelInput) { i.Provider = "mock" },
		"weight zero":      func(i *CreateChannelInput) { i.Weight = 0 },
		"weight high":      func(i *CreateChannelInput) { i.Weight = 10001 },
		"weight negative":  func(i *CreateChannelInput) { i.Weight = -1 },
		"no models":        func(i *CreateChannelInput) { i.Models = nil },
		"65 models":        func(i *CreateChannelInput) { i.Models = make([]ModelMappingInput, 65) },
		"duplicate model":  func(i *CreateChannelInput) { i.Models = append(i.Models, i.Models[0]) },
		"unknown model":    func(i *CreateChannelInput) { i.Models[0].Model = "unknown" },
		"empty model":      func(i *CreateChannelInput) { i.Models[0].Model = "" },
		"long model":       func(i *CreateChannelInput) { i.Models[0].Model = strings.Repeat("中", 129) },
		"empty upstream":   func(i *CreateChannelInput) { i.Models[0].UpstreamModel = "" },
		"long upstream":    func(i *CreateChannelInput) { i.Models[0].UpstreamModel = strings.Repeat("中", 129) },
		"no keys":          func(i *CreateChannelInput) { i.Keys = nil },
		"65 keys":          func(i *CreateChannelInput) { i.Keys = make([]string, 65) },
		"empty key":        func(i *CreateChannelInput) { i.Keys[0] = "" },
		"485 byte key":     func(i *CreateChannelInput) { i.Keys[0] = strings.Repeat("k", 485) },
		"late invalid key": func(i *CreateChannelInput) { i.Keys = append(i.Keys, "bad\nkey") },
		"unicode control":  func(i *CreateChannelInput) { i.Keys[0] = "bad\u0085key" },
	}
	for _, u := range []string{"", "/relative", "ftp://host", "http:opaque", "http://", "http://u:p@host",
		"http://host?", "http://host?q=x", "http://host#", "http://host#frag", "http://host:0", "http://host:65536",
		"http://host:", "http://host:abc", "http://[::1", "http://ho st", "http://host/" + strings.Repeat("x", 244)} {
		cases["url "+u] = func(i *CreateChannelInput) { i.BaseURL = u }
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s, _, _, _, inv := newFakeService(t)
			in := validInput()
			mutate(&in)
			s.encrypt = func([]byte, []byte) ([]byte, error) { t.Fatal("invalid input encrypted"); return nil, nil }
			got, err := s.Create(context.Background(), in)
			require.ErrorIs(t, err, errInvalidInput)
			assert.Nil(t, got)
			assert.Zero(t, inv.calls) // unset fake write functions also panic if called
			update := UpdateChannelInput{Name: in.Name, Provider: in.Provider, BaseURL: in.BaseURL, Weight: in.Weight, Models: in.Models}
			if validateConfig(update) != nil || name == "unknown model" {
				got, err = s.Update(context.Background(), 1, update)
				assert.ErrorIs(t, err, errInvalidInput)
				assert.Nil(t, got)
			}
		})
	}
}

func TestAdminValidBoundaries(t *testing.T) {
	s, c, k, m, inv := newFakeService(t)
	in := validInput()
	in.Name = strings.Repeat("中", 128)
	in.BaseURL = "https://[::1]:65535/" + strings.Repeat("x", 235)
	require.Len(t, in.BaseURL, 255)
	in.Priority, in.Weight = math.MinInt32, 10000
	in.Models, in.Keys = nil, nil
	aliases := make([]store.ModelAlias, 64)
	for n := range aliases {
		aliases[n].Name = fmt.Sprintf("%064d", n)
		in.Models = append(in.Models, ModelMappingInput{Model: aliases[n].Name, UpstreamModel: strings.Repeat("文", 128)})
		in.Keys = append(in.Keys, strings.Repeat("k", 484))
	}
	m.list = func(context.Context) ([]store.ModelAlias, error) { return aliases, nil }
	ctx := context.WithValue(context.Background(), testContextKey{}, "caller")
	c.create = func(got context.Context, ch *store.Channel, models []store.ChannelModel, enc [][]byte) error {
		assert.Same(t, ctx, got)
		assert.Equal(t, in.Name, ch.Name)
		assert.Equal(t, int32(math.MinInt32), ch.Priority)
		assert.False(t, ch.Enabled)
		assert.Len(t, models, 64)
		require.Len(t, enc, 64)
		for _, ciphertext := range enc {
			assert.Len(t, ciphertext, 512)
			raw, err := secret.Decrypt(ciphertext, s.masterKey)
			require.NoError(t, err)
			assert.Equal(t, in.Keys[0], string(raw))
		}
		assert.NotEqual(t, enc[0], enc[1])
		ch.ID = 9
		return nil
	}
	c.get = func(context.Context, int64) (*store.Channel, error) {
		assert.Equal(t, 1, inv.calls, "invalidate before response read")
		return &store.Channel{ID: 9}, nil
	}
	c.models = func(context.Context, int64) ([]store.ChannelModel, error) { return nil, nil }
	k.list = func(context.Context, int64) ([]store.ChannelKey, error) { return nil, nil }
	got, err := s.Create(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, int64(9), got.ID)
	for _, base := range []string{"http://localhost", "https://example.com/v1", "http://10.0.0.1:80/prefix/", "http://[::1]:1"} {
		assert.True(t, validBaseURL(base), base)
	}
}

func TestAdminViewsAreWhitelistedSortedAndIndependent(t *testing.T) {
	s, c, k, _, _ := newFakeService(t)
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.FixedZone("test", 8*3600))
	ch := &store.Channel{ID: 3, CreatedAt: now, UpdatedAt: now, LastCheckedAt: &now,
		ErrorRateCache: .5, LatencyP99: 999}
	models := []store.ChannelModel{{ChannelID: 3, Model: "z"}, {ChannelID: 3, Model: "a"}}
	keys := []store.ChannelKey{{ID: 7, ChannelID: 3, ProviderKeyEnc: []byte("private-ciphertext"), CreatedAt: now}, {ID: 2, ChannelID: 3}}
	c.get = func(context.Context, int64) (*store.Channel, error) { return ch, nil }
	c.models = func(context.Context, int64) ([]store.ChannelModel, error) { return models, nil }
	k.list = func(context.Context, int64) ([]store.ChannelKey, error) { return keys, nil }
	detail, err := s.Get(context.Background(), 3)
	require.NoError(t, err)
	assert.Equal(t, "a", detail.Models[0].Model)
	assert.Equal(t, int64(2), detail.Keys[0].ID)
	assert.Equal(t, time.UTC, detail.CreatedAt.Location())
	assert.Equal(t, "2026-10-22T04:00:00Z", detail.CreatedAt.Format(time.RFC3339))
	raw, err := json.Marshal(detail)
	require.NoError(t, err)
	var obj map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &obj))
	assert.Len(t, obj, 13)
	for _, field := range []string{"id", "name", "provider", "base_url", "priority", "weight", "enabled", "health_status", "last_checked_at", "created_at", "updated_at", "models", "keys"} {
		assert.Contains(t, obj, field)
	}
	var keyObjects []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(obj["keys"], &keyObjects))
	assert.Len(t, keyObjects[0], 5)
	for _, field := range []string{"id", "channel_id", "weight", "status", "created_at"} {
		assert.Contains(t, keyObjects[0], field)
	}
	assert.NotContains(t, string(raw), "private-ciphertext")
	*detail.LastCheckedAt = time.Time{}
	detail.Models[0].Model = "caller"
	detail.Keys[0].Weight = 999
	assert.Equal(t, "z", models[0].Model)
	assert.Equal(t, uint64(7), keys[0].ID)
	assert.Equal(t, now, *ch.LastCheckedAt)
	ch.LastCheckedAt = nil
	c.models = func(context.Context, int64) ([]store.ChannelModel, error) { return nil, nil }
	k.list = func(context.Context, int64) ([]store.ChannelKey, error) { return nil, nil }
	detail, err = s.Get(context.Background(), 3)
	require.NoError(t, err)
	raw, err = json.Marshal(detail)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"last_checked_at":null`)
	assert.Contains(t, string(raw), `"models":[]`)
	assert.Contains(t, string(raw), `"keys":[]`)
	c.list = func(context.Context) ([]store.Channel, error) { return []store.Channel{{ID: 9}, *ch}, nil }
	list, err := s.List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(3), list[0].ID)
	c.list = func(context.Context) ([]store.Channel, error) { return nil, nil }
	list, err = s.List(context.Background())
	require.NoError(t, err)
	assert.NotNil(t, list)
}

func TestAdminPrewriteFailuresAndCommittedResponseFailure(t *testing.T) {
	privateCause := errors.New("raw-private private-ciphertext hash-private https://private-url master-private")
	for _, stage := range []string{"aliases", "encryption", "transaction", "response"} {
		t.Run(stage, func(t *testing.T) {
			s, c, _, m, inv := newFakeService(t)
			writes, encrypted := 0, 0
			s.encrypt = func(raw, master []byte) ([]byte, error) {
				encrypted++
				if stage == "encryption" && encrypted == 2 {
					return nil, privateCause
				}
				return secret.Encrypt(raw, master)
			}
			if stage == "aliases" {
				m.list = func(context.Context) ([]store.ModelAlias, error) { return nil, privateCause }
			}
			c.create = func(_ context.Context, ch *store.Channel, _ []store.ChannelModel, _ [][]byte) error {
				writes++
				if stage == "transaction" {
					return privateCause
				}
				ch.ID = 1
				return nil
			}
			c.get = func(context.Context, int64) (*store.Channel, error) { return nil, privateCause }
			in := validInput()
			in.Keys = append(in.Keys, "second-private")
			got, err := s.Create(context.Background(), in)
			require.ErrorIs(t, err, privateCause)
			assert.Nil(t, got)
			assert.NotContains(t, err.Error(), "private")
			var log strings.Builder
			slog.New(slog.NewJSONHandler(&log, nil)).Error("operation failed", "err", err)
			assert.NotContains(t, log.String(), "private")
			if stage == "aliases" || stage == "encryption" {
				assert.Zero(t, writes)
			} else {
				assert.Equal(t, 1, writes)
			}
			if stage == "response" {
				assert.Equal(t, 1, inv.calls)
			} else {
				assert.Zero(t, inv.calls)
			}
		})
	}
}

func TestAdminKeyInputsAndWriteInvalidation(t *testing.T) {
	s, c, k, _, inv := newFakeService(t)
	ctx := context.WithValue(context.Background(), testContextKey{}, "owner-context")
	k.create = func(got context.Context, id int64, enc []byte) (*store.ChannelKey, error) {
		assert.Same(t, ctx, got)
		plain, err := secret.Decrypt(enc, s.masterKey)
		require.NoError(t, err)
		assert.Equal(t, "new-private", string(plain))
		return &store.ChannelKey{ID: 5, ChannelID: uint64(id), Weight: 1, Status: 1}, nil
	}
	k.update = func(got context.Context, id, key int64, w int32, status int8) (*store.ChannelKey, error) {
		assert.Same(t, ctx, got)
		return &store.ChannelKey{ID: uint64(key), ChannelID: uint64(id), Weight: w, Status: status}, nil
	}
	k.remove = func(got context.Context, id, key int64) error {
		assert.Same(t, ctx, got)
		assert.Equal(t, int64(3), id)
		assert.Equal(t, int64(5), key)
		return nil
	}
	c.remove = func(got context.Context, id int64) error { assert.Same(t, ctx, got); return nil }
	key, err := s.AddKey(ctx, 3, "new-private")
	require.NoError(t, err)
	assert.Equal(t, int32(1), key.Weight)
	key, err = s.UpdateKey(ctx, 3, 5, 10000, 0)
	require.NoError(t, err)
	assert.Zero(t, key.Status)
	require.NoError(t, s.RemoveKey(ctx, 3, 5))
	require.NoError(t, s.Delete(ctx, 3))
	assert.Equal(t, 4, inv.calls)
	for _, raw := range []string{"", "bad\x00", strings.Repeat("k", 485)} {
		_, err := s.AddKey(ctx, 3, raw)
		assert.ErrorIs(t, err, errInvalidInput)
	}
	for _, values := range [][4]int64{{0, 5, 1, 1}, {3, 0, 1, 1}, {3, 5, 0, 1}, {3, 5, 10001, 1}, {3, 5, 1, -1}, {3, 5, 1, 2}} {
		_, err := s.UpdateKey(ctx, values[0], values[1], int32(values[2]), int8(values[3]))
		assert.ErrorIs(t, err, errInvalidInput)
	}
	assert.ErrorIs(t, s.RemoveKey(ctx, 0, 5), errInvalidInput)
	assert.ErrorIs(t, s.RemoveKey(ctx, 3, 0), errInvalidInput)
	assert.ErrorIs(t, s.Delete(ctx, 0), errInvalidInput)
	_, err = s.Get(ctx, 0)
	assert.ErrorIs(t, err, errInvalidInput)
	_, err = s.Update(ctx, 0, UpdateChannelInput{})
	assert.ErrorIs(t, err, errInvalidInput)
	assert.Equal(t, 4, inv.calls)
}

func TestAdminCancellationAndConcurrentReads(t *testing.T) {
	s, c, _, _, inv := newFakeService(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := s.Create(ctx, validInput())
	assert.ErrorIs(t, err, context.Canceled)
	_, err = s.AddKey(ctx, 1, "private")
	assert.ErrorIs(t, err, context.Canceled)
	_, err = s.UpdateKey(ctx, 1, 1, 1, 1)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, s.RemoveKey(ctx, 1, 1), context.Canceled)
	assert.ErrorIs(t, s.Delete(ctx, 1), context.Canceled)
	_, err = s.Get(ctx, 1)
	assert.ErrorIs(t, err, context.Canceled)
	_, err = s.List(ctx)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, inv.calls)
	checked := time.Now()
	c.list = func(context.Context) ([]store.Channel, error) {
		return []store.Channel{{ID: 1, LastCheckedAt: &checked}}, nil
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			views, err := s.List(context.Background())
			assert.NoError(t, err)
			*views[0].LastCheckedAt = time.Time{}
		}()
	}
	wg.Wait()
	assert.False(t, checked.IsZero())
}
