package channel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/provider"
	"gaigate/internal/secret"
	"gaigate/internal/store"
)

func TestEndpointPickerMySQLMappedEndpointAndLifecycle(t *testing.T) {
	db := adminMySQL(t) // serial, real migrations, fixed gaigate_test
	service, channels, keys, _ := mysqlService(t, db)
	ctx := context.Background()
	in := validInput()
	in.Enabled, in.Weight = true, 10000
	in.Keys = []string{"endpoint-A-private", "endpoint-B-private"}
	created, err := service.Create(ctx, in)
	require.NoError(t, err)
	_, err = service.UpdateKey(ctx, created.ID, created.Keys[0].ID, 3, 1)
	require.NoError(t, err)
	ch, err := channels.Get(ctx, created.ID)
	require.NoError(t, err)
	masterInput := append([]byte(nil), service.masterKey...)
	p, err := NewEndpointPicker(channels, keys, masterInput)
	require.NoError(t, err)
	masterInput[0] ^= 1
	before, err := keys.ListByChannel(ctx, created.ID)
	require.NoError(t, err)
	for _, expected := range []string{"A", "A", "B", "A"} {
		ep, err := p.Pick(ctx, ch, "a")
		require.NoError(t, err)
		assert.Equal(t, expected, endpointLabel(ep))
		assert.Equal(t, ch.BaseURL, ep.BaseURL)
		assert.Equal(t, "up", ep.Model)
		assert.Equal(t, created.ID, ep.ChannelID)
		assert.Equal(t, "openai", ep.Provider)
		ep.APIKey, ep.Model, ep.BaseURL = "caller", "caller", "caller"
	}
	after, err := keys.ListByChannel(ctx, created.ID)
	require.NoError(t, err)
	assert.Equal(t, before, after, "selection must never update repository rows")
	// A separate replica cache and picker have an independent starting point.
	otherChannels := store.NewChannelRepo(db, store.NewConfigCache(time.Minute))
	other, err := NewEndpointPicker(otherChannels, keys, service.masterKey)
	require.NoError(t, err)
	assert.Equal(t, "A", pickEndpointLabel(t, other, ch))
	assert.Equal(t, "A", pickEndpointLabel(t, other, ch))
	assert.Equal(t, "B", pickEndpointLabel(t, other, ch))
	// Committed management changes are read on the next selection without TTL.
	_, err = service.UpdateKey(ctx, created.ID, created.Keys[0].ID, 3, 0)
	require.NoError(t, err)
	assert.Equal(t, "B", pickEndpointLabel(t, p, ch))
	_, err = service.UpdateKey(ctx, created.ID, created.Keys[1].ID, 1, 0)
	require.NoError(t, err)
	_, err = p.Pick(ctx, ch, "a")
	assert.ErrorIs(t, err, provider.ErrNoCandidate)
	_, err = service.UpdateKey(ctx, created.ID, created.Keys[0].ID, 1, 1)
	require.NoError(t, err)
	assert.Equal(t, "A", pickEndpointLabel(t, p, ch))
	_, err = p.Pick(ctx, ch, "b")
	assert.ErrorIs(t, err, store.ErrNotFound, "missing mapping cannot fall back to the alias")
	assert.NotErrorIs(t, err, provider.ErrNoCandidate)

	updated := UpdateChannelInput{Name: in.Name, Provider: "openai", BaseURL: "https://localhost/updated/v1",
		Priority: in.Priority, Weight: 1, Enabled: true,
		Models: []ModelMappingInput{{Model: "a", UpstreamModel: "new-upstream"}}}
	_, err = service.Update(ctx, created.ID, updated)
	require.NoError(t, err)
	ch, err = channels.Get(ctx, created.ID)
	require.NoError(t, err)
	ep, err := p.Pick(ctx, ch, "a")
	require.NoError(t, err)
	assert.Equal(t, "new-upstream", ep.Model)
	assert.Equal(t, updated.BaseURL, ep.BaseURL)
}

func TestEndpointPickerMySQLCorruptSelectionAndCiphertextRotation(t *testing.T) {
	db := adminMySQL(t)
	service, channels, keys, _ := mysqlService(t, db)
	ctx := context.Background()
	in := validInput()
	in.Enabled, in.Keys = true, []string{"endpoint-A-private", "endpoint-B-private"}
	created, err := service.Create(ctx, in)
	require.NoError(t, err)
	ch, err := channels.Get(ctx, created.ID)
	require.NoError(t, err)
	// Only our freshly created row is corrupted, never a seed/business channel.
	require.NoError(t, db.Model(&store.ChannelKey{}).
		Where("channel_id = ? AND id = ?", created.ID, created.Keys[0].ID).
		Update("provider_key_enc", []byte("private-corrupt-ciphertext")).Error)
	var reads atomic.Int64
	countedKeys := &keyFake{KeyRepo: keys, list: func(ctx context.Context, id int64) ([]store.ChannelKey, error) {
		reads.Add(1)
		return keys.ListByChannel(ctx, id)
	}}
	p, err := NewEndpointPicker(channels, countedKeys, service.masterKey)
	require.NoError(t, err)
	_, err = p.Pick(ctx, ch, "a")
	require.Error(t, err)
	assert.NotErrorIs(t, err, provider.ErrNoCandidate)
	assert.NotContains(t, err.Error(), "private")
	assert.Equal(t, int64(1), reads.Load(), "corrupt A must not cause a second read or backup attempt")
	assert.Equal(t, "B", pickEndpointLabel(t, p, ch), "failed A consumed its slot")
	assert.Equal(t, int64(2), reads.Load())
	enc, err := secret.Encrypt([]byte("endpoint-C-private"), service.masterKey)
	require.NoError(t, err)
	require.NoError(t, db.Model(&store.ChannelKey{}).
		Where("channel_id = ? AND id = ?", created.ID, created.Keys[0].ID).
		Update("provider_key_enc", enc).Error)
	assert.Equal(t, "C", pickEndpointLabel(t, p, ch), "ciphertext replacement resets the whole snapshot")
	assert.Equal(t, "B", pickEndpointLabel(t, p, ch))
	// Deleting the formerly selected record cannot retain a stale secret.
	require.NoError(t, service.RemoveKey(ctx, created.ID, created.Keys[0].ID))
	assert.Equal(t, "B", pickEndpointLabel(t, p, ch))
}

func TestEndpointPickerMySQLSnapshotCommitBarrier(t *testing.T) {
	db := adminMySQL(t)
	service, channels, keys, _ := mysqlService(t, db)
	ctx := context.Background()
	in := validInput()
	in.Enabled, in.Keys = true, []string{"endpoint-A-private", "endpoint-B-private"}
	created, err := service.Create(ctx, in)
	require.NoError(t, err)
	ch, err := channels.Get(ctx, created.ID)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(release) }) })
	var queries atomic.Int64
	barrierKeys := &keyFake{KeyRepo: keys, list: func(ctx context.Context, id int64) ([]store.ChannelKey, error) {
		query := queries.Add(1)
		rows, err := keys.ListByChannel(ctx, id)
		if err != nil {
			return nil, err
		}
		if query == 1 {
			close(entered) // actual SELECT completed with old 1:1 ciphertext rows
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return rows, nil
	}}
	p, err := NewEndpointPicker(channels, barrierKeys, service.masterKey)
	require.NoError(t, err)
	first := endpointAsyncPick(p, ctx, ch)
	endpointAwait(t, entered)
	_, err = service.UpdateKey(ctx, created.ID, created.Keys[0].ID, 3, 1)
	require.NoError(t, err) // management COMMIT precedes starting the next Pick
	waitCtx := &endpointGateContext{Context: ctx, entered: make(chan struct{})}
	second := endpointAsyncPick(p, waitCtx, ch)
	endpointAwait(t, waitCtx.entered)
	assert.Equal(t, int64(1), queries.Load(), "queued Pick has not read a stale result outside the gate")
	unblock.Do(func() { close(release) })
	for _, result := range []endpointPickResult{endpointAwait(t, first), endpointAwait(t, second)} {
		require.NoError(t, result.err)
		assert.Equal(t, "A", result.label)
	}
	assert.Equal(t, "A", pickEndpointLabel(t, p, ch))
	assert.Equal(t, "B", pickEndpointLabel(t, p, ch), "current 3:1 snapshot continues rather than reinstalling 1:1")
	assert.Equal(t, "A", pickEndpointLabel(t, p, ch))
	assert.Equal(t, int64(5), queries.Load())
}
