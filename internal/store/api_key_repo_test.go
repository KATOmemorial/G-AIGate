package store

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	mysqlerr "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func testAPIKey(userID uint64, fingerprint int) APIKey {
	return APIKey{UserID: userID, KeyPrefix: "sk-ABCDE", KeyHash: fmt.Sprintf("%064x", fingerprint)}
}

func TestAPIKeyRepoCreate(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewAPIKeyRepo(db)
	ctx := context.Background()
	key := testAPIKey(1, 1)
	key.Name = strings.Repeat("钥", 128)
	before := time.Now().UTC().Add(-time.Second)
	require.NoError(t, repo.Create(ctx, &key))
	assert.NotZero(t, key.ID)
	assert.Equal(t, APIKeyStatusActive, key.Status)
	assert.Equal(t, time.UTC, key.CreatedAt.Location())
	assert.WithinRange(t, key.CreatedAt, before, time.Now().UTC())
	assert.Nil(t, key.ExpiredAt)
	var stored APIKey
	require.NoError(t, db.First(&stored, key.ID).Error)
	assert.Equal(t, key.UserID, stored.UserID)
	assert.Equal(t, key.Name, stored.Name)
	assert.Equal(t, key.KeyPrefix, stored.KeyPrefix)
	assert.Equal(t, key.KeyHash, stored.KeyHash)
	assert.Equal(t, APIKeyStatusActive, stored.Status)
	assert.WithinDuration(t, key.CreatedAt, stored.CreatedAt, time.Millisecond)
	assert.Nil(t, stored.ExpiredAt)

	zone := time.FixedZone("test-local", 8*60*60)
	created := time.Date(2026, 10, 20, 12, 0, 0, 0, zone)
	expires := created.Add(time.Hour)
	timed := testAPIKey(1, 2)
	timed.CreatedAt, timed.ExpiredAt = created, &expires
	require.NoError(t, repo.Create(ctx, &timed))
	assert.Equal(t, time.UTC, timed.CreatedAt.Location())
	assert.Equal(t, time.UTC, timed.ExpiredAt.Location())
	assert.True(t, timed.CreatedAt.Equal(created))
	assert.True(t, timed.ExpiredAt.Equal(expires))
	assert.Equal(t, zone, expires.Location(), "must not mutate caller's expiry")
	var storedTimed APIKey
	require.NoError(t, db.First(&storedTimed, timed.ID).Error)
	assert.True(t, storedTimed.CreatedAt.Equal(created))
	require.NotNil(t, storedTimed.ExpiredAt)
	assert.True(t, storedTimed.ExpiredAt.Equal(expires))

	duplicate := testAPIKey(2, 1)
	original := duplicate
	err := repo.Create(ctx, &duplicate)
	var sqlErr *mysqlerr.MySQLError
	require.ErrorAs(t, err, &sqlErr)
	assert.Equal(t, uint16(1062), sqlErr.Number, "hash unique constraint must be enforced")
	assert.Equal(t, original, duplicate, "failed Create must not report a generated ID")
}

func TestAPIKeyRepoCandidatesAndOwnerLists(t *testing.T) {
	db := migratedTestDB(t)
	ctx := context.Background()
	repo := NewAPIKeyRepo(db)
	expired := time.Now().UTC().Add(-time.Hour)
	// Explicit IDs inserted out of order exercise SQL ordering rather than insert order.
	for _, tc := range []struct {
		id, owner uint64
		status    int8
		expiry    *time.Time
	}{
		{30, 1, APIKeyStatusActive, &expired},
		{10, 2, APIKeyStatusRevoked, nil},
		{20, 1, APIKeyStatusActive, nil},
	} {
		key := testAPIKey(tc.owner, int(tc.id))
		key.ID, key.Status, key.ExpiredAt = tc.id, tc.status, tc.expiry
		key.CreatedAt = time.Now().UTC()
		require.NoError(t, db.Create(&key).Error)
	}
	keys, err := repo.FindByPrefix(ctx, "sk-ABCDE")
	require.NoError(t, err)
	require.Len(t, keys, 3, "non-unique prefix must include all statuses and owners")
	assert.Equal(t, []uint64{10, 20, 30}, []uint64{keys[0].ID, keys[1].ID, keys[2].ID})
	assert.Equal(t, APIKeyStatusRevoked, keys[0].Status)
	assert.Equal(t, fmt.Sprintf("%064x", 20), keys[1].KeyHash, "later candidate retains full hash")
	require.NotNil(t, keys[2].ExpiredAt)
	assert.True(t, keys[2].ExpiredAt.Before(time.Now().UTC()))
	for _, tc := range []struct {
		owner int64
		ids   []uint64
	}{{1, []uint64{20, 30}}, {2, []uint64{10}}, {3, []uint64{}}} {
		owned, err := repo.ListByUser(ctx, tc.owner)
		require.NoError(t, err)
		require.NotNil(t, owned)
		ids := make([]uint64, 0, len(owned))
		for _, key := range owned {
			assert.Equal(t, uint64(tc.owner), key.UserID)
			ids = append(ids, key.ID)
		}
		assert.Equal(t, tc.ids, ids)
	}
	empty, err := repo.FindByPrefix(ctx, "sk-ZZZZZ")
	require.NoError(t, err)
	assert.NotNil(t, empty)
	assert.Empty(t, empty)
}

func TestAPIKeyRepoRevokeOwned(t *testing.T) {
	db := migratedTestDB(t)
	ctx := context.Background()
	repo := NewAPIKeyRepo(db)
	key := testAPIKey(1, 1)
	require.NoError(t, repo.Create(ctx, &key))
	keyID := int64(key.ID)
	assert.ErrorIs(t, repo.RevokeOwned(ctx, 2, keyID), ErrNotFound)
	var unchanged APIKey
	require.NoError(t, db.First(&unchanged, key.ID).Error)
	assert.Equal(t, APIKeyStatusActive, unchanged.Status, "foreign owner must not modify key")
	require.NoError(t, repo.RevokeOwned(ctx, 1, keyID))
	var revoked APIKey
	require.NoError(t, db.First(&revoked, key.ID).Error)
	assert.Equal(t, APIKeyStatusRevoked, revoked.Status)
	assert.Equal(t, unchanged.KeyHash, revoked.KeyHash)
	assert.Equal(t, unchanged.CreatedAt, revoked.CreatedAt)

	// Mechanism evidence: observe the real MySQL no-change update and fallback query.
	var affected int64 = -1
	var existenceChecks int
	require.NoError(t, db.Callback().Update().After("gorm:update").Register("test:revoke_rows", func(tx *gorm.DB) {
		affected = tx.RowsAffected
	}))
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:revoke_existence", func(tx *gorm.DB) {
		if tx.Statement.Table == "api_keys" {
			existenceChecks++
		}
	}))
	require.NoError(t, repo.RevokeOwned(ctx, 1, keyID))
	assert.Zero(t, affected, "repeated revoke must exercise changed-rows=0")
	assert.Equal(t, 1, existenceChecks, "no-change path checks existence under same owner")
	assert.ErrorIs(t, repo.RevokeOwned(ctx, 2, keyID), ErrNotFound)
	assert.ErrorIs(t, repo.RevokeOwned(ctx, 1, keyID+100), ErrNotFound)
	assert.ErrorIs(t, repo.RevokeOwned(ctx, 2, keyID+100), ErrNotFound)
}

func TestAPIKeyRepoConcurrentRevocations(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewAPIKeyRepo(db)
	ctx := context.Background()
	key := testAPIKey(1, 1)
	require.NoError(t, repo.Create(ctx, &key))
	const count = 8
	results := make(chan error, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- repo.RevokeOwned(ctx, 1, int64(key.ID))
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		require.NoError(t, err)
	}
	owned, err := repo.ListByUser(ctx, 1)
	require.NoError(t, err)
	require.Len(t, owned, 1)
	assert.Equal(t, APIKeyStatusRevoked, owned[0].Status)
}

func TestAPIKeyRepoInvalidParametersBeforeSQL(t *testing.T) {
	// A nil database would panic if any invalid argument reached GORM.
	repo := NewAPIKeyRepo(nil)
	ctx := context.Background()
	valid := testAPIKey(1, 1)
	for _, tc := range []struct {
		name string
		edit func(*APIKey)
	}{
		{"existing ID", func(k *APIKey) { k.ID = 1 }},
		{"zero owner", func(k *APIKey) { k.UserID = 0 }},
		{"overflow owner", func(k *APIKey) { k.UserID = math.MaxInt64 + 1 }},
		{"long name", func(k *APIKey) { k.Name = strings.Repeat("钥", 129) }},
		{"bad UTF8", func(k *APIKey) { k.Name = string([]byte{0xff}) }},
		{"prefix length", func(k *APIKey) { k.KeyPrefix = "sk-ABCDEFGH" }},
		{"prefix scheme", func(k *APIKey) { k.KeyPrefix = "xx-ABCDE" }},
		{"prefix alphabet", func(k *APIKey) { k.KeyPrefix = "sk-ABCD!" }},
		{"hash length", func(k *APIKey) { k.KeyHash = "abc" }},
		{"hash uppercase", func(k *APIKey) { k.KeyHash = strings.Repeat("A", 64) }},
		{"hash alphabet", func(k *APIKey) { k.KeyHash = strings.Repeat("g", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := valid
			tc.edit(&key)
			require.Error(t, repo.Create(ctx, &key))
		})
	}
	require.Error(t, repo.Create(ctx, nil))
	for _, prefix := range []string{"", "sk-ABC", "sk-ABCD!"} {
		_, err := repo.FindByPrefix(ctx, prefix)
		require.Error(t, err)
	}
	for _, id := range []int64{0, -1} {
		_, err := repo.ListByUser(ctx, id)
		require.Error(t, err)
		require.Error(t, repo.RevokeOwned(ctx, id, 1))
		require.Error(t, repo.RevokeOwned(ctx, 1, id))
	}
}
