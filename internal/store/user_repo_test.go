package store

import (
	"context"
	"strings"
	"testing"
	"time"

	mysqlerr "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestUserRepoReadsAndNotFound(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewUserRepo(db)
	ctx := context.Background()
	for _, status := range []int8{UserStatusEnabled, UserStatusDisabled} {
		user := User{Username: "user-" + string(rune('a'+status)), PasswordHash: "test-password-digest",
			Role: RoleAdmin, GroupID: 7, Status: status, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
		require.NoError(t, db.Create(&user).Error)
		byID, err := repo.Get(ctx, int64(user.ID))
		require.NoError(t, err)
		byName, err := repo.FindByUsername(ctx, user.Username)
		require.NoError(t, err)
		for _, got := range []*User{byID, byName} {
			assert.Equal(t, user.ID, got.ID)
			assert.Equal(t, user.Username, got.Username)
			assert.Equal(t, user.PasswordHash, got.PasswordHash)
			assert.Equal(t, user.Role, got.Role)
			assert.Equal(t, user.GroupID, got.GroupID)
			assert.Equal(t, status, got.Status, "repo must not hide disabled users")
		}
	}
	got, err := repo.Get(ctx, 999999)
	assert.Nil(t, got)
	assert.ErrorIs(t, err, ErrNotFound)
	got, err = repo.FindByUsername(ctx, "missing-user")
	assert.Nil(t, got)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestUserRepoInvalidParametersBeforeSQL(t *testing.T) {
	repo := NewUserRepo(nil)
	for _, id := range []int64{0, -1} {
		_, err := repo.Get(context.Background(), id)
		require.Error(t, err)
	}
	for _, name := range []string{"", strings.Repeat("用", 65), string([]byte{0xff})} {
		_, err := repo.FindByUsername(context.Background(), name)
		require.Error(t, err)
	}
}

func TestIdentityReposCancellationAndSQLFailures(t *testing.T) {
	db := migratedTestDB(t)
	keys, users := NewAPIKeyRepo(db), NewUserRepo(db)
	operations := []struct {
		name string
		run  func(context.Context) error
	}{
		{"find prefix", func(ctx context.Context) error { _, err := keys.FindByPrefix(ctx, "sk-ABCDE"); return err }},
		{"list owner", func(ctx context.Context) error { _, err := keys.ListByUser(ctx, 1); return err }},
		{"create", func(ctx context.Context) error { k := testAPIKey(1, 1); return keys.Create(ctx, &k) }},
		{"revoke", func(ctx context.Context) error { return keys.RevokeOwned(ctx, 1, 1) }},
		{"get user", func(ctx context.Context) error { _, err := users.Get(ctx, 1); return err }},
		{"find username", func(ctx context.Context) error { _, err := users.FindByUsername(ctx, "test-user"); return err }},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, op := range operations {
		t.Run("canceled/"+op.name, func(t *testing.T) {
			err := op.run(ctx)
			assert.ErrorIs(t, err, context.Canceled)
			assert.NotErrorIs(t, err, ErrNotFound)
		})
	}
	var count int64
	require.NoError(t, db.Model(&APIKey{}).Count(&count).Error)
	assert.Zero(t, count, "canceled Create must not persist")
	// Real SQL errors must retain driver causes and must not masquerade as not-found.
	require.NoError(t, db.Exec("DROP TABLE api_keys, users").Error)
	for _, op := range operations {
		t.Run("SQL failure/"+op.name, func(t *testing.T) {
			err := op.run(context.Background())
			var driverErr *mysqlerr.MySQLError
			require.ErrorAs(t, err, &driverErr)
			assert.Equal(t, uint16(1146), driverErr.Number)
			assert.NotErrorIs(t, err, ErrNotFound)
		})
	}
}

func TestAPIKeyRepoRevokeExistenceFailure(t *testing.T) {
	db := migratedTestDB(t)
	repo := NewAPIKeyRepo(db)
	key := testAPIKey(1, 1)
	require.NoError(t, repo.Create(context.Background(), &key))
	require.NoError(t, repo.RevokeOwned(context.Background(), 1, int64(key.ID)))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cancel only after the real no-change SQL update to isolate fallback propagation.
	require.NoError(t, db.Callback().Update().After("gorm:commit_or_rollback_transaction").
		Register("test:cancel_after_update", func(tx *gorm.DB) {
			require.NoError(t, tx.Error)
			require.Zero(t, tx.RowsAffected)
			cancel()
		}))
	err := repo.RevokeOwned(ctx, 1, int64(key.ID))
	assert.ErrorIs(t, err, context.Canceled)
	assert.NotErrorIs(t, err, ErrNotFound)
}
