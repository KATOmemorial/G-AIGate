package access

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/store"
)

type managedKeysStub struct {
	store.APIKeyRepo
	create func(context.Context, *store.APIKey) error
	list   func(context.Context, int64) ([]store.APIKey, error)
	revoke func(context.Context, int64, int64) error
}

func (s managedKeysStub) Create(ctx context.Context, k *store.APIKey) error {
	return s.create(ctx, k)
}
func (s managedKeysStub) ListByUser(ctx context.Context, id int64) ([]store.APIKey, error) {
	return s.list(ctx, id)
}
func (s managedKeysStub) RevokeOwned(ctx context.Context, owner, id int64) error {
	return s.revoke(ctx, owner, id)
}

func localKeyContext() context.Context {
	return WithPrincipal(context.Background(), &Principal{UserID: 3, APIKeyID: 0})
}

func enabledKeyOwner() store.UserRepo {
	return userLookupStub{get: func(ctx context.Context, id int64) (*store.User, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return &store.User{ID: uint64(id), Status: store.UserStatusEnabled}, nil
	}}
}

func TestKeyServiceOwnerRecheckBeforeEveryOperation(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  context.Context
		user *store.User
		err  error
		want error
	}{
		{"missing principal", context.Background(), nil, nil, ErrUnauthorized},
		{"nil principal", WithPrincipal(context.Background(), nil), nil, nil, ErrUnauthorized},
		{"invalid owner", WithPrincipal(context.Background(), &Principal{UserID: -1}), nil, nil, ErrUnauthorized},
		{"disabled", localKeyContext(), &store.User{ID: 3, Status: store.UserStatusDisabled}, nil, ErrUserDisabled},
		{"missing user", localKeyContext(), nil, store.ErrNotFound, store.ErrNotFound},
		{"SQL failure", localKeyContext(), nil, io.ErrUnexpectedEOF, io.ErrUnexpectedEOF},
		{"nil user", localKeyContext(), nil, nil, nil},
		{"wrong user", localKeyContext(), &store.User{ID: 4, Status: store.UserStatusEnabled}, nil, nil},
		{"overflow user", localKeyContext(), &store.User{ID: math.MaxInt64 + 1}, nil, nil},
		{"invalid status", localKeyContext(), &store.User{ID: 3, Status: 7}, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reads, business, generated := 0, 0, 0
			svc := NewKeyService(managedKeysStub{
				create: func(context.Context, *store.APIKey) error { business++; return nil },
				list:   func(context.Context, int64) ([]store.APIKey, error) { business++; return nil, nil },
				revoke: func(context.Context, int64, int64) error { business++; return nil },
			}, userLookupStub{get: func(ctx context.Context, id int64) (*store.User, error) {
				reads++
				assert.Equal(t, int64(3), id)
				assert.Equal(t, tc.ctx, ctx)
				return tc.user, tc.err
			}}).(*keyService)
			svc.generate = func() (string, string, string, error) { generated++; return "", "", "", nil }
			created, err := svc.Create(tc.ctx, "test")
			assert.Nil(t, created)
			errs := []error{err}
			listed, err := svc.List(tc.ctx)
			assert.Nil(t, listed)
			errs = append(errs, err, svc.Revoke(tc.ctx, 1))
			for _, err := range errs {
				require.Error(t, err)
				if tc.want != nil {
					assert.ErrorIs(t, err, tc.want)
				}
				assert.NotErrorIs(t, err, errOwnedKeyGone, "missing owner must not become HTTP Key 404")
			}
			assert.Zero(t, business)
			assert.Zero(t, generated)
			p, ok := PrincipalFromContext(tc.ctx)
			if ok && p.UserID > 0 {
				assert.Equal(t, 3, reads)
			} else {
				assert.Zero(t, reads)
			}
		})
	}
}

func TestKeyServiceCreateAndFailures(t *testing.T) {
	for i, name := range []string{"", "  keep spaces  ", strings.Repeat("钥", 128), strings.Repeat("😀", 128)} {
		t.Run(fmt.Sprintf("valid/%d", i), func(t *testing.T) {
			var row store.APIKey
			now := time.Now().In(time.FixedZone("local", 8*60*60))
			svc := NewKeyService(managedKeysStub{create: func(ctx context.Context, k *store.APIKey) error {
				assert.Equal(t, uint64(3), k.UserID)
				assert.Zero(t, k.ID)
				assert.Nil(t, k.ExpiredAt)
				row = *k
				k.ID, k.CreatedAt = 12, now
				return nil
			}}, enabledKeyOwner())
			created, err := svc.Create(localKeyContext(), name)
			require.NoError(t, err)
			require.NotNil(t, created)
			assert.Equal(t, int64(12), created.ID)
			assert.Equal(t, name, created.Name)
			assert.Equal(t, name, row.Name)
			assert.True(t, validRawKey(created.Key))
			assert.Equal(t, created.Key[:8], row.KeyPrefix)
			assert.Equal(t, row.KeyPrefix, created.KeyPrefix)
			digest := sha256.Sum256([]byte(created.Key))
			assert.True(t, row.KeyHash == hex.EncodeToString(digest[:]), "stored digest must match raw")
			assert.Equal(t, time.UTC, created.CreatedAt.Location())
			assert.True(t, now.Equal(created.CreatedAt))
			data, err := json.Marshal(created)
			require.NoError(t, err)
			assertJSONFields(t, data, "id", "name", "key", "key_prefix", "created_at")
			assert.NotContains(t, string(data), row.KeyHash)
		})
	}
	for _, name := range []string{strings.Repeat("a", 129), strings.Repeat("钥", 129), string([]byte{0xff})} {
		svc := NewKeyService(managedKeysStub{}, enabledKeyOwner()).(*keyService)
		svc.generate = func() (string, string, string, error) {
			t.Fatal("invalid name consumed entropy")
			return "", "", "", nil
		}
		created, err := svc.Create(localKeyContext(), name)
		assert.Nil(t, created)
		assert.ErrorIs(t, err, errKeyInput)
	}
	for _, failure := range []string{"entropy", "repository"} {
		t.Run(failure, func(t *testing.T) {
			writes := 0
			svc := NewKeyService(managedKeysStub{create: func(_ context.Context, k *store.APIKey) error {
				writes++
				k.ID = 123 // Even a partially mutated repo result must not escape.
				return io.ErrUnexpectedEOF
			}}, enabledKeyOwner()).(*keyService)
			if failure == "entropy" {
				svc.generate = func() (string, string, string, error) { return generateKey(strings.NewReader("")) }
			}
			created, err := svc.Create(localKeyContext(), "")
			assert.Nil(t, created)
			require.Error(t, err)
			if failure == "repository" {
				assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
				assert.Equal(t, 1, writes)
			} else {
				assert.ErrorIs(t, err, io.EOF)
				assert.Zero(t, writes)
			}
		})
	}
}

func TestKeyServiceListWhitelistAndRevokeOwner(t *testing.T) {
	zone := time.FixedZone("local", 8*60*60)
	now := time.Now().In(zone)
	expires := now.Add(-time.Hour)
	rows := []store.APIKey{
		{ID: 1, UserID: 3, Name: "revoked", KeyPrefix: "sk-ABCDE", KeyHash: "private-hash", Status: 0, CreatedAt: now},
		{ID: 2, UserID: 3, Name: "expired", KeyPrefix: "sk-ABCDE", Status: 1, CreatedAt: now, ExpiredAt: &expires},
	}
	var listErr, revokeErr error
	revocations := 0
	svc := NewKeyService(managedKeysStub{
		list: func(ctx context.Context, id int64) ([]store.APIKey, error) {
			assert.Equal(t, int64(3), id)
			return rows, listErr
		},
		revoke: func(ctx context.Context, owner, id int64) error {
			revocations++
			assert.Equal(t, int64(3), owner)
			assert.Equal(t, int64(2), id)
			return revokeErr
		},
	}, enabledKeyOwner())
	views, err := svc.List(localKeyContext())
	require.NoError(t, err)
	require.Len(t, views, 2)
	assert.Equal(t, int64(1), views[0].ID)
	assert.Equal(t, int8(0), views[0].Status)
	assert.Nil(t, views[0].ExpiredAt)
	assert.Equal(t, time.UTC, views[0].CreatedAt.Location())
	assert.Equal(t, time.UTC, views[1].ExpiredAt.Location())
	assert.Equal(t, zone, expires.Location())
	data, err := json.Marshal(views)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "private-hash")
	for _, view := range views {
		data, err := json.Marshal(view)
		require.NoError(t, err)
		assertJSONFields(t, data, "id", "name", "key_prefix", "status", "created_at", "expired_at")
	}
	rows = nil
	views, err = svc.List(localKeyContext())
	require.NoError(t, err)
	assert.NotNil(t, views)
	assert.Empty(t, views)
	listErr = io.ErrUnexpectedEOF
	views, err = svc.List(localKeyContext())
	assert.Nil(t, views)
	assert.ErrorIs(t, err, listErr)
	for _, invalid := range []int64{0, -1} {
		assert.ErrorIs(t, svc.Revoke(localKeyContext(), invalid), errKeyInput)
	}
	assert.Zero(t, revocations)
	require.NoError(t, svc.Revoke(localKeyContext(), 2))
	revokeErr = store.ErrNotFound
	err = svc.Revoke(localKeyContext(), 2)
	assert.ErrorIs(t, err, errOwnedKeyGone)
	assert.ErrorIs(t, err, store.ErrNotFound)
	revokeErr = io.ErrUnexpectedEOF
	err = svc.Revoke(localKeyContext(), 2)
	assert.ErrorIs(t, err, revokeErr)
	assert.False(t, errors.Is(err, errOwnedKeyGone))
}

func TestKeyServiceCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(localKeyContext())
	cancel()
	svc := NewKeyService(managedKeysStub{}, enabledKeyOwner())
	created, err := svc.Create(ctx, "")
	assert.Nil(t, created)
	assert.ErrorIs(t, err, context.Canceled)
	views, err := svc.List(ctx)
	assert.Nil(t, views)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, svc.Revoke(ctx, 1), context.Canceled)
}

func assertJSONFields(t *testing.T, data []byte, fields ...string) {
	t.Helper()
	var object map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &object))
	got := make([]string, 0, len(object))
	for field := range object {
		got = append(got, field)
	}
	assert.ElementsMatch(t, fields, got)
}
