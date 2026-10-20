package access

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/store"
)

type keyLookupStub struct {
	store.APIKeyRepo
	find func(context.Context, string) ([]store.APIKey, error)
}

func (s keyLookupStub) FindByPrefix(ctx context.Context, prefix string) ([]store.APIKey, error) {
	return s.find(ctx, prefix)
}

type userLookupStub struct {
	store.UserRepo
	get func(context.Context, int64) (*store.User, error)
}

func (s userLookupStub) Get(ctx context.Context, id int64) (*store.User, error) {
	return s.get(ctx, id)
}

func authFixture() (string, store.APIKey, store.User) {
	raw := "sk-" + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	digest := sha256.Sum256([]byte(raw))
	key := store.APIKey{ID: 2, UserID: 3, KeyPrefix: raw[:8], KeyHash: hex.EncodeToString(digest[:]), Status: store.APIKeyStatusActive}
	user := store.User{ID: 3, GroupID: 4, Username: "test-owner", Role: store.RoleAdmin, Status: store.UserStatusEnabled}
	return raw, key, user
}

func TestAuthenticatorRejectsMalformedWithoutRepositories(t *testing.T) {
	raw, _, _ := authFixture()
	for i, bad := range []string{"", "sk-", raw[:45], raw + "A", "xx-" + raw[3:], raw[:45] + "=",
		raw[:45] + "!", " " + raw[1:], raw[:10] + " " + raw[11:], raw[:45] + "B", strings.ToUpper(raw[:3]) + raw[3:]} {
		t.Run(fmt.Sprintf("case-%d", i), func(t *testing.T) {
			// Nil dependencies prove all malformed credentials stop before repository access.
			p, err := NewAuthenticator(nil, nil).Authenticate(context.Background(), bad)
			assert.Nil(t, p)
			assert.ErrorIs(t, err, ErrUnauthorized)
		})
	}
}

func TestAuthenticatorCollisionAndFullPrincipal(t *testing.T) {
	raw, key, user := authFixture()
	wrong := key
	wrong.ID, wrong.Status, wrong.KeyHash = 1, store.APIKeyStatusRevoked, strings.Repeat("0", 64)
	type marker struct{}
	ctx := context.WithValue(context.Background(), marker{}, "request")
	keyCalls, userCalls := 0, 0
	a := NewAuthenticator(keyLookupStub{find: func(got context.Context, prefix string) ([]store.APIKey, error) {
		keyCalls++
		assert.Equal(t, ctx, got)
		assert.Equal(t, raw[:8], prefix)
		return []store.APIKey{wrong, key}, nil
	}}, userLookupStub{get: func(got context.Context, id int64) (*store.User, error) {
		userCalls++
		assert.Equal(t, ctx, got)
		assert.Equal(t, int64(key.UserID), id)
		return &user, nil
	}})
	p, err := a.Authenticate(ctx, raw)
	require.NoError(t, err)
	assert.Equal(t, &Principal{UserID: 3, APIKeyID: 2, GroupID: 4, Username: user.Username, Role: user.Role}, p)
	assert.Equal(t, 1, keyCalls)
	assert.Equal(t, 1, userCalls)
}

func TestAuthenticatorOrderAndErrors(t *testing.T) {
	now := time.Date(2026, 10, 20, 9, 0, 0, 0, time.UTC)
	before, exact, after := now.Add(-time.Nanosecond), now, now.Add(time.Nanosecond)
	zone := time.FixedZone("expiry-local", 8*3600)
	localExact := now.In(zone)
	failure := errors.New("repository failure")
	for _, tc := range []struct {
		name      string
		change    func(*store.APIKey, *store.User)
		keyErr    error
		userErr   error
		empty     bool
		nilUser   bool
		wantErr   error
		internal  bool
		userCalls int
	}{
		{name: "permanent", userCalls: 1},
		{name: "before expiry", change: func(k *store.APIKey, _ *store.User) { k.ExpiredAt = &after }, userCalls: 1},
		{name: "exact expiry", change: func(k *store.APIKey, _ *store.User) { k.ExpiredAt = &exact }, wantErr: ErrKeyExpired},
		{name: "after expiry", change: func(k *store.APIKey, _ *store.User) { k.ExpiredAt = &before }, wantErr: ErrKeyExpired},
		{name: "UTC instant", change: func(k *store.APIKey, _ *store.User) { k.ExpiredAt = &localExact }, wantErr: ErrKeyExpired},
		{name: "revoked precedes expiry and user", change: func(k *store.APIKey, u *store.User) {
			k.Status, k.ExpiredAt, u.Status = store.APIKeyStatusRevoked, &before, store.UserStatusDisabled
		}, wantErr: ErrKeyRevoked},
		{name: "hash before state", change: func(k *store.APIKey, _ *store.User) {
			k.KeyHash, k.Status = strings.Repeat("0", 64), store.APIKeyStatusRevoked
		}, wantErr: ErrUnauthorized},
		{name: "no candidates", empty: true, wantErr: ErrUnauthorized},
		{name: "disabled", change: func(_ *store.APIKey, u *store.User) { u.Status = store.UserStatusDisabled }, wantErr: ErrUserDisabled, userCalls: 1},
		{name: "key SQL error", keyErr: failure, wantErr: failure, internal: true},
		{name: "user SQL error", userErr: failure, wantErr: failure, internal: true, userCalls: 1},
		{name: "orphaned key", userErr: fmt.Errorf("missing: %w", store.ErrNotFound), wantErr: store.ErrNotFound, internal: true, userCalls: 1},
		{name: "nil user", nilUser: true, internal: true, userCalls: 1},
		{name: "unknown key status", change: func(k *store.APIKey, _ *store.User) { k.Status = 8 }, internal: true},
		{name: "unknown user status", change: func(_ *store.APIKey, u *store.User) { u.Status = 8 }, internal: true, userCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, key, user := authFixture()
			if tc.change != nil {
				tc.change(&key, &user)
			}
			calls := 0
			a := NewAuthenticator(keyLookupStub{find: func(context.Context, string) ([]store.APIKey, error) {
				if tc.empty {
					return []store.APIKey{}, nil
				}
				return []store.APIKey{key}, tc.keyErr
			}}, userLookupStub{get: func(context.Context, int64) (*store.User, error) {
				calls++
				if tc.nilUser {
					return nil, nil
				}
				return &user, tc.userErr
			}}).(*authenticator)
			a.now = func() time.Time { return now }
			p, err := a.Authenticate(context.Background(), raw)
			assert.Equal(t, tc.userCalls, calls)
			if tc.wantErr == nil && !tc.internal {
				require.NoError(t, err)
				require.NotNil(t, p)
				return
			}
			require.Error(t, err)
			assert.Nil(t, p)
			if tc.wantErr != nil {
				assert.ErrorIs(t, err, tc.wantErr)
			}
			if tc.internal {
				for _, sentinel := range []error{ErrUnauthorized, ErrKeyRevoked, ErrKeyExpired, ErrUserDisabled} {
					assert.NotErrorIs(t, err, sentinel)
				}
			}
		})
	}
}

func TestAuthenticatorIdentityIDBounds(t *testing.T) {
	for _, field := range []string{"key", "owner", "user", "group"} {
		for _, invalid := range []uint64{0, math.MaxInt64 + 1} {
			t.Run(fmt.Sprintf("%s/%d", field, invalid), func(t *testing.T) {
				raw, key, user := authFixture()
				switch field {
				case "key":
					key.ID = invalid
				case "owner":
					key.UserID = invalid
				case "user":
					user.ID = invalid
				case "group":
					user.GroupID = invalid
				}
				userCalls := 0
				a := NewAuthenticator(keyLookupStub{find: func(context.Context, string) ([]store.APIKey, error) { return []store.APIKey{key}, nil }},
					userLookupStub{get: func(context.Context, int64) (*store.User, error) { userCalls++; return &user, nil }})
				p, err := a.Authenticate(context.Background(), raw)
				require.Error(t, err)
				assert.Nil(t, p)
				assert.NotErrorIs(t, err, ErrUnauthorized)
				if field == "key" || field == "owner" {
					assert.Zero(t, userCalls, "invalid model IDs must be checked before int64 conversion and lookup")
				} else {
					assert.Equal(t, 1, userCalls)
				}
			})
		}
	}
	raw, key, user := authFixture()
	key.ID, key.UserID, user.ID, user.GroupID = math.MaxInt64, math.MaxInt64, math.MaxInt64, math.MaxInt64
	a := NewAuthenticator(keyLookupStub{find: func(context.Context, string) ([]store.APIKey, error) { return []store.APIKey{key}, nil }},
		userLookupStub{get: func(_ context.Context, id int64) (*store.User, error) {
			assert.Equal(t, int64(math.MaxInt64), id)
			return &user, nil
		}})
	p, err := a.Authenticate(context.Background(), raw)
	require.NoError(t, err)
	assert.Equal(t, int64(math.MaxInt64), p.UserID)
	assert.Equal(t, int64(math.MaxInt64), p.APIKeyID)
	assert.Equal(t, int64(math.MaxInt64), p.GroupID)
	user.ID--
	_, err = a.Authenticate(context.Background(), raw)
	require.Error(t, err, "mismatched owner must not yield a principal")
}

func TestPrincipalContextSnapshotAndIsolation(t *testing.T) {
	base := context.Background()
	p, ok := PrincipalFromContext(base)
	assert.Nil(t, p)
	assert.False(t, ok)
	want := Principal{UserID: 1, APIKeyID: 2, GroupID: 3, Username: "owner", Role: "user"}
	input := want
	ctx := WithPrincipal(base, &input)
	input.UserID = 999
	p, ok = PrincipalFromContext(ctx)
	require.True(t, ok)
	assert.Equal(t, want, *p)
	p.UserID = 888
	p, _ = PrincipalFromContext(ctx)
	assert.Equal(t, want, *p, "getter must not expose mutable context identity")
	_, ok = PrincipalFromContext(base)
	assert.False(t, ok)
	p, ok = PrincipalFromContext(WithPrincipal(ctx, nil))
	assert.Nil(t, p)
	assert.False(t, ok)
}
