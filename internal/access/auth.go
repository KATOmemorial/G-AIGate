package access

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"gaigate/internal/store"
)

var (
	ErrUnauthorized = errors.New("unauthorized")
	ErrKeyRevoked   = errors.New("API Key revoked")
	ErrKeyExpired   = errors.New("API Key expired")
	ErrUserDisabled = errors.New("user disabled")
)

// Principal is the authenticated identity shared through standard context.
type Principal struct {
	UserID, APIKeyID, GroupID int64
	Username, Role            string
}

type Authenticator interface {
	Authenticate(ctx context.Context, rawKey string) (*Principal, error)
}

type authenticator struct {
	keys  store.APIKeyRepo
	users store.UserRepo
	now   func() time.Time
}

func NewAuthenticator(keys store.APIKeyRepo, users store.UserRepo) Authenticator {
	return &authenticator{keys: keys, users: users, now: time.Now}
}

func validRawKey(raw string) bool {
	if len(raw) != 46 || !strings.HasPrefix(raw, "sk-") ||
		strings.Trim(raw[3:], "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_") != "" {
		return false
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(raw[3:])
	return err == nil && len(decoded) == 32
}

func (a *authenticator) Authenticate(ctx context.Context, rawKey string) (*Principal, error) {
	if !validRawKey(rawKey) {
		return nil, ErrUnauthorized
	}
	keys, err := a.keys.FindByPrefix(ctx, rawKey[:8])
	if err != nil {
		return nil, fmt.Errorf("access: find API Key candidates: %w", err)
	}
	digest := sha256.Sum256([]byte(rawKey))
	hash := hex.EncodeToString(digest[:])
	var matched *store.APIKey
	for i := range keys {
		if subtle.ConstantTimeCompare([]byte(hash), []byte(keys[i].KeyHash)) == 1 {
			matched = &keys[i]
			break
		}
	}
	if matched == nil {
		return nil, ErrUnauthorized
	}
	switch matched.Status {
	case store.APIKeyStatusRevoked:
		return nil, ErrKeyRevoked
	case store.APIKeyStatusActive:
	default:
		return nil, fmt.Errorf("access: invalid API Key status")
	}
	if matched.ExpiredAt != nil && !a.now().UTC().Before(matched.ExpiredAt.UTC()) {
		return nil, ErrKeyExpired
	}
	if !validIdentityID(matched.ID) || !validIdentityID(matched.UserID) {
		return nil, fmt.Errorf("access: invalid API Key identity IDs")
	}
	user, err := a.users.Get(ctx, int64(matched.UserID))
	if err != nil {
		return nil, fmt.Errorf("access: load API Key owner: %w", err)
	}
	if user == nil || !validIdentityID(user.ID) || user.ID != matched.UserID {
		return nil, fmt.Errorf("access: invalid API Key owner identity")
	}
	switch user.Status {
	case store.UserStatusDisabled:
		return nil, ErrUserDisabled
	case store.UserStatusEnabled:
	default:
		return nil, fmt.Errorf("access: invalid user status")
	}
	if !validIdentityID(user.GroupID) {
		return nil, fmt.Errorf("access: invalid user group ID")
	}
	return &Principal{UserID: int64(user.ID), APIKeyID: int64(matched.ID), GroupID: int64(user.GroupID),
		Username: user.Username, Role: user.Role}, nil
}

func validIdentityID(id uint64) bool { return id > 0 && id <= math.MaxInt64 }
