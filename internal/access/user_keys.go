package access

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"gaigate/internal/store"
)

// CreatedKey contains the credential only for the successful creation output.
type CreatedKey struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Key       string    `json:"key"`
	KeyPrefix string    `json:"key_prefix"`
	CreatedAt time.Time `json:"created_at"`
}

// KeyView is the public metadata whitelist; it cannot carry a credential hash.
type KeyView struct {
	ID        int64      `json:"id"`
	Name      string     `json:"name"`
	KeyPrefix string     `json:"key_prefix"`
	Status    int8       `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiredAt *time.Time `json:"expired_at"`
}

type KeyService interface {
	Create(ctx context.Context, name string) (*CreatedKey, error)
	List(ctx context.Context) ([]KeyView, error)
	Revoke(ctx context.Context, keyID int64) error
}

var (
	errKeyInput     = errors.New("invalid user Key input")
	errOwnedKeyGone = errors.New("owned Key not found")
)

type keyService struct {
	keys     store.APIKeyRepo
	users    store.UserRepo
	generate func() (raw, prefix, hash string, err error)
}

func NewKeyService(keys store.APIKeyRepo, users store.UserRepo) KeyService {
	return &keyService{keys: keys, users: users, generate: GenerateKey}
}

func validKeyName(name string) bool {
	return utf8.ValidString(name) && utf8.RuneCountInString(name) <= 128
}

func (s *keyService) owner(ctx context.Context) (int64, error) {
	p, ok := PrincipalFromContext(ctx)
	if !ok || p.UserID <= 0 {
		return 0, ErrUnauthorized
	}
	// APIKeyID=0 is the local CLI marker, not an HTTP authentication bypass.
	if s.users == nil || s.keys == nil {
		return 0, fmt.Errorf("access: missing user Key repositories")
	}
	u, err := s.users.Get(ctx, p.UserID)
	if err != nil {
		return 0, fmt.Errorf("access: recheck Key owner: %w", errors.Join(err, ctx.Err()))
	}
	if u == nil || !validIdentityID(u.ID) || int64(u.ID) != p.UserID {
		return 0, fmt.Errorf("access: invalid Key owner identity")
	}
	switch u.Status {
	case store.UserStatusDisabled:
		return 0, ErrUserDisabled
	case store.UserStatusEnabled:
		return p.UserID, nil
	default:
		return 0, fmt.Errorf("access: invalid Key owner status")
	}
}

func (s *keyService) Create(ctx context.Context, name string) (*CreatedKey, error) {
	owner, err := s.owner(ctx)
	if err != nil {
		return nil, err
	}
	if !validKeyName(name) {
		return nil, errKeyInput
	}
	raw, prefix, hash, err := s.generate()
	if err != nil {
		return nil, fmt.Errorf("access: create user Key entropy: %w", err)
	}
	k := store.APIKey{UserID: uint64(owner), Name: name, KeyPrefix: prefix, KeyHash: hash}
	if err := s.keys.Create(ctx, &k); err != nil {
		// GORM may retain only a rollback cause after cancellation. Keep both
		// the repository cause and the independently observable context cause.
		return nil, fmt.Errorf("access: create user Key: %w", errors.Join(err, ctx.Err()))
	}
	if !validIdentityID(k.ID) {
		return nil, fmt.Errorf("access: invalid created Key ID")
	}
	return &CreatedKey{ID: int64(k.ID), Name: k.Name, Key: raw, KeyPrefix: k.KeyPrefix, CreatedAt: k.CreatedAt.UTC()}, nil
}

func (s *keyService) List(ctx context.Context) ([]KeyView, error) {
	owner, err := s.owner(ctx)
	if err != nil {
		return nil, err
	}
	keys, err := s.keys.ListByUser(ctx, owner)
	if err != nil {
		return nil, fmt.Errorf("access: list user Keys: %w", errors.Join(err, ctx.Err()))
	}
	views := make([]KeyView, 0, len(keys))
	for _, k := range keys {
		if !validIdentityID(k.ID) || k.UserID != uint64(owner) {
			return nil, fmt.Errorf("access: invalid listed Key identity")
		}
		v := KeyView{ID: int64(k.ID), Name: k.Name, KeyPrefix: k.KeyPrefix, Status: k.Status, CreatedAt: k.CreatedAt.UTC()}
		if k.ExpiredAt != nil {
			expiry := k.ExpiredAt.UTC()
			v.ExpiredAt = &expiry
		}
		views = append(views, v)
	}
	return views, nil
}

func (s *keyService) Revoke(ctx context.Context, keyID int64) error {
	owner, err := s.owner(ctx)
	if err != nil {
		return err
	}
	if keyID <= 0 {
		return errKeyInput
	}
	if err := s.keys.RevokeOwned(ctx, owner, keyID); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("access: revoke user Key: %w: %w", errOwnedKeyGone, err)
		}
		return fmt.Errorf("access: revoke user Key: %w", errors.Join(err, ctx.Err()))
	}
	return nil
}
