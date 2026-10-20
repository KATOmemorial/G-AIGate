package store

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
)

// APIKeyRepo stores user credentials, distinct from channel KeyRepo.
// No method accepts or persists a complete raw credential.
type APIKeyRepo interface {
	FindByPrefix(ctx context.Context, prefix string) ([]APIKey, error)
	ListByUser(ctx context.Context, userID int64) ([]APIKey, error)
	Create(ctx context.Context, k *APIKey) error
	RevokeOwned(ctx context.Context, userID, keyID int64) error
}

type apiKeyRepo struct{ db *gorm.DB }

func NewAPIKeyRepo(db *gorm.DB) APIKeyRepo { return &apiKeyRepo{db: db} }

// FindByPrefix returns every candidate, including revoked and expired keys.
func (r *apiKeyRepo) FindByPrefix(ctx context.Context, prefix string) ([]APIKey, error) {
	if !validAPIKeyPrefix(prefix) {
		return nil, fmt.Errorf("store: invalid API Key prefix")
	}
	keys := make([]APIKey, 0)
	if err := r.db.WithContext(ctx).Where("key_prefix = ?", prefix).
		Order("id ASC").Find(&keys).Error; err != nil {
		return nil, fmt.Errorf("store: find API Keys by prefix: %w", err)
	}
	return keys, nil
}

func (r *apiKeyRepo) ListByUser(ctx context.Context, userID int64) ([]APIKey, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("store: list API Keys requires positive user ID")
	}
	keys := make([]APIKey, 0)
	if err := r.db.WithContext(ctx).Where("user_id = ?", userID).
		Order("id ASC").Find(&keys).Error; err != nil {
		return nil, fmt.Errorf("store: list user API Keys: %w", err)
	}
	return keys, nil
}

// Create explicitly sets the initial active state and UTC timestamps. On success
// the supplied model receives its generated ID; failed writes leave it unchanged.
func (r *apiKeyRepo) Create(ctx context.Context, k *APIKey) error {
	if k == nil || k.ID != 0 || k.UserID == 0 || k.UserID > math.MaxInt64 {
		return fmt.Errorf("store: create API Key requires new key and valid user ID")
	}
	if !utf8.ValidString(k.Name) || utf8.RuneCountInString(k.Name) > 128 {
		return fmt.Errorf("store: API Key name exceeds 128 characters or is invalid UTF-8")
	}
	if !validAPIKeyPrefix(k.KeyPrefix) || len(k.KeyHash) != 64 ||
		strings.Trim(k.KeyHash, "0123456789abcdef") != "" {
		return fmt.Errorf("store: invalid API Key prefix or SHA-256 fingerprint")
	}
	row := *k
	row.Status = APIKeyStatusActive
	if row.CreatedAt.IsZero() {
		row.CreatedAt = time.Now().UTC()
	} else {
		row.CreatedAt = row.CreatedAt.UTC()
	}
	if row.ExpiredAt != nil {
		expiry := row.ExpiredAt.UTC()
		row.ExpiredAt = &expiry
	}
	if err := r.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("store: create API Key: %w", err)
	}
	*k = row
	return nil
}

// RevokeOwned scopes both the update and the no-change existence check to owner.
func (r *apiKeyRepo) RevokeOwned(ctx context.Context, userID, keyID int64) error {
	if userID <= 0 || keyID <= 0 {
		return fmt.Errorf("store: revoke API Key requires positive owner and key IDs")
	}
	res := r.db.WithContext(ctx).Model(&APIKey{}).
		Where("user_id = ? AND id = ?", userID, keyID).Update("status", APIKeyStatusRevoked)
	if res.Error != nil {
		return fmt.Errorf("store: revoke owned API Key: %w", res.Error)
	}
	// MySQL reports changed rows by default. A repeated revocation changes none.
	if res.RowsAffected == 0 {
		var count int64
		if err := r.db.WithContext(ctx).Model(&APIKey{}).
			Where("user_id = ? AND id = ?", userID, keyID).Count(&count).Error; err != nil {
			return fmt.Errorf("store: check owned API Key existence: %w", err)
		}
		if count == 0 {
			return fmt.Errorf("store: revoke owned API Key: %w", ErrNotFound)
		}
	}
	return nil
}

func validAPIKeyPrefix(prefix string) bool {
	return len(prefix) == 8 && strings.HasPrefix(prefix, "sk-") &&
		strings.Trim(prefix[3:], "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_") == ""
}
