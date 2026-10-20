package store

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"gorm.io/gorm"
)

// UserRepo reads existing users. Callers must not serialize or log password data.
type UserRepo interface {
	Get(ctx context.Context, id int64) (*User, error)
	FindByUsername(ctx context.Context, username string) (*User, error)
}

type userRepo struct{ db *gorm.DB }

func NewUserRepo(db *gorm.DB) UserRepo { return &userRepo{db: db} }

func (r *userRepo) Get(ctx context.Context, id int64) (*User, error) {
	if id <= 0 {
		return nil, fmt.Errorf("store: get user requires positive ID")
	}
	return r.find(r.db.WithContext(ctx).Where("id = ?", id))
}

func (r *userRepo) FindByUsername(ctx context.Context, username string) (*User, error) {
	if username == "" || !utf8.ValidString(username) || utf8.RuneCountInString(username) > 64 {
		return nil, fmt.Errorf("store: invalid username")
	}
	return r.find(r.db.WithContext(ctx).Where("username = ?", username))
}

func (r *userRepo) find(query *gorm.DB) (*User, error) {
	var user User
	if err := query.First(&user).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("store: find user: %w", ErrNotFound)
		}
		return nil, fmt.Errorf("store: find user: %w", err)
	}
	return &user, nil
}
