package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"gaigate/internal/access"
	"gaigate/internal/config"
	"gaigate/internal/store"
)

type commandUserStub struct {
	store.UserRepo
	find func(context.Context, string) (*store.User, error)
}

func (s commandUserStub) FindByUsername(ctx context.Context, name string) (*store.User, error) {
	return s.find(ctx, name)
}

type commandServiceStub struct {
	access.KeyService
	create func(context.Context, string) (*access.CreatedKey, error)
}

func (s commandServiceStub) Create(ctx context.Context, name string) (*access.CreatedKey, error) {
	return s.create(ctx, name)
}

type failedOutput struct{ short bool }

func (w failedOutput) Write([]byte) (int, error) {
	if w.short {
		return 0, nil
	}
	return 0, errors.New("private-output-cause")
}

func TestRunValidationBeforeOpening(t *testing.T) {
	for _, args := range [][]string{
		{}, {"-name", "x"}, {"-user", ""}, {"-user", strings.Repeat("用", 65)},
		{"-user", "u", "-name", strings.Repeat("😀", 129)}, {"-user", "u", "-name", string([]byte{0xff})},
		{"-user", "u", "extra"}, {"-private-password", "secret"},
	} {
		var out, errOut bytes.Buffer
		code := run(context.Background(), args, &out, &errOut, commandDeps{})
		assert.Equal(t, 1, code)
		assert.Empty(t, out.String())
		assert.NotEmpty(t, errOut.String())
		assert.NotContains(t, errOut.String(), "secret")
		assert.NotContains(t, errOut.String(), "private-password")
	}
	var out, errOut bytes.Buffer
	assert.Zero(t, run(context.Background(), []string{"-h"}, &out, &errOut, commandDeps{}))
	assert.Empty(t, out.String())
	assert.Contains(t, errOut.String(), "用法")
}

func TestRunOneBudgetIdentityOutputAndCleanup(t *testing.T) {
	for _, stage := range []string{"success", "config", "open", "unknown", "disabled", "create", "output error", "short write", "close"} {
		t.Run(stage, func(t *testing.T) {
			var out, errOut bytes.Buffer
			var stdout io.Writer = &out
			if stage == "output error" {
				stdout = failedOutput{}
			}
			if stage == "short write" {
				stdout = failedOutput{short: true}
			}
			closed, writes, opens := 0, 0, 0
			var commandDeadline time.Time
			sensitive := errors.New("private-dsn private-password private-output-cause")
			deps := commandDeps{
				load: func(path string) (*config.Config, error) {
					assert.Equal(t, "input.yaml", path)
					if stage == "config" {
						return nil, sensitive
					}
					return &config.Config{MySQL: config.MySQLConfig{Host: "test-host", Port: 3307, User: "test-user", Password: "private-password", Database: "gaigate_test"}}, nil
				},
				open: func(ctx context.Context, opts store.Options) (*keyDatabase, error) {
					opens++
					var ok bool
					commandDeadline, ok = ctx.Deadline()
					assert.True(t, ok)
					assert.WithinDuration(t, time.Now().Add(commandTimeout), commandDeadline, time.Second)
					assert.Equal(t, "gaigate_test", opts.Database)
					assert.Equal(t, "private-password", opts.Password)
					if stage == "open" {
						return nil, sensitive
					}
					return &keyDatabase{
						users: commandUserStub{find: func(queryCtx context.Context, name string) (*store.User, error) {
							assert.Equal(t, "owner", name)
							deadline, _ := queryCtx.Deadline()
							assert.Equal(t, commandDeadline, deadline)
							if stage == "unknown" {
								return nil, store.ErrNotFound
							}
							u := &store.User{ID: 3, GroupID: 4, Username: "owner", Role: store.RoleUser, Status: store.UserStatusEnabled}
							if stage == "disabled" {
								u.Status = store.UserStatusDisabled
							}
							return u, nil
						}},
						service: commandServiceStub{create: func(createCtx context.Context, name string) (*access.CreatedKey, error) {
							writes++
							p, ok := access.PrincipalFromContext(createCtx)
							require.True(t, ok)
							assert.Equal(t, int64(3), p.UserID)
							assert.Zero(t, p.APIKeyID)
							assert.Equal(t, int64(4), p.GroupID)
							assert.Equal(t, "  钥匙  ", name)
							deadline, _ := createCtx.Deadline()
							assert.Equal(t, commandDeadline, deadline)
							if stage == "create" {
								return nil, sensitive
							}
							return &access.CreatedKey{ID: 1, Name: name, Key: "one-time-test-output", KeyPrefix: "prefix", CreatedAt: time.Now().UTC()}, nil
						}},
						close: func() error {
							closed++
							if stage == "close" {
								return sensitive
							}
							return nil
						},
					}, nil
				},
			}
			code := run(context.Background(), []string{"-config", "input.yaml", "-user", "owner", "-name", "  钥匙  "}, stdout, &errOut, deps)
			if stage == "success" {
				assert.Zero(t, code)
				assert.Empty(t, errOut.String())
			} else {
				assert.Equal(t, 1, code)
			}
			if opens > 0 && stage != "open" {
				assert.Equal(t, 1, closed)
			} else {
				assert.Zero(t, closed)
			}
			if stage == "output error" || stage == "short write" || stage == "close" {
				assert.Contains(t, errOut.String(), "可能已创建，请另行查询或吊销")
				assert.Equal(t, 1, writes)
			}
			for _, secret := range []string{"private-dsn", "private-password", "private-output-cause", "one-time-test-output"} {
				assert.NotContains(t, errOut.String(), secret)
			}
			if stage == "success" || stage == "close" {
				decoder := json.NewDecoder(&out)
				var key access.CreatedKey
				require.NoError(t, decoder.Decode(&key))
				assert.Equal(t, int64(1), key.ID)
				var extra any
				assert.ErrorIs(t, decoder.Decode(&extra), io.EOF)
			} else {
				assert.Empty(t, out.String())
			}
		})
	}
}

func TestRunParentCancellationDoesNotResetBudget(t *testing.T) {
	for _, stage := range []string{"already canceled", "during initialization", "user query", "create"} {
		t.Run(stage, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			if stage == "already canceled" {
				cancel()
			}
			deadline, _ := parent.Deadline()
			closed, writes := 0, 0
			deps := commandDeps{load: func(string) (*config.Config, error) { return &config.Config{}, nil }, open: func(ctx context.Context, _ store.Options) (*keyDatabase, error) {
				got, _ := ctx.Deadline()
				assert.Equal(t, deadline, got)
				if stage == "during initialization" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return &keyDatabase{users: commandUserStub{find: func(ctx context.Context, _ string) (*store.User, error) {
					if stage == "user query" {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					return &store.User{ID: 1, GroupID: 1, Status: 1}, nil
				}}, service: commandServiceStub{create: func(ctx context.Context, _ string) (*access.CreatedKey, error) {
					writes++
					<-ctx.Done()
					return nil, ctx.Err()
				}}, close: func() error { closed++; return nil }}, nil
			}}
			var out, errOut bytes.Buffer
			start := time.Now()
			code := run(parent, []string{"-user", "owner"}, &out, &errOut, deps)
			assert.Equal(t, 1, code)
			assert.Empty(t, out.String())
			assert.Less(t, time.Since(start), 2*time.Second)
			if stage == "create" {
				assert.Equal(t, 1, writes)
			} else {
				assert.Zero(t, writes)
			}
			if stage == "user query" || stage == "create" {
				assert.Equal(t, 1, closed)
			} else {
				assert.Zero(t, closed)
			}
		})
	}
}
