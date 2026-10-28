package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"gaigate/internal/access"
	"gaigate/internal/store"
)

func TestCommandIndependentTransactionCancellationKeepsOwnerAlive(t *testing.T) {
	fixture, opts := commandTestDB(t, true)
	user := commandTestUser(t, fixture)
	for _, deadline := range []bool{false, true} {
		name := "cause"
		if deadline {
			name = "earlier_deadline"
		}
		t.Run(name, func(t *testing.T) {
			proxy := newRollbackTransportProxy(t, opts)
			commandCtx, stop := context.WithTimeout(context.Background(), commandTimeout)
			defer stop()
			db, owner, err := openMySQLWithOwner(commandCtx, proxy.Options())
			require.NoError(t, err)
			pool, err := db.DB()
			require.NoError(t, err)
			defer pool.Close()
			physical := captureCommandConnection(t, commandCtx, pool)
			var before, after uint64
			require.NoError(t, db.Raw("SELECT CONNECTION_ID()").Scan(&before).Error)
			caller, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			ctx := caller
			if deadline {
				var end context.CancelFunc
				ctx, end = context.WithTimeout(caller, 400*time.Millisecond)
				defer end()
			}
			cause := errors.New("independent transaction canceled before commit")
			require.NoError(t, db.Callback().Create().Before("gorm:commit_or_rollback_transaction").Register("134:independent_transaction", func(tx *gorm.DB) {
				if tx.Statement.Table != "api_keys" || tx.Error != nil {
					return
				}
				owner.mu.Lock()
				physical.network.mu.Lock()
				defer owner.mu.Unlock()
				defer physical.network.mu.Unlock()
				if deadline {
					<-ctx.Done()
				} else {
					cancel(cause)
				}
				ownerWait(t, proxy.held, "independent caller genuine ROLLBACK")
			}))
			ctx = access.WithPrincipal(ctx, &access.Principal{UserID: int64(user.ID), GroupID: int64(user.GroupID), Username: user.Username, Role: user.Role})
			service := access.NewKeyService(store.NewAPIKeyRepo(db), store.NewUserRepo(db))
			done := make(chan struct{})
			var created *access.CreatedKey
			var createErr error
			go func() { defer close(done); created, createErr = service.Create(ctx, "independent transaction") }()
			defer func() {
				proxy.Release()
				stop()
				ownerWait(t, done, "independent service joined")
				_ = pool.Close()
				proxy.Close()
			}()
			ownerWait(t, proxy.held, "independent ROLLBACK transport held")
			ownerWait(t, done, "independent canceled service return")
			ownerWait(t, proxy.peerEOF, "only caller TCP interrupted")
			assert.Nil(t, created)
			require.Error(t, createErr)
			if deadline {
				assert.ErrorIs(t, createErr, context.DeadlineExceeded)
			} else {
				assert.ErrorIs(t, context.Cause(ctx), cause)
			}
			require.NoError(t, commandCtx.Err())
			owner.mu.Lock()
			assert.False(t, owner.closing)
			owner.mu.Unlock()
			require.NoError(t, db.WithContext(commandCtx).Raw("SELECT CONNECTION_ID()").Scan(&after).Error)
			assert.NotEqual(t, before, after)
			owner.mu.Lock()
			assert.Len(t, owner.callbacks, 1)
			owner.mu.Unlock()
			require.NoError(t, pool.Close())
			assertCommandOwnerClosed(t, owner)
			assertCommandNetworkUnregistered(t, owner)
			proxy.Close()
			var count int64
			require.NoError(t, fixture.Model(&store.APIKey{}).Count(&count).Error)
			assert.Zero(t, count)
		})
	}
}

func TestCommandLifecycleInterruptsIndependentCallerRollback(t *testing.T) {
	fixture, opts := commandTestDB(t, true)
	user := commandTestUser(t, fixture)
	for _, activeCause := range []bool{false, true} {
		name := "deadline"
		if activeCause {
			name = "cause"
		}
		t.Run(name, func(t *testing.T) {
			proxy := newRollbackTransportProxy(t, opts)
			parent, stop := context.WithTimeout(context.Background(), 400*time.Millisecond)
			defer stop()
			commandCtx, cancel := context.WithCancelCause(parent)
			defer cancel(nil)
			db, owner, err := openMySQLWithOwner(commandCtx, proxy.Options())
			require.NoError(t, err)
			pool, err := db.DB()
			require.NoError(t, err)
			defer pool.Close()
			caller := access.WithPrincipal(context.Background(), &access.Principal{UserID: int64(user.ID), GroupID: int64(user.GroupID), Username: user.Username, Role: user.Role})
			require.NoError(t, db.Callback().Create().Before("gorm:commit_or_rollback_transaction").Register("134:lifecycle_only", func(tx *gorm.DB) {
				if tx.Statement.Table != "api_keys" || tx.Error != nil {
					return
				}
				tx.AddError(errors.New("controlled rollback before commit"))
				_ = tx.Rollback()
			}))
			service := access.NewKeyService(store.NewAPIKeyRepo(db), store.NewUserRepo(db))
			done := make(chan struct{})
			var created *access.CreatedKey
			var createErr error
			go func() { defer close(done); created, createErr = service.Create(caller, "lifecycle cancellation") }()
			defer func() {
				proxy.Release()
				cancel(nil)
				ownerWait(t, done, "lifecycle service joined")
				_ = pool.Close()
				proxy.Close()
			}()
			ownerWait(t, proxy.held, "independent caller foreground ROLLBACK held")
			cause := errors.New("command lifetime canceled while caller still valid")
			if activeCause {
				cancel(cause)
			}
			<-commandCtx.Done()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("original command lifetime did not interrupt independent caller ROLLBACK")
			}
			proxy.Release()
			ownerWait(t, done, "lifecycle cancellation finished")
			require.NoError(t, pool.Close()) // also clean the intentional no-interrupt mutation
			ownerWait(t, proxy.peerEOF, "actual lifecycle TCP EOF")
			assert.Nil(t, created)
			require.Error(t, createErr)
			assert.NoError(t, caller.Err())
			if activeCause {
				assert.ErrorIs(t, context.Cause(commandCtx), cause)
			} else {
				assert.ErrorIs(t, commandCtx.Err(), context.DeadlineExceeded)
			}
			require.NoError(t, pool.Close())
			assertCommandOwnerClosed(t, owner)
			assertCommandNetworkUnregistered(t, owner)
			proxy.Close()
			var count int64
			require.NoError(t, fixture.Model(&store.APIKey{}).Count(&count).Error)
			assert.Zero(t, count)
		})
	}
}
