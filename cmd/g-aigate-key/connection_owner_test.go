package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ownerTestConnector struct {
	connect func(context.Context) (driver.Conn, error)
	closing chan struct{}
}

func (c *ownerTestConnector) Connect(ctx context.Context) (driver.Conn, error) { return c.connect(ctx) }
func (*ownerTestConnector) Driver() driver.Driver                              { return unusedSQLDriver{} }
func (c *ownerTestConnector) Close() error {
	if c.closing != nil {
		close(c.closing)
	}
	return nil
}

type ownerTestConnection struct {
	*initializationConnection
	rollbackEntered, rollbackRelease, closeEntered, closeRelease chan struct{}
	rollbackErr, closeErr                                        error
	closed                                                       atomic.Bool
	check                                                        func(string, context.Context)
}

func (c *ownerTestConnection) Close() error {
	if c.closeEntered != nil {
		close(c.closeEntered)
	}
	if c.closeRelease != nil {
		<-c.closeRelease
	}
	c.closed.Store(true)
	_ = c.initializationConnection.Close()
	return c.closeErr
}
func (c *ownerTestConnection) mark(op string, ctx context.Context) {
	if c.check != nil {
		c.check(op, ctx)
	}
}
func (c *ownerTestConnection) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	c.mark("BeginTx", ctx)
	return ownerTestTransaction{c}, ctx.Err()
}
func (c *ownerTestConnection) PrepareContext(ctx context.Context, _ string) (driver.Stmt, error) {
	c.mark("PrepareContext", ctx)
	return nil, ctx.Err()
}
func (c *ownerTestConnection) Exec(_ string, _ []driver.Value) (driver.Result, error) {
	c.mark("Exec", nil)
	return driver.RowsAffected(1), nil
}
func (c *ownerTestConnection) Query(_ string, _ []driver.Value) (driver.Rows, error) {
	c.mark("Query", nil)
	return &versionRows{}, nil
}
func (c *ownerTestConnection) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	c.mark("ExecContext", ctx)
	return driver.RowsAffected(1), ctx.Err()
}
func (c *ownerTestConnection) ResetSession(ctx context.Context) error {
	c.mark("ResetSession", ctx)
	return ctx.Err()
}
func (c *ownerTestConnection) IsValid() bool { c.mark("IsValid", nil); return true }
func (c *ownerTestConnection) CheckNamedValue(v *driver.NamedValue) error {
	c.mark("CheckNamedValue", nil)
	v.Value = int64(9)
	return nil
}

type ownerTestTransaction struct{ c *ownerTestConnection }

func (ownerTestTransaction) Commit() error { return nil }
func (tx ownerTestTransaction) Rollback() error {
	if tx.c.rollbackEntered != nil {
		close(tx.c.rollbackEntered)
	}
	if tx.c.rollbackRelease != nil {
		<-tx.c.rollbackRelease
	}
	return tx.c.rollbackErr
}

func ownerTestBase() *ownerTestConnection {
	return &ownerTestConnection{initializationConnection: &initializationConnection{owner: &initializationConnector{version: "8.4.11"}}}
}
func ownerWait(t *testing.T, ch <-chan struct{}, stage string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for %s", stage)
	}
}
func ownerMustBlock(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("close returned while actual resource owner was still held")
	case <-time.After(50 * time.Millisecond):
	}
}
func assertCommandOwnerClosed(t *testing.T, c *commandConnector) {
	t.Helper()
	require.NotNil(t, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	assert.True(t, c.closing)
	assert.True(t, c.closed)
	assert.Empty(t, c.connecting)
	assert.Zero(t, c.connections)
	assert.Zero(t, c.networks)
	assert.Equal(t, c.dialed, c.networkDone)
	if c.networkName != "" {
		assert.Positive(t, c.dialed)
	}
	assert.Positive(t, c.acquired)
	assert.Equal(t, c.acquired, c.released)
}

func TestCommandPoolCloseJoinsBothTransactionOwners(t *testing.T) {
	for _, backgroundWins := range []bool{false, true} {
		name := "foreground_close"
		if backgroundWins {
			name = "background_close"
		}
		t.Run(name, func(t *testing.T) {
			conn := ownerTestBase()
			conn.rollbackEntered = make(chan struct{})
			conn.rollbackRelease = make(chan struct{})
			conn.closeEntered = make(chan struct{})
			conn.closeRelease = make(chan struct{})
			if backgroundWins {
				conn.rollbackErr = driver.ErrBadConn
			}
			underlying := &ownerTestConnector{connect: func(context.Context) (driver.Conn, error) { return conn, nil }, closing: make(chan struct{})}
			owner := newCommandConnector(underlying)
			pool := sql.OpenDB(owner)
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			tx, err := pool.BeginTx(ctx, nil)
			require.NoError(t, err)
			var rollbackOnce, closeOnce sync.Once
			releaseRollback := func() { rollbackOnce.Do(func() { close(conn.rollbackRelease) }) }
			releaseClose := func() { closeOnce.Do(func() { close(conn.closeRelease) }) }
			done := make(chan struct{})
			var closeErr error
			defer func() {
				releaseRollback()
				releaseClose()
				_ = pool.Close()
				ownerWait(t, done, "foreground owner cleanup")
				owner.owners.Wait() // also join actual resources on a negative-control failure
			}()
			cause := errors.New("controlled command cancellation")
			cancel(cause)
			ownerWait(t, conn.rollbackEntered, "background rollback ownership")
			require.ErrorIs(t, tx.Rollback(), sql.ErrTxDone)
			if backgroundWins {
				releaseRollback()
				ownerWait(t, conn.closeEntered, "background actual Close")
			}
			go func() { defer close(done); closeErr = pool.Close() }()
			ownerWait(t, underlying.closing, "sealed connector cleanup")
			assert.Error(t, pool.PingContext(context.Background()))
			ownerMustBlock(t, done)
			if !backgroundWins {
				releaseRollback()
				ownerWait(t, conn.closeEntered, "foreground actual Close")
				ownerMustBlock(t, done)
			}
			assert.False(t, conn.closed.Load())
			releaseClose()
			ownerWait(t, done, "actual connection Close completion")
			assert.NoError(t, closeErr)
			assert.True(t, conn.closed.Load())
			assertCommandOwnerClosed(t, owner)
			assert.ErrorIs(t, context.Cause(ctx), cause)
		})
	}
}

func TestCommandConnectionAndConnectorConcurrentCloseJoin(t *testing.T) {
	conn := ownerTestBase()
	conn.closeEntered = make(chan struct{})
	conn.closeRelease = make(chan struct{})
	closeCause := errors.New("controlled physical close failure")
	conn.closeErr = closeCause
	base := &ownerTestConnector{connect: func(context.Context) (driver.Conn, error) { return conn, nil }, closing: make(chan struct{})}
	owner := newCommandConnector(base)
	wrapped, err := owner.Connect(context.Background())
	require.NoError(t, err)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(conn.closeRelease) }) }
	var wg sync.WaitGroup
	results := make(chan error, 4)
	done := make(chan struct{})
	defer func() { release(); wg.Wait() }()
	wg.Add(1)
	go func() { defer wg.Done(); results <- wrapped.Close() }()
	ownerWait(t, conn.closeEntered, "physical Close")
	wg.Add(3)
	go func() { defer wg.Done(); results <- wrapped.Close() }()
	go func() { defer wg.Done(); results <- owner.Close() }()
	go func() { defer wg.Done(); results <- owner.Close() }()
	ownerWait(t, base.closing, "connector Close entry")
	go func() { wg.Wait(); close(done) }()
	ownerMustBlock(t, done)
	release()
	ownerWait(t, done, "all concurrent close owners")
	for i := 0; i < 4; i++ {
		assert.ErrorIs(t, <-results, closeCause)
	}
	assert.Equal(t, int64(1), conn.owner.closed.Load())
	assertCommandOwnerClosed(t, owner)
}

func TestCommandConnectorSealsAndJoinsLateConnect(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		name := "canceled_connect"
		if lateSuccess {
			name = "late_success"
		}
		t.Run(name, func(t *testing.T) {
			entered, observedCancel, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			conn := ownerTestBase()
			var calls atomic.Int64
			var observedCause error
			base := &ownerTestConnector{closing: make(chan struct{}), connect: func(ctx context.Context) (driver.Conn, error) {
				calls.Add(1)
				close(entered)
				<-ctx.Done()
				observedCause = context.Cause(ctx)
				close(observedCancel)
				<-release
				if lateSuccess {
					return conn, nil
				}
				return nil, ctx.Err()
			}}
			owner := newCommandConnector(base)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			connectDone, closeDone := make(chan struct{}), make(chan struct{})
			var actual driver.Conn
			var connectErr, closeErr error
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() {
				unblock()
				ownerWait(t, connectDone, "Connect cleanup")
				ownerWait(t, closeDone, "Close cleanup")
			}()
			go func() { defer close(connectDone); actual, connectErr = owner.Connect(ctx) }()
			ownerWait(t, entered, "Connect entered")
			go func() { defer close(closeDone); closeErr = owner.Close() }()
			ownerWait(t, base.closing, "connector sealed")
			ownerWait(t, observedCancel, "in-flight Connect cancellation")
			assert.ErrorIs(t, observedCause, errCommandPoolClosed)
			assert.NoError(t, ctx.Err())
			ownerMustBlock(t, closeDone)
			_, err := owner.Connect(ctx)
			assert.ErrorIs(t, err, errCommandPoolClosed)
			assert.Equal(t, int64(1), calls.Load())
			unblock()
			ownerWait(t, connectDone, "late Connect return")
			ownerWait(t, closeDone, "Close complete")
			assert.Nil(t, actual)
			assert.ErrorIs(t, connectErr, context.Canceled)
			assert.ErrorIs(t, connectErr, errCommandPoolClosed)
			assert.NoError(t, closeErr)
			assert.Equal(t, lateSuccess, conn.closed.Load())
			owner.mu.Lock()
			assert.Empty(t, owner.connecting)
			assert.Zero(t, owner.connections)
			assert.True(t, owner.closed)
			owner.mu.Unlock()
		})
	}
}

func TestCommandConnectorKeepsCallerContextAndCapabilities(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	conn := ownerTestBase()
	deadline, _ := ctx.Deadline()
	conn.owner.deadline = deadline
	base := &ownerTestConnector{connect: func(actual context.Context) (driver.Conn, error) {
		d, ok := actual.Deadline()
		assert.True(t, ok)
		assert.Equal(t, deadline, d)
		return conn, nil
	}}
	owner := newCommandConnector(base)
	wrapped, err := owner.Connect(ctx)
	require.NoError(t, err)
	defer owner.Close()
	defer wrapped.Close()
	capabilities, ok := wrapped.(commandMySQLCapabilities)
	require.True(t, ok)
	seen := map[string]bool{}
	cause := errors.New("independent caller cause")
	child, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	conn.check = func(op string, actual context.Context) {
		seen[op] = true
		if actual != nil {
			assert.Same(t, child, actual)
			assert.ErrorIs(t, context.Cause(actual), cause)
		}
	}
	_, err = capabilities.BeginTx(child, driver.TxOptions{})
	assert.ErrorIs(t, err, context.Canceled)
	_, err = capabilities.PrepareContext(child, "x")
	assert.ErrorIs(t, err, context.Canceled)
	_, err = capabilities.ExecContext(child, "x", nil)
	assert.ErrorIs(t, err, context.Canceled)
	_, err = capabilities.Exec("x", nil)
	assert.NoError(t, err)
	rows, err := capabilities.Query("x", nil)
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	assert.ErrorIs(t, capabilities.ResetSession(child), context.Canceled)
	assert.True(t, capabilities.IsValid())
	value := driver.NamedValue{Value: 1}
	require.NoError(t, capabilities.CheckNamedValue(&value))
	assert.Equal(t, int64(9), value.Value)
	for _, op := range []string{"BeginTx", "PrepareContext", "ExecContext", "Exec", "Query", "ResetSession", "IsValid", "CheckNamedValue"} {
		assert.True(t, seen[op], op)
	}
	rows, err = capabilities.QueryContext(ctx, "SELECT VERSION()", nil)
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	require.NoError(t, capabilities.Ping(ctx))
	assert.NoError(t, ctx.Err())
	_, err = owner.Connect(child)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, cause)
}

func TestCommandOwnedInitializationFailuresCloseActualResources(t *testing.T) {
	for _, stage := range []string{"connect", "version error", "version cancel", "ping error", "ping cancel", "already canceled"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			cause := errors.New("controlled initialization fault")
			init := &initializationConnector{version: "8.4.11", stage: stage, err: cause, deadline: deadline}
			base := &ownerTestConnector{connect: func(ctx context.Context) (driver.Conn, error) {
				c, err := init.Connect(ctx)
				if err != nil {
					return nil, err
				}
				return &ownerTestConnection{initializationConnection: c.(*initializationConnection)}, nil
			}}
			owner := newCommandConnector(base)
			pool := sql.OpenDB(owner)
			if stage == "already canceled" {
				cancel()
			}
			db, err := initializeMySQL(ctx, pool)
			assert.Nil(t, db)
			require.Error(t, err)
			switch stage {
			case "version cancel", "ping cancel":
				assert.ErrorIs(t, err, context.DeadlineExceeded)
			case "already canceled":
				assert.ErrorIs(t, err, context.Canceled)
			default:
				assert.ErrorIs(t, err, cause)
			}
			assert.Error(t, pool.PingContext(context.Background()))
			owner.mu.Lock()
			assert.True(t, owner.closed)
			assert.Zero(t, owner.connections)
			assert.Empty(t, owner.connecting)
			assert.Equal(t, owner.acquired, owner.released)
			owner.mu.Unlock()
		})
	}
}

func TestCommandConnectorCallerCancellationDuringConnect(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cause"
		if deadline {
			name = "earlier_deadline"
		}
		t.Run(name, func(t *testing.T) {
			cause := errors.New("independent connection cancellation")
			parent, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			ctx := parent
			if deadline {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(parent, 100*time.Millisecond)
				defer stop()
			}
			entered := make(chan struct{})
			done := make(chan struct{})
			var actualDeadline time.Time
			base := &ownerTestConnector{connect: func(actual context.Context) (driver.Conn, error) {
				actualDeadline, _ = actual.Deadline()
				close(entered)
				<-actual.Done()
				return nil, actual.Err()
			}}
			owner := newCommandConnector(base)
			var err error
			go func() { defer close(done); _, err = owner.Connect(ctx) }()
			defer func() {
				cancel(nil)
				ownerWait(t, done, "caller canceled Connect cleanup")
				assert.NoError(t, owner.Close())
			}()
			ownerWait(t, entered, "caller Connect entered")
			if !deadline {
				cancel(cause)
			}
			ownerWait(t, done, "original caller cancellation")
			if deadline {
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				expected, _ := ctx.Deadline()
				assert.Equal(t, expected, actualDeadline)
			} else {
				assert.ErrorIs(t, err, context.Canceled)
				assert.ErrorIs(t, err, cause)
			}
		})
	}
}

func TestCommandConnectorRejectsMissingCapabilitiesAndClosesConnection(t *testing.T) {
	base := &initializationConnector{}
	owner := newCommandConnector(base)
	conn, err := owner.Connect(context.Background())
	assert.Nil(t, conn)
	require.ErrorContains(t, err, "capabilities unavailable")
	assert.Equal(t, int64(1), base.closed.Load())
	require.NoError(t, owner.Close())
	owner.mu.Lock()
	assert.Empty(t, owner.connecting)
	assert.Zero(t, owner.connections)
	assert.True(t, owner.closed)
	owner.mu.Unlock()
}
