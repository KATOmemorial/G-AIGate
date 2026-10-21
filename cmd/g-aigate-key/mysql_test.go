package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"

	"gaigate/internal/store"
)

type initializationConnector struct {
	version                                  string
	stage                                    string
	err                                      error
	deadline                                 time.Time
	connected, closed, versionQueries, pings atomic.Int64
}

func (c *initializationConnector) Driver() driver.Driver { return unusedSQLDriver{} }
func (c *initializationConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.connected.Add(1)
	if c.stage == "connect" {
		return nil, c.err
	}
	return &initializationConnection{owner: c}, nil
}

type unusedSQLDriver struct{}

func (unusedSQLDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("unexpected legacy Open")
}

type initializationConnection struct{ owner *initializationConnector }

func (c *initializationConnection) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unused prepare")
}
func (c *initializationConnection) Begin() (driver.Tx, error) {
	return nil, errors.New("unused transaction")
}
func (c *initializationConnection) Close() error { c.owner.closed.Add(1); return nil }
func (c *initializationConnection) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if query != "SELECT VERSION()" {
		return nil, errors.New("unexpected query")
	}
	c.owner.versionQueries.Add(1)
	if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(c.owner.deadline) {
		return nil, errors.New("version budget changed")
	}
	if c.owner.stage == "version cancel" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if c.owner.stage == "version error" {
		return nil, c.owner.err
	}
	return &versionRows{version: c.owner.version}, nil
}
func (c *initializationConnection) Ping(ctx context.Context) error {
	c.owner.pings.Add(1)
	if deadline, ok := ctx.Deadline(); !ok || !deadline.Equal(c.owner.deadline) {
		return errors.New("ping budget changed")
	}
	if c.owner.stage == "ping cancel" {
		<-ctx.Done()
		return ctx.Err()
	}
	if c.owner.stage == "ping error" {
		return c.owner.err
	}
	return ctx.Err()
}

type versionRows struct {
	version string
	read    bool
}

func (*versionRows) Columns() []string { return []string{"VERSION()"} }
func (*versionRows) Close() error      { return nil }
func (r *versionRows) Next(values []driver.Value) error {
	if r.read {
		return io.EOF
	}
	r.read = true
	values[0] = r.version
	return nil
}

func TestInitializeMySQLVersionsAndActualPool(t *testing.T) {
	for _, tc := range []struct {
		version                          string
		renameIndex, renameColumn, share bool
	}{
		{"8.4.11", false, false, false}, {"5.7.44", false, true, true}, {"5.6.51", true, true, true}, {"10.11.10-MariaDB", true, true, true},
	} {
		t.Run(tc.version, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			deadline, _ := ctx.Deadline()
			connector := &initializationConnector{version: tc.version, deadline: deadline}
			pool := sql.OpenDB(connector)
			db, err := initializeMySQL(ctx, pool)
			require.NoError(t, err)
			actual, err := db.DB()
			require.NoError(t, err)
			assert.Same(t, pool, actual)
			assert.Same(t, pool, db.ConnPool)
			assert.Same(t, pool, db.Statement.ConnPool)
			dialect := db.Dialector.(*mysql.Dialector)
			assert.Equal(t, tc.version, dialect.ServerVersion)
			assert.False(t, dialect.SkipInitializeWithVersion)
			assert.Equal(t, tc.renameIndex, dialect.DontSupportRenameIndex)
			assert.Equal(t, tc.renameColumn, dialect.DontSupportRenameColumn)
			assert.Equal(t, tc.share, dialect.DontSupportForShareClause)
			assert.Same(t, pool, dialect.Conn)
			assert.Equal(t, int64(1), connector.versionQueries.Load())
			assert.Equal(t, int64(1), connector.pings.Load())
			require.NoError(t, actual.Close())
			assert.Equal(t, int64(1), connector.closed.Load())
			assert.Error(t, pool.PingContext(ctx))
		})
	}
}

func TestInitializeMySQLFailuresClosePool(t *testing.T) {
	for _, stage := range []string{"connect", "version error", "version cancel", "ping error", "ping cancel", "already canceled"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			cause := errors.New("controlled initialization failure")
			connector := &initializationConnector{version: "8.4.11", stage: stage, err: cause, deadline: deadline}
			pool := sql.OpenDB(connector)
			if stage == "already canceled" {
				cancel()
			}
			start := time.Now()
			db, err := initializeMySQL(ctx, pool)
			assert.Nil(t, db)
			require.Error(t, err)
			switch stage {
			case "version cancel", "ping cancel":
				assert.ErrorIs(t, err, context.DeadlineExceeded)
				assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
			case "already canceled":
				assert.ErrorIs(t, err, context.Canceled)
				assert.Zero(t, connector.connected.Load())
			default:
				assert.ErrorIs(t, err, cause)
			}
			assert.Less(t, time.Since(start), 2*time.Second)
			assert.Zero(t, pool.Stats().OpenConnections)
			assert.Error(t, pool.PingContext(context.Background()), "failed initializer must close the pool")
			if stage != "connect" && stage != "already canceled" {
				assert.Equal(t, int64(1), connector.closed.Load())
			}
			if stage == "version error" || stage == "version cancel" {
				assert.Zero(t, connector.pings.Load())
			}
		})
	}
}

func TestOpenMySQLRejectsCanceledContextWithCause(t *testing.T) {
	cause := errors.New("independent cancellation")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	db, err := openMySQL(ctx, store.Options{Host: "127.0.0.1", Port: 1, Database: "gaigate_test"})
	assert.Nil(t, db)
	assert.ErrorIs(t, err, context.Canceled)
	assert.ErrorIs(t, err, cause)
}
