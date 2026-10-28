package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	driver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"gaigate/internal/store"
)

// This pool is used ONLY while GORM synchronously detects the server version
// using Background. It is removed before any caller can perform an operation.
// Thus no later caller context, transaction capability or cancellation is hidden.
type initializationPool struct {
	*sql.DB
	ctx context.Context
}

func (p initializationPool) QueryRowContext(_ context.Context, query string, args ...any) *sql.Row {
	return p.DB.QueryRowContext(p.ctx, query, args...)
}

func (p initializationPool) GetDBConn() (*sql.DB, error) { return p.DB, nil }

type silentDriverLogger struct{}

func (silentDriverLogger) Print(...any) {}

func openMySQL(ctx context.Context, opts store.Options) (*gorm.DB, error) {
	db, _, err := openMySQLWithOwner(ctx, opts)
	return db, err
}

func openMySQLWithOwner(ctx context.Context, opts store.Options) (*gorm.DB, *commandConnector, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("command: initialize MySQL: %w", errors.Join(err, context.Cause(ctx)))
	}
	cfg, err := driver.ParseDSN(opts.DSN())
	if err != nil {
		return nil, nil, fmt.Errorf("command: parse MySQL configuration: %w", err)
	}
	cfg.Logger = silentDriverLogger{}
	owner := newCommandConnector(nil)
	owner.registerTCP(cfg)
	connector, err := driver.NewConnector(cfg)
	if err != nil {
		closeErr := owner.Close()
		return nil, owner, fmt.Errorf("command: create MySQL connector: %w", errors.Join(err, closeErr))
	}
	owner.Connector = connector
	db, err := initializeMySQL(ctx, sql.OpenDB(owner))
	return db, owner, err
}

// Own the lazy pool from entry, including all version-query and ping failures.
// No goroutine, global driver override, new timeout or skipped detection is used.
func initializeMySQL(ctx context.Context, pool *sql.DB) (*gorm.DB, error) {
	failed := true
	defer func() {
		if failed {
			_ = pool.Close()
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("command: initialize MySQL: %w", errors.Join(err, context.Cause(ctx)))
	}
	dialector := mysql.New(mysql.Config{Conn: initializationPool{DB: pool, ctx: ctx}})
	db, err := gorm.Open(dialector, &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	if err == nil {
		err = pool.PingContext(ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("command: initialize MySQL: %w", errors.Join(err, ctx.Err(), context.Cause(ctx)))
	}
	// Restore BOTH handles. GORM sessions derive their pool from Statement.
	db.ConnPool = pool
	db.Statement.ConnPool = pool
	dialector.(*mysql.Dialector).Conn = pool
	failed = false
	return db.WithContext(ctx), nil
}
