package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/golang-migrate/migrate/v4"
	mysqldriver "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file" // 注册 file:// 迁移源
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Options 是 MySQL 连接参数。main 从 config.MySQLConfig 装配传入；
// store 不 import internal/config——装配层依赖全部模块，反向依赖被
// 00 §5.1 禁止。
type Options struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

// DSN 返回应用侧（GORM）连接串。loc=UTC + parseTime：库内时间一律 UTC
// （00 §7.4），读取为 time.Time。
func (o Options) DSN() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=UTC",
		o.User, o.Password, o.Host, o.Port, o.Database)
}

// Open 打开 GORM MySQL 客户端（连接时 ping 失败即返回错误，fail-fast）。
// schema 由 migrations/ 管理，禁用 AutoMigrate（01-工程规范 §1）；
// GORM 日志设为 Silent，避免非 JSON 行污染 00 §8.3 的结构化日志（SQL
// 错误经返回值上抛，由调用方以 JSON 记录）。
func Open(opts Options) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(opts.DSN()), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, fmt.Errorf("store: 连接 MySQL: %w", err)
	}
	return db, nil
}

// 迁移并发控制（11.1 §3 / §7）：多副本同时启动时，GET_LOCK 串行化迁移；
// 拿不到锁的副本等待重试，总预算耗尽后 fail-fast 退出（依赖 DBA 干预），
// 不静默跳过迁移。
const (
	migrateLockName       = "gaigate_migrate"
	migrateLockTimeoutSec = 10              // 单次 GET_LOCK 服务器侧等待秒数
	migrateLockBudget     = 60 * time.Second // 拿锁总预算（含重试），超限 fail-fast
	migrateRetryDelay     = 2 * time.Second
)

// RunMigrations 启动时自动执行 migrations 目录下的 up 迁移（11.1 §3）。
//
// 执行序列：专用连接上 GET_LOCK('gaigate_migrate', 10)（重试至总预算）→
// migrate.Up → RELEASE_LOCK。GET_LOCK 是会话级锁，故在从连接池取出的
// 专用 *sql.Conn 上获取并持有，迁移本身经池内其他连接执行；迁移用
// *sql.DB 需 multiStatements=true（迁移文件含多条 DDL）。
// 生产可改为 K8s Job 执行（11.1 §3 已注明），语义不变。
func RunMigrations(opts Options, migrationsDir string) error {
	db, err := sql.Open("mysql", opts.DSN()+"&multiStatements=true")
	if err != nil {
		return fmt.Errorf("store: 打开迁移连接: %w", err)
	}
	defer func() { _ = db.Close() }() // 关闭失败无补救动作，忽略

	// 启动阶段，无请求 context；等待由 GET_LOCK 服务器侧超时与拿锁总预算约束
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("store: 获取迁移专用连接: %w", err)
	}
	defer func() { _ = conn.Close() }() // 会话锁随连接关闭自动释放

	if err := acquireMigrateLock(ctx, conn); err != nil {
		return err
	}
	defer func() { releaseMigrateLock(context.Background(), conn) }()

	m, err := newMigrator(db, migrationsDir)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: 执行迁移: %w", err)
	}
	return nil
}

// newMigrator 构造 golang-migrate 实例：SQL 文件源 + MySQL 目标库。
func newMigrator(db *sql.DB, migrationsDir string) (*migrate.Migrate, error) {
	drv, err := mysqldriver.WithInstance(db, &mysqldriver.Config{})
	if err != nil {
		return nil, fmt.Errorf("store: 初始化 migrate 驱动: %w", err)
	}
	m, err := migrate.NewWithDatabaseInstance(
		"file://"+migrationsDir, "mysql", drv)
	if err != nil {
		return nil, fmt.Errorf("store: 初始化 migrate: %w", err)
	}
	return m, nil
}

// acquireMigrateLock 获取迁移锁：拿不到则等待重试，超过总预算 fail-fast
// （11.1 §7：不静默跳过迁移）。
func acquireMigrateLock(ctx context.Context, conn *sql.Conn) error {
	deadline := time.Now().Add(migrateLockBudget)
	for {
		// GET_LOCK 返回 1=获得 0=超时 NULL=出错（Scan 报错路径）
		var got int
		if err := conn.QueryRowContext(ctx,
			"SELECT GET_LOCK(?, ?)", migrateLockName, migrateLockTimeoutSec,
		).Scan(&got); err != nil {
			return fmt.Errorf("store: GET_LOCK(%q) 执行失败: %w", migrateLockName, err)
		}
		if got == 1 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf(
				"store: 迁移锁 %q 等待超过 %s（其他副本迁移中？），fail-fast 退出，需人工介入",
				migrateLockName, migrateLockBudget)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(migrateRetryDelay):
		}
	}
}

// releaseMigrateLock 释放迁移锁。失败可忽略：专用连接随后关闭，
// GET_LOCK 的会话级锁随会话终结自动释放。
func releaseMigrateLock(ctx context.Context, conn *sql.Conn) {
	_, _ = conn.ExecContext(ctx, "SELECT RELEASE_LOCK(?)", migrateLockName)
}
