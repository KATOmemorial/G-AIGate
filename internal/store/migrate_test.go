package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// 集成测试对接 docker compose 的 MySQL（deploy/infra）。
// MySQL 不可达时跳过（t.Skip）而非失败，保证 `go test ./...` 在无中间件
// 环境依然全绿（10.x 回归基线）；可用环境变量覆盖连接参数：
//
//	GAIGATE_TEST_MYSQL_HOST / GAIGATE_TEST_MYSQL_PORT / GAIGATE_TEST_MYSQL_PASSWORD
//
// 测试使用独立数据库 gaigate_test（非网关数据库 gaigate），避免污染
// §5 验收用的开发数据；每个用例重建测试库保证隔离。

const testDatabase = "gaigate_test"

func testOptions(t *testing.T) Options {
	t.Helper()
	port := 3307 // 宿主机 docker 映射端口（deploy/infra/.env MYSQL_PORT）
	if v := os.Getenv("GAIGATE_TEST_MYSQL_PORT"); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("GAIGATE_TEST_MYSQL_PORT 非数字: %v", err)
		}
		port = p
	}
	host := os.Getenv("GAIGATE_TEST_MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	password := os.Getenv("GAIGATE_TEST_MYSQL_PASSWORD")
	if password == "" {
		password = "gaigate_dev" // deploy/infra/.env.example MYSQL_ROOT_PASSWORD
	}
	return Options{Host: host, Port: port, User: "root", Password: password, Database: testDatabase}
}

// recreateTestDatabase 重建空的测试库；MySQL 不可达时跳过测试。
func recreateTestDatabase(t *testing.T, opts Options) {
	t.Helper()
	admin, err := sql.Open("mysql", fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?parseTime=true&charset=utf8mb4",
		opts.User, opts.Password, opts.Host, opts.Port))
	if err != nil {
		t.Fatalf("打开管理连接: %v", err)
	}
	defer admin.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Skipf("MySQL 不可达，跳过集成测试（docker compose up mysql 后重跑）: %v", err)
	}
	for _, stmt := range []string{
		"DROP DATABASE IF EXISTS " + testDatabase,
		"CREATE DATABASE " + testDatabase,
	} {
		if _, err := admin.ExecContext(context.Background(), stmt); err != nil {
			t.Fatalf("执行 %q: %v", stmt, err)
		}
	}
}

// tableNames 返回库内全部表名（含 golang-migrate 的 schema_migrations）。
func tableNames(t *testing.T, opts Options) []string {
	t.Helper()
	db, err := sql.Open("mysql", opts.DSN())
	if err != nil {
		t.Fatalf("打开连接: %v", err)
	}
	defer db.Close()
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		t.Fatalf("SHOW TABLES: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("扫描表名: %v", err)
		}
		names = append(names, name)
	}
	return names
}

// 11.1 §3 的 11 张业务表。
var wantTables = map[string]bool{
	"groups": true, "users": true, "api_keys": true, "channels": true,
	"channel_models": true, "channel_keys": true, "model_aliases": true,
	"model_prices": true, "usage_records": true, "quotas": true, "bills": true,
}

func checkTables(t *testing.T, opts Options) {
	t.Helper()
	names := tableNames(t, opts)
	got := map[string]bool{}
	for _, n := range names {
		got[n] = true
	}
	for tbl := range wantTables {
		if !got[tbl] {
			t.Errorf("缺少表 %q（实际表: %v）", tbl, names)
		}
	}
	if !got["schema_migrations"] {
		t.Errorf("缺少 schema_migrations（golang-migrate 版本表，实际表: %v）", names)
	}
	if len(names) != len(wantTables)+1 {
		t.Errorf("表数量应为 11+1，实际 %d: %v", len(names), names)
	}
}

func TestRunMigrations_CreatesAllTables(t *testing.T) {
	opts := testOptions(t)
	recreateTestDatabase(t, opts)

	if err := RunMigrations(opts, "../../migrations"); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	checkTables(t, opts)
}

func TestRunMigrations_Idempotent(t *testing.T) {
	opts := testOptions(t)
	recreateTestDatabase(t, opts)

	// 连续两次 up：第二次应成功返回（migrate.ErrNoChange 已被吸收）
	for i := 1; i <= 2; i++ {
		if err := RunMigrations(opts, "../../migrations"); err != nil {
			t.Fatalf("第 %d 次 RunMigrations: %v", i, err)
		}
	}
	checkTables(t, opts)
}

func TestMigrateDownUpChain(t *testing.T) {
	opts := testOptions(t)
	recreateTestDatabase(t, opts)

	if err := RunMigrations(opts, "../../migrations"); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}

	// down 1：全部业务表被清除（000001_init.down 为全量回滚）
	db, err := sql.Open("mysql", opts.DSN()+"&multiStatements=true")
	if err != nil {
		t.Fatalf("打开迁移连接: %v", err)
	}
	defer db.Close()
	m, err := newMigrator(db, "../../migrations")
	if err != nil {
		t.Fatalf("newMigrator: %v", err)
	}
	if err := m.Steps(-1); err != nil { // down 1：回滚一个版本
		t.Fatalf("migrate down 1: %v", err)
	}
	names := tableNames(t, opts)
	for _, n := range names {
		if wantTables[n] {
			t.Fatalf("down 后业务表 %q 仍存在", n)
		}
	}

	// up 1：schema 完整恢复
	if err := m.Up(); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	checkTables(t, opts)
}
