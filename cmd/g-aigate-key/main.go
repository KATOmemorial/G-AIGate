// g-aigate-key issues a user's first credential through a local-only command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"
	"unicode/utf8"

	"gaigate/internal/access"
	"gaigate/internal/config"
	"gaigate/internal/store"
)

const commandTimeout = 10 * time.Second

func main() { os.Exit(commandMain()) }

func commandMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return run(ctx, os.Args[1:], os.Stdout, os.Stderr, commandDeps{load: config.Load, open: openKeyDatabase})
}

// All seams are private to this command; the production service is unchanged.
type keyDatabase struct {
	users   store.UserRepo
	service access.KeyService
	close   func() error
}

type commandDeps struct {
	load func(string) (*config.Config, error)
	open func(context.Context, store.Options) (*keyDatabase, error)
}

func openKeyDatabase(ctx context.Context, opts store.Options) (*keyDatabase, error) {
	db, err := openMySQL(ctx, opts)
	if err != nil {
		return nil, err
	}
	pool, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("command: obtain initialized pool: %w", err)
	}
	users := store.NewUserRepo(db)
	return &keyDatabase{users: users, service: access.NewKeyService(store.NewAPIKeyRepo(db), users), close: pool.Close}, nil
}

func run(parent context.Context, args []string, stdout, stderr io.Writer, deps commandDeps) (code int) {
	ctx, cancel := context.WithTimeout(parent, commandTimeout)
	defer cancel()
	report := func(message string) { _, _ = fmt.Fprintln(stderr, message) }
	flags := flag.NewFlagSet("g-aigate-key", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Flag errors can contain user-supplied secrets.
	path := flags.String("config", "config.yaml", "configuration file")
	username := flags.String("user", "", "existing username (required)")
	name := flags.String("name", "", "Key name")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			report("用法: g-aigate-key -config config.yaml -user <已有用户名> [-name <名称>]")
			return 0
		}
		report("命令参数无效")
		return 1
	}
	if flags.NArg() != 0 || *username == "" || !utf8.ValidString(*username) || utf8.RuneCountInString(*username) > 64 {
		report("必须显式指定有效的 -user 用户名")
		return 1
	}
	if !utf8.ValidString(*name) || utf8.RuneCountInString(*name) > 128 {
		report("-name 必须是最多 128 个字符的 UTF-8 文本")
		return 1
	}
	if ctx.Err() != nil {
		report("命令已取消或超时")
		return 1
	}
	cfg, err := deps.load(*path)
	if err != nil {
		report("配置加载或校验失败")
		return 1
	}
	if ctx.Err() != nil {
		report("命令已取消或超时")
		return 1
	}
	db, err := deps.open(ctx, store.Options{Host: cfg.MySQL.Host, Port: cfg.MySQL.Port, User: cfg.MySQL.User,
		Password: cfg.MySQL.Password, Database: cfg.MySQL.Database})
	if err != nil {
		report("数据库初始化失败或命令已取消／超时")
		return 1
	}
	created := false
	defer func() {
		if err := db.close(); err != nil {
			code = 1
			if created {
				report("数据库关闭失败；Key 可能已创建，请另行查询或吊销")
			} else {
				report("数据库关闭失败")
			}
		}
	}()
	user, err := db.users.FindByUsername(ctx, *username)
	if err != nil || user == nil {
		report("查询已有用户失败")
		return 1
	}
	if user.Status != store.UserStatusEnabled || user.ID == 0 || user.ID > math.MaxInt64 || user.GroupID == 0 || user.GroupID > math.MaxInt64 {
		report("用户已禁用或身份数据无效")
		return 1
	}
	ctx = access.WithPrincipal(ctx, &access.Principal{UserID: int64(user.ID), APIKeyID: 0,
		GroupID: int64(user.GroupID), Username: user.Username, Role: user.Role})
	key, err := db.service.Create(ctx, *name)
	if err != nil {
		report("Key 创建失败或命令已取消／超时")
		return 1
	}
	created = true
	data, err := json.Marshal(key)
	if err == nil && ctx.Err() == nil {
		data = append(data, '\n')
		var n int
		n, err = stdout.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
	}
	if err != nil || ctx.Err() != nil {
		report("创建结果输出失败；Key 可能已创建，请另行查询或吊销")
		return 1
	}
	return 0
}
