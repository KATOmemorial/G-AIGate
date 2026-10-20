package store

import (
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisOptions 是 Redis 连接参数（11.4 §3）。main 从 config.RedisConfig 装配传入；
// store 不 import internal/config——装配层依赖全部模块，反向依赖被 00 §5.1 禁止。
type RedisOptions struct {
	Addr string
	DB   int
}

// NewRedis 构造 go-redis v9 客户端（11.4 §2；01-工程规范 §1 基线）。
// 连接惰性建立，就绪性由 main 注册的 readyz Ping 表达；启动连通性 fail-fast
// 留给 20.1（台账 #20）。网络读写遵守调用 context 的 deadline，包括 readyz 的预算。
func NewRedis(opts RedisOptions) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:                  opts.Addr,
		DB:                    opts.DB,
		ContextTimeoutEnabled: true,
	})
}

// nowFn 是包内可注入时钟（测试覆盖以验证跨天/跨月的 periodTag 与 TTL 推导）；
// 生产恒为 time.Now。QuotaKey.String / TTLHint 的签名无时钟参数（11.4 §3 冻结），
// 故以包级变量承载，仅供测试覆盖。
var nowFn = time.Now

// nowUTC 返回当前 UTC 时刻：periodTag 与 TTL 推导一律按 UTC 日切/月切（11.4 §7 决策）。
func nowUTC() time.Time { return nowFn().UTC() }
