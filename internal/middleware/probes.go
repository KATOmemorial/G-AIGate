package middleware

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
)

// probeCheck 单个就绪检查项（10.2 §3）。
type probeCheck struct {
	name  string
	check func(ctx context.Context) error
}

// Probes 是 /readyz 探针注册器（10.2 §3 / §4 决策①）：
// 依赖就绪后动态注册检查项（B2 末由 main 注册 MySQL/Redis ping；
// B3+ 模块可继续追加），滚动更新时 /readyz 翻转先于 SIGTERM 生效。
// 当前 store 未建（11.x 交付），注册器空转：0 检查项 → 恒 200。
type Probes struct {
	mu     sync.RWMutex
	checks []probeCheck
}

// Register 注册一个就绪检查项；check 返回非 nil error 即该项失败。
// name 用于失败项列表定位（如 "mysql"、"redis"）。
// 注意：注册器不设单项超时，挂起的 check 会拖住整个 /readyz
// （check 收到请求 ctx，客户端断开即取消；K8s 探针自带超时）——
// B2 末注册 MySQL/Redis ping 时 check 须自带超时。
func (p *Probes) Register(name string, check func(ctx context.Context) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checks = append(p.checks, probeCheck{name: name, check: check})
}

// ReadyHandler 全部检查通过 → 200；任一失败 → 503 + 失败项列表
// （10.2 §5 用例⑤）。检查逐项执行，失败信息格式 "name: err"。
func (p *Probes) ReadyHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		p.mu.RLock()
		checks := make([]probeCheck, len(p.checks))
		copy(checks, p.checks)
		p.mu.RUnlock()

		var failed []string
		for _, ck := range checks {
			if err := ck.check(c.Request.Context()); err != nil {
				failed = append(failed, fmt.Sprintf("%s: %v", ck.name, err))
			}
		}
		if len(failed) == 0 {
			c.JSON(http.StatusOK, gin.H{"status": "ok"})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "unavailable",
			"failed": failed,
		})
	}
}
