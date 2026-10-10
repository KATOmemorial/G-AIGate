package middleware

import (
	"context"
	"log/slog"
	"time"

	"github.com/gin-gonic/gin"
)

// AccessLog 请求完成摘要中间件（10.2 §3 / 00 §8.3）：
// 每请求完成打一行 msg="request_done"，字段全集为
// ts, level（slog 自带）+ request_id, user_id, channel_id, model,
// latency_ms, status_class, status_code, method, path。
// 业务字段由后续模块通过 ctx 注入（context.go 的 With* 入口），
// B2 阶段为零值占位。
//
// 管道顺序（10.2 §3 契约，2026-10-10 修正）：RequestID → AccessLog → Recover。
// AccessLog 在 Recover 之外（先注册）：panic 展栈时内层 defer 先执行，
// Recover 先接住 panic 并写出 500，本中间件的 defer 后执行，
// status_code 记录真实值。已知取舍：AccessLog 自身 panic 不受
// Recover 保护（薄代码，风险接受，文档 §3 已注明）。
func AccessLog(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		defer func() {
			ctx := c.Request.Context()
			status := c.Writer.Status()
			logger.Info("request_done",
				"request_id", mustRequestID(ctx),
				"user_id", UserIDFromContext(ctx),
				"channel_id", ChannelIDFromContext(ctx),
				"model", ModelFromContext(ctx),
				"latency_ms", time.Since(start).Milliseconds(),
				"status_class", statusClass(status),
				"status_code", status,
				"method", c.Request.Method,
				"path", c.Request.URL.Path,
			)
		}()
		c.Next()
	}
}

// mustRequestID 取 request_id；AccessLog 位于 RequestID 之内，缺失即管道装配错误。
func mustRequestID(ctx context.Context) string {
	id, ok := RequestIDFromContext(ctx)
	if !ok {
		return ""
	}
	return id
}

// statusClass 归一化为 2xx/3xx/4xx/5xx（00 §8.3 status_class）。
func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	default:
		return "2xx"
	}
}
