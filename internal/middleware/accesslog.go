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
// 摘要在 defer 中输出：panic 请求也能产出 request_done（AccessLog 在
// Recover 之内，10.2 §3 契约）。已知取舍：panic 时刻响应尚未写出
// （外层 Recover 的 defer 随后才写 500），status_code 记为 gin 默认值 200；
// 请求的真实结果以 panic 日志与 500 响应为准。
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
