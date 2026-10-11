package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/oklog/ulid/v2"
)

// RequestID 入口中间件（00 §8.1 / 10.2 §3 契约）：
// 每请求生成 req_ + ULID，写入响应头 X-Request-Id 回显，并注入 context，
// 贯穿日志 / trace / usage_records 对账（00 §3.2 时序约束③）。
// ulid.Make 使用包级单调熵源（锁保护，并发安全），同毫秒内单调递增。
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := "req_" + ulid.Make().String()
		c.Header("X-Request-Id", id)
		c.Request = c.Request.WithContext(WithRequestID(c.Request.Context(), id))
		c.Next()
	}
}
