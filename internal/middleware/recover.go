package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
)

// Recover panic 恢复中间件（10.2 §3 契约）：panic → 500 +
// OpenAI 风格错误体（code=gaigate_internal，00 §8.2）+ 带堆栈与
// request_id 的 Error 日志。位于 AccessLog 之外，panic 恢复后
// AccessLog 的 defer 摘要仍会带 request_id 输出。
func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				// 请求路径 request_id 缺失视为 bug（01 §4）：此处必有 RequestID 在外层注入
				requestID, _ := RequestIDFromContext(c.Request.Context())
				slog.Default().Error("panic recovered",
					"request_id", requestID,
					"panic", rec,
					"stack", string(debug.Stack()))
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": gin.H{
						"message": "internal server error",
						"type":    "gaigate_error",
						"code":    "gaigate_internal",
					},
					"request_id": requestID,
				})
			}
		}()
		c.Next()
	}
}
