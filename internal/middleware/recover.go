package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"strings"

	"github.com/gin-gonic/gin"
)

// Recover preserves ErrAbortHandler for net/http. Ordinary panics produce a
// safe fault/stack log and either a precommit 500 or a committed HTTP abort
// (13.3, 台账#26). Outer AccessLog still records the actual status once.
func Recover() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				// 请求路径 request_id 缺失视为 bug（01 §4）：此处必有 RequestID 在外层注入
				requestID, _ := RequestIDFromContext(c.Request.Context())
				if rec == http.ErrAbortHandler {
					slog.Default().ErrorContext(c.Request.Context(), "request aborted",
						"request_id", requestID, "reason", "http_abort", "wrote_header", c.Writer.Written())
					panic(rec)
				}
				slog.Default().Error("panic recovered",
					"request_id", requestID,
					"reason", "panic", "wrote_header", c.Writer.Written(),
					"stack", panicStack())
				if c.Writer.Written() {
					panic(http.ErrAbortHandler)
				}
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

// Record function/file/line frames without arguments or formatting the panic
// value. A panic may contain secrets or implement an unsafe String/Error method.
func panicStack() string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(2, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	var stack strings.Builder
	for {
		frame, more := frames.Next()
		_, _ = fmt.Fprintf(&stack, "%s\n\t%s:%d\n", frame.Function, frame.File, frame.Line)
		if !more {
			return stack.String()
		}
	}
}
