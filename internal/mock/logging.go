package mock

import (
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/oklog/ulid/v2"
)

const chatRunKey = "mock.chat_run"

// NewLogger is the mock process's JSON logger. Tests use this same constructor
// so that the required ts key is verified on the actual production handler.
func NewLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(w, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, attr slog.Attr) slog.Attr {
			if len(groups) == 0 && attr.Key == slog.TimeKey {
				attr.Key = "ts"
			}
			return attr
		},
	}))
}

// RequestLog assigns a request ID and emits one summary even when an injected
// ErrAbortHandler unwinds the stack. It deliberately does not recover panics.
// Only static route patterns are logged; keys, Authorization and bodies are not.
func RequestLog(logger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		id := c.GetHeader("X-Request-Id")
		encoded, prefixed := strings.CutPrefix(id, "req_")
		if _, err := ulid.ParseStrict(encoded); !prefixed || err != nil {
			id = "req_" + ulid.Make().String()
		}
		c.Header("X-Request-Id", id)
		defer func() {
			model, scenario, outcome, chunks := "", Scenario(""), "completed", 0
			if c.Writer.Status() >= 400 {
				outcome = "error"
			}
			if value, ok := c.Get(chatRunKey); ok {
				run := value.(*chatRun)
				model, scenario, chunks, outcome = run.meta.Model, run.scenario, run.chunksWritten, run.outcome
			} else if c.Request.Context().Err() != nil {
				outcome = "canceled"
			}
			level := slog.LevelInfo
			if c.Writer.Status() >= 500 {
				level = slog.LevelError
			}
			logger.Log(c.Request.Context(), level, "request_done",
				"request_id", id, "model", model, "scenario", string(scenario),
				"chunks_written", chunks, "outcome", outcome,
				"latency_ms", time.Since(started).Milliseconds(),
				"method", c.Request.Method, "path", c.FullPath(),
			)
		}()
		c.Next()
	}
}
