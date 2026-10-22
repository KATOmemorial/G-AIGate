package middleware

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() { gin.SetMode(gin.TestMode) }

// newCapturingLogger 输出到内存 buffer 的 slog JSON 日志器，供断言日志字段。
// 时间键 ReplaceAttr 与 main 的 newLogger 一致（00 §8.3：ts 而非 slog
// 默认的 time），保证测试断言的就是生产键名。
func newCapturingLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey && len(groups) == 0 {
				a.Key = "ts"
			}
			return a
		},
	}))
}

// findLogEntry 找到第一条 msg 匹配的日志行并解析为 map；不存在则失败。
func findLogEntry(t *testing.T, buf *bytes.Buffer, msg string) map[string]any {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("日志行非 JSON: %q: %v", line, err)
		}
		if m["msg"] == msg {
			return m
		}
	}
	t.Fatalf("未找到 msg=%q 的日志行，实际 buf=%s", msg, buf.String())
	return nil
}

// 用例①（10.2 §5）：request_id 在日志与响应头一致；ULID 形态与唯一性。
func TestRequestIDHeaderAndContextConsistent(t *testing.T) {
	r := gin.New()
	r.Use(RequestID())
	var ctxID string
	r.GET("/ping", func(c *gin.Context) {
		ctxID, _ = RequestIDFromContext(c.Request.Context())
		c.JSON(200, gin.H{"status": "ok"})
	})

	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/ping", nil))
		headerID := w.Header().Get("X-Request-Id")
		assert.NotEmpty(t, headerID)
		assert.True(t, strings.HasPrefix(headerID, "req_"), "须以 req_ 前缀: %s", headerID)
		assert.Len(t, strings.TrimPrefix(headerID, "req_"), 26, "ULID 须为 26 字符")
		assert.Equal(t, headerID, ctxID, "ctx 与响应头的 request_id 须一致")
		assert.False(t, seen[headerID], "request_id 须唯一: %s", headerID)
		seen[headerID] = true
	}
}

// 用例②（10.2 §5）：panic 路由返回 500 + gaigate_internal + request_id。
func TestRecoverReturns500WithGaigateInternal(t *testing.T) {
	r := gin.New()
	r.Use(RequestID(), Recover())
	r.GET("/boom", func(c *gin.Context) { panic("boom for test") })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/boom", nil))
	require.Equal(t, 500, w.Code)

	var body struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    string `json:"code"`
		} `json:"error"`
		RequestID string `json:"request_id"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "gaigate_internal", body.Error.Code)
	assert.Equal(t, "gaigate_error", body.Error.Type)
	assert.NotEmpty(t, body.Error.Message)
	// 00 §8.2：错误体 request_id 与响应头一致，供客户端凭此对账
	assert.Equal(t, w.Header().Get("X-Request-Id"), body.RequestID)
}

// 用例③（10.2 §5，契约 2026-10-10 修正）：契约顺序
// RequestID → AccessLog → Recover 下，AccessLog 输出必含 request_id
// （正常与 panic 请求均须输出摘要行），且 panic 请求的 request_done
// 记录真实 status_code=500（Recover 先写 500，AccessLog 的 defer 后执行）。
func TestPipelineOrderAccessLogContainsRequestID(t *testing.T) {
	var buf bytes.Buffer
	logger := newCapturingLogger(&buf)
	// Recover 的堆栈日志走 slog.Default()（与 main 装配一致：SetDefault）
	prev := slog.Default()
	slog.SetDefault(logger)
	t.Cleanup(func() { slog.SetDefault(prev) })
	r := gin.New()
	r.Use(RequestID(), AccessLog(logger), Recover())
	r.GET("/ok", func(c *gin.Context) { c.JSON(200, gin.H{}) })
	r.GET("/boom", func(c *gin.Context) { panic("boom for test") })

	// 正常请求：request_done 字段全集（00 §8.3 + 10.2 §3）
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/ok", nil))
	entry := findLogEntry(t, &buf, "request_done")
	assert.Equal(t, w.Header().Get("X-Request-Id"), entry["request_id"])
	// 00 §8.3：时间键为 ts（评审阻断项①补断言，防 slog 默认 time 回归）
	assert.Contains(t, entry, "ts")
	assert.NotContains(t, entry, "time")
	assert.Equal(t, "GET", entry["method"])
	assert.Equal(t, "/ok", entry["path"])
	assert.EqualValues(t, 200, entry["status_code"])
	assert.Equal(t, "2xx", entry["status_class"])
	// 业务字段 B2 阶段零值占位（后续模块经 ctx 注入）
	assert.EqualValues(t, 0, entry["user_id"])
	assert.EqualValues(t, 0, entry["channel_id"])
	assert.Equal(t, "", entry["model"])
	assert.Contains(t, entry, "latency_ms")

	// panic 请求：request_done 仍输出且带 request_id；Recover 已先写出 500，
	// AccessLog 的 defer 记录真实 status_code（契约修正的核心验收点）
	buf.Reset()
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/boom", nil))
	require.Equal(t, 500, w.Code)
	entry = findLogEntry(t, &buf, "request_done")
	assert.Equal(t, w.Header().Get("X-Request-Id"), entry["request_id"])
	assert.EqualValues(t, 500, entry["status_code"])
	assert.Equal(t, "5xx", entry["status_class"])

	// panic 堆栈日志（Recover 产出）也带同一 request_id，且含堆栈
	stackEntry := findLogEntry(t, &buf, "panic recovered")
	assert.Equal(t, w.Header().Get("X-Request-Id"), stackEntry["request_id"])
	assert.Equal(t, "panic", stackEntry["reason"])
	assert.NotContains(t, buf.String(), "boom for test")
	assert.Contains(t, stackEntry["stack"], "recover.go")
}

// 用例④（10.2 §5）：WrapWriter 断言 Flusher/Hijacker 透传。
func TestWrapWriterPassthrough(t *testing.T) {
	rec := httptest.NewRecorder()
	base := &fakeGinWriter{ResponseRecorder: rec}
	wrapped := WrapWriter(base)

	// gin.ResponseWriter 接口保持（可继续作为 15 流式包装的基底）
	require.Implements(t, (*gin.ResponseWriter)(nil), wrapped)

	// Flusher 透传
	f, ok := wrapped.(http.Flusher)
	require.True(t, ok, "包装后须仍实现 http.Flusher")
	f.Flush()
	assert.True(t, rec.Flushed, "Flush 须透传到底层")

	// Hijacker 透传
	h, ok := wrapped.(http.Hijacker)
	require.True(t, ok, "包装后须仍实现 http.Hijacker")
	_, _, err := h.Hijack()
	require.Error(t, err, "测试桩 Hijack 返回错误即证明调用透传到了底层")
	assert.True(t, base.hijackCalled)

	// 基础写行为透传
	wrapped.WriteHeader(200)
	_, err = wrapped.Write([]byte("hi"))
	require.NoError(t, err)
	assert.Equal(t, 200, rec.Code)
	assert.Equal(t, "hi", rec.Body.String())
}

func TestWrapWriterUnwrapReturnsImmediateLayer(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	first := WrapWriter(c.Writer)
	second := WrapWriter(first)
	require.Same(t, first, second.(interface{ Unwrap() http.ResponseWriter }).Unwrap())
	require.Same(t, c.Writer, first.(interface{ Unwrap() http.ResponseWriter }).Unwrap())
	second.WriteHeader(http.StatusCreated)
	require.False(t, c.Writer.Written(), "Gin status staging must remain intact")
	require.Equal(t, http.StatusCreated, c.Writer.Status())
	_, err := second.Write([]byte("hi"))
	require.NoError(t, err)
	require.True(t, c.Writer.Written())
	require.Equal(t, 2, c.Writer.Size())
	require.Equal(t, "hi", rec.Body.String())
	require.Equal(t, http.StatusCreated, rec.Code)
}

// ctx 注入/读取辅助（业务字段由后续模块通过 ctx 补充，10.2 §3）。
func TestContextHelpers(t *testing.T) {
	tests := []struct {
		name string
		inj  func(context.Context) context.Context
		want any
		got  func(context.Context) any
	}{
		{"user_id 未注入为零值", nil, int64(0), func(c context.Context) any { return UserIDFromContext(c) }},
		{"channel_id 未注入为零值", nil, int64(0), func(c context.Context) any { return ChannelIDFromContext(c) }},
		{"model 未注入为空串", nil, "", func(c context.Context) any { return ModelFromContext(c) }},
		{"request_id 未注入为缺", nil, false, func(c context.Context) any { _, ok := RequestIDFromContext(c); return ok }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.inj != nil {
				ctx = tt.inj(ctx)
			}
			assert.Equal(t, tt.want, tt.got(ctx))
		})
	}

	ctx := context.Background()
	ctx = WithRequestID(ctx, "req_x")
	ctx = WithUserID(ctx, 42)
	ctx = WithChannelID(ctx, 7)
	ctx = WithModel(ctx, "gpt-4o")
	id, ok := RequestIDFromContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, "req_x", id)
	assert.EqualValues(t, 42, UserIDFromContext(ctx))
	assert.EqualValues(t, 7, ChannelIDFromContext(ctx))
	assert.Equal(t, "gpt-4o", ModelFromContext(ctx))
}

// fakeGinWriter 是实现 gin.ResponseWriter 的测试桩（httptest.Recorder
// 不含 Hijack，补齐 gin 接口面并记录 Hijack 调用）。
type fakeGinWriter struct {
	*httptest.ResponseRecorder
	hijackCalled bool
}

func (w *fakeGinWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijackCalled = true
	return nil, nil, assert.AnError
}

// CloseNotify 实现 gin.ResponseWriter 接口面（继承自 http.CloseNotifier，
// 桩实现仅返回 nil，本模块不消费该 deprecated 通知）。
func (w *fakeGinWriter) CloseNotify() <-chan bool { return nil }

func (w *fakeGinWriter) Status() int                       { return w.Code }
func (w *fakeGinWriter) Size() int                         { return w.Body.Len() }
func (w *fakeGinWriter) WriteString(s string) (int, error) { return w.Body.WriteString(s) }
func (w *fakeGinWriter) Written() bool                     { return w.Code != 0 }
func (w *fakeGinWriter) WriteHeaderNow()                   {}
func (w *fakeGinWriter) Pusher() http.Pusher               { return nil }
