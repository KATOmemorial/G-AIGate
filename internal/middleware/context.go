// Context helpers preserve standard directional values; the private 13.3
// summary also carries user/channel/model changes to the outer completion log.
package middleware

import (
	"context"
	"sync"
)

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyUserID
	ctxKeyChannelID
	ctxKeyModel
	ctxKeySummary
)

// requestSummary carries log fields only. Identity remains in the ordinary
// context values; each AccessLog invocation installs a fresh, private instance.
type requestSummary struct {
	mu                sync.RWMutex
	userID, channelID int64
	model             string
}

func withRequestSummary(ctx context.Context) context.Context {
	return context.WithValue(ctx, ctxKeySummary, &requestSummary{
		userID: UserIDFromContext(ctx), channelID: ChannelIDFromContext(ctx), model: ModelFromContext(ctx),
	})
}

func summaryFromContext(ctx context.Context) *requestSummary {
	summary, _ := ctx.Value(ctxKeySummary).(*requestSummary)
	return summary
}

func completedFields(ctx context.Context) (int64, int64, string) {
	if summary := summaryFromContext(ctx); summary != nil {
		summary.mu.RLock()
		defer summary.mu.RUnlock()
		return summary.userID, summary.channelID, summary.model
	}
	return UserIDFromContext(ctx), ChannelIDFromContext(ctx), ModelFromContext(ctx)
}

// WithRequestID 将 request_id 注入 context（入口中间件调用）。
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, ctxKeyRequestID, id)
}

// RequestIDFromContext 取 request_id；缺失时第二返回值为 false
// （请求路径日志缺 request_id 视为 bug，01-工程规范 §4）。
func RequestIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ctxKeyRequestID).(string)
	return id, ok
}

// WithUserID 由 13-access 鉴权成功后注入 Principal.UserID。
func WithUserID(ctx context.Context, id int64) context.Context {
	if summary := summaryFromContext(ctx); summary != nil {
		summary.mu.Lock()
		summary.userID = id
		summary.mu.Unlock()
	}
	return context.WithValue(ctx, ctxKeyUserID, id)
}

// UserIDFromContext 取 user_id，未注入时返回零值占位（B2）。
func UserIDFromContext(ctx context.Context) int64 {
	id, _ := ctx.Value(ctxKeyUserID).(int64)
	return id
}

// WithChannelID 由 16-router 选定渠道后注入。
func WithChannelID(ctx context.Context, id int64) context.Context {
	if summary := summaryFromContext(ctx); summary != nil {
		summary.mu.Lock()
		summary.channelID = id
		summary.mu.Unlock()
	}
	return context.WithValue(ctx, ctxKeyChannelID, id)
}

// ChannelIDFromContext reads the standard value, never the mutable log summary.
func ChannelIDFromContext(ctx context.Context) int64 {
	id, _ := ctx.Value(ctxKeyChannelID).(int64)
	return id
}

// WithModel 由 13.3 解析请求体后注入。
func WithModel(ctx context.Context, model string) context.Context {
	if summary := summaryFromContext(ctx); summary != nil {
		summary.mu.Lock()
		summary.model = model
		summary.mu.Unlock()
	}
	return context.WithValue(ctx, ctxKeyModel, model)
}

// ModelFromContext 取 model，未注入时返回零值占位（B2）。
func ModelFromContext(ctx context.Context) string {
	model, _ := ctx.Value(ctxKeyModel).(string)
	return model
}
