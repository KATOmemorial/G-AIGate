// context key 与取值辅助：request_id 及 request_done 摘要的业务字段
// （00 §8.3：user_id / channel_id / model，由后续模块通过 ctx 补充，
// 10.2 阶段 AccessLog 以零值占位）。key 定义在消费方包内（工程约定）。
package middleware

import "context"

type ctxKey int

const (
	ctxKeyRequestID ctxKey = iota
	ctxKeyUserID
	ctxKeyChannelID
	ctxKeyModel
)

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
	return context.WithValue(ctx, ctxKeyUserID, id)
}

// UserIDFromContext 取 user_id，未注入时返回零值占位（B2）。
func UserIDFromContext(ctx context.Context) int64 {
	id, _ := ctx.Value(ctxKeyUserID).(int64)
	return id
}

// WithChannelID 由 16-router 选定渠道后注入。
func WithChannelID(ctx context.Context, id int64) context.Context {
	return context.WithValue(ctx, ctxKeyChannelID, id)
}

// ChannelIDFromContext 取 channel_id，未注入时返回零值占位（B2）。
func ChannelIDFromContext(ctx context.Context) int64 {
	id, _ := ctx.Value(ctxKeyChannelID).(int64)
	return id
}

// WithModel 由 13.3 解析请求体后注入。
func WithModel(ctx context.Context, model string) context.Context {
	return context.WithValue(ctx, ctxKeyModel, model)
}

// ModelFromContext 取 model，未注入时返回零值占位（B2）。
func ModelFromContext(ctx context.Context) string {
	model, _ := ctx.Value(ctxKeyModel).(string)
	return model
}
