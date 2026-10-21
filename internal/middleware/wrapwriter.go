package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// wrapWriter 以组合方式包装 gin.ResponseWriter：嵌入的接口自身
// 声明了 http.Flusher / http.Hijacker / http.CloseNotifier，因此包装后
// 这些接口必然透传到最底层 writer——这是 10-L1 风险①（15 流式依赖
// Flusher/Hijacker，自定义包装易丢接口）的前置解（10.2 §4 决策③）。
type wrapWriter struct {
	gin.ResponseWriter
}

// Unwrap exposes only the immediate next writer (15.1 §3.3). Gin's concrete
// Unwrap is absent from its ResponseWriter interface; explicitly preserve the
// chain so callers can discover error-returning Flush without bypassing writes.
func (w *wrapWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// WrapWriter 返回一个永远透传 Flusher 与 Hijacker 的 ResponseWriter
// 包装器（10.2 §3 契约签名）。后续需要拦截写行为的中间件（15 proxy
// 的流式逐分片写出）以本类型为基底再包一层，接口不丢。
func WrapWriter(w gin.ResponseWriter) gin.ResponseWriter {
	return &wrapWriter{ResponseWriter: w}
}
