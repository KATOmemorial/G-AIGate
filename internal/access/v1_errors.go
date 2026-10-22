package access

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
)

func writeV1Error(c *gin.Context, err error) {
	var execution *ExecutionError
	errors.As(err, &execution)
	if c.Writer.Written() || execution != nil && execution.WroteHeader {
		// M2 makes the production Recover transparently propagate this sentinel.
		panic(http.ErrAbortHandler)
	}
	// Consume the classified budget flag before inspecting Canceled: a latched
	// Overall result can legitimately retain both deadline and cancellation.
	if execution != nil && execution.TimedOut {
		writeError(c, 502, "gaigate_upstream_timeout", "upstream timed out")
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(c.Request.Context().Err(), context.Canceled) {
		// Gin commits an implicit 200 after a normal handler return, even after
		// Abort. Unwind instead so a canceled request performs no further writes.
		panic(http.ErrAbortHandler)
	}
	status, code, message := 500, "gaigate_internal", "internal server error"
	var upstream *provider.UpstreamError
	switch {
	case errors.Is(err, ErrModelNotFound):
		status, code, message = 404, "gaigate_model_not_found", "model not found"
	case errors.Is(err, provider.ErrNoCandidate):
		status, code, message = 503, "gaigate_no_candidate", "no upstream candidate"
	case errors.As(err, &upstream):
		status, code, message = 502, "gaigate_upstream_error", "upstream request failed"
		if upstream.StatusCode == 401 || upstream.StatusCode == 403 {
			code, message = "gaigate_upstream_auth_failed", "upstream authentication failed"
		} else if body, ok := safeUpstreamError(c, upstream); ok {
			requestID, _ := middleware.RequestIDFromContext(c.Request.Context())
			c.AbortWithStatusJSON(upstream.StatusCode, gin.H{"error": body, "request_id": requestID})
			return
		}
	case errors.Is(err, provider.ErrUpstreamUnavailable), errors.Is(err, provider.ErrStreamInterrupted):
		status, code, message = 502, "gaigate_upstream_error", "upstream request failed"
	}
	requestID, _ := middleware.RequestIDFromContext(c.Request.Context())
	// Fixed classification only: no cause, upstream body, URL or credentials.
	slog.Default().ErrorContext(c.Request.Context(), "v1 request failed", "request_id", requestID, "code", code)
	writeError(c, status, code, message)
}

func safeUpstreamError(c *gin.Context, upstream *provider.UpstreamError) (json.RawMessage, bool) {
	if upstream.BodyTruncated || (upstream.StatusCode != 400 && upstream.StatusCode != 404 && upstream.StatusCode != 422) {
		return nil, false
	}
	fields, err := uniqueJSONObject(upstream.Body)
	if err != nil {
		return nil, false
	}
	raw := fields["error"]
	object, err := uniqueJSONObject(raw)
	var message, kind string
	if err != nil || !nonemptyJSONString(object["message"], &message) || !nonemptyJSONString(object["type"], &kind) {
		return nil, false
	}
	for _, name := range []string{"code", "param"} {
		if value, exists := object[name]; exists && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			var text string
			if json.Unmarshal(value, &text) != nil {
				return nil, false
			}
		}
	}
	key, ok := bearerKey(c.Request.Header)
	if !ok || containsJSONCredential(upstream.Body, key) {
		return nil, false
	}
	return raw, true
}

// Decode strings including field names, so escaped credentials are caught.
// The adapter has already checked Endpoint.APIKey; this checks client Bearer.
func containsJSONCredential(data []byte, key string) bool {
	if key == "" {
		return true
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	for {
		token, err := d.Token()
		if errors.Is(err, io.EOF) {
			return false
		}
		if err != nil {
			return true
		}
		if value, ok := token.(string); ok && strings.Contains(value, key) {
			return true
		}
	}
}
