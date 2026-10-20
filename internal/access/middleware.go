package access

import (
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"gaigate/internal/middleware"
)

func AuthMiddleware(a Authenticator) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw, ok := bearerKey(c.Request.Header)
		if !ok || !validRawKey(raw) {
			writeAuthError(c, ErrUnauthorized)
			return
		}
		if a == nil {
			writeAuthError(c, errors.New("missing authenticator"))
			return
		}
		p, err := a.Authenticate(c.Request.Context(), raw)
		if err != nil {
			writeAuthError(c, err)
			return
		}
		if p == nil || p.UserID <= 0 || p.APIKeyID <= 0 || p.GroupID <= 0 {
			writeAuthError(c, errors.New("invalid authenticated identity"))
			return
		}
		ctx := WithPrincipal(c.Request.Context(), p)
		ctx = middleware.WithUserID(ctx, p.UserID)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

// Inspect all values, including differently cased map keys. Header.Get alone
// would silently accept the first of several credentials.
func bearerKey(header http.Header) (string, bool) {
	var value string
	found := false
	for name, values := range header {
		if !strings.EqualFold(name, "Authorization") {
			continue
		}
		if found || len(values) != 1 {
			return "", false
		}
		found, value = true, values[0]
	}
	if !found || strings.Contains(value, ",") {
		return "", false
	}
	fields := strings.Fields(value)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return "", false
	}
	return fields[1], true
}

func writeAuthError(c *gin.Context, err error) {
	status, code, message := http.StatusInternalServerError, "gaigate_internal", "internal server error"
	switch {
	case errors.Is(err, ErrUnauthorized):
		status, code, message = http.StatusUnauthorized, "gaigate_unauthorized", "invalid API Key"
	case errors.Is(err, ErrKeyRevoked):
		status, code, message = http.StatusForbidden, "gaigate_key_revoked", "API Key revoked"
	case errors.Is(err, ErrKeyExpired):
		status, code, message = http.StatusForbidden, "gaigate_key_expired", "API Key expired"
	case errors.Is(err, ErrUserDisabled):
		status, code, message = http.StatusForbidden, "gaigate_user_disabled", "user disabled"
	}
	if status == http.StatusInternalServerError {
		requestID, _ := middleware.RequestIDFromContext(c.Request.Context())
		slog.Default().ErrorContext(c.Request.Context(), "authentication failed", "request_id", requestID, "code", code)
	}
	writeError(c, status, code, message)
}
