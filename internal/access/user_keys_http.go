package access

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
)

const userKeyBodyLimit = 64 * 1024

// RegisterUserKeyRoutes owns the complete path. Cache policy precedes identity
// so authentication failures are also never cached. 19.1 replaces identity only.
func RegisterUserKeyRoutes(r gin.IRouter, identity gin.HandlerFunc, svc KeyService) {
	if identity == nil || svc == nil {
		panic("access: user Key routes require identity and service")
	}
	g := r.Group("/api/v1/user/keys", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Next()
	}, identity, func(c *gin.Context) {
		p, ok := PrincipalFromContext(c.Request.Context())
		if !ok || p.UserID <= 0 {
			writeAuthError(c, ErrUnauthorized)
			return
		}
		c.Next()
	})
	g.POST("", func(c *gin.Context) {
		mediaType, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeError(c, 415, "gaigate_unsupported_media_type", "application/json required")
			return
		}
		body := http.MaxBytesReader(c.Writer, c.Request.Body, userKeyBodyLimit)
		defer func() { _ = body.Close() }() // Request-body close has no recoverable response action.
		data, err := io.ReadAll(body)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeError(c, 413, "gaigate_request_too_large", "request body too large")
			} else {
				writeKeyError(c, errKeyInput)
			}
			return
		}
		name, err := decodeKeyName(data)
		if err != nil {
			writeKeyError(c, err)
			return
		}
		created, err := svc.Create(c.Request.Context(), name)
		if err != nil {
			writeKeyError(c, err)
			return
		}
		c.JSON(http.StatusCreated, created)
	})
	g.GET("", func(c *gin.Context) {
		views, err := svc.List(c.Request.Context())
		if err != nil {
			writeKeyError(c, err)
			return
		}
		c.JSON(http.StatusOK, views)
	})
	g.DELETE("/:id", func(c *gin.Context) {
		value := c.Param("id")
		id, err := strconv.ParseInt(value, 10, 64)
		if err != nil || id <= 0 || strings.Trim(value, "0123456789") != "" {
			writeKeyError(c, errKeyInput)
			return
		}
		if err := svc.Revoke(c.Request.Context(), id); err != nil {
			writeKeyError(c, err)
			return
		}
		c.Status(http.StatusNoContent)
	})
}

// Read is bounded before decoding. Tokens preserve duplicate field detection,
// including escaped spellings that decode to the same field name.
func decodeKeyName(data []byte) (string, error) {
	if !utf8.Valid(data) {
		return "", errKeyInput
	}
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return "", errKeyInput
	}
	var name string
	seen := false
	for d.More() {
		field, err := d.Token()
		if err != nil || field != "name" || seen {
			return "", errKeyInput
		}
		seen = true
		var value json.RawMessage
		if err := d.Decode(&value); err != nil || bytes.Equal(value, []byte("null")) {
			return "", errKeyInput
		}
		if err := json.Unmarshal(value, &name); err != nil {
			return "", errKeyInput
		}
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return "", errKeyInput
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) || !validKeyName(name) {
		return "", errKeyInput
	}
	return name, nil
}

func writeKeyError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, errKeyInput):
		writeError(c, 400, "gaigate_invalid_request", "invalid user Key input")
	case errors.Is(err, errOwnedKeyGone):
		writeError(c, 404, "gaigate_not_found", "Key not found")
	default:
		writeAuthError(c, err)
	}
}
