package channel

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/store"
)

func RegisterAdminRoutes(r gin.IRouter, auth gin.HandlerFunc, isAdmin func(context.Context) bool, service AdminService, tester ConnectivityTester) {
	for _, dependency := range []any{r, auth, isAdmin, service, tester} {
		if nilDependency(dependency) {
			panic("channel: admin routes require dependencies")
		}
	}
	g := r.Group("/api/v1/admin/channels", func(c *gin.Context) {
		c.Header("Cache-Control", "no-store")
		c.Next()
	}, auth, func(c *gin.Context) {
		if !isAdmin(c.Request.Context()) {
			writeAdminEnvelope(c, 403, "gaigate_forbidden", "administrator required")
			return
		}
		c.Next()
	})
	g.GET("", func(c *gin.Context) {
		views, err := service.List(c.Request.Context())
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 200, gin.H{"data": views})
	})
	g.GET("/:id", withChannelID(func(c *gin.Context, id int64) {
		view, err := service.Get(c.Request.Context(), id)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 200, view)
	}))
	g.POST("", func(c *gin.Context) {
		in, err := readChannelConfig(c, true)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		view, err := service.Create(c.Request.Context(), in)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 201, view)
	})
	g.PUT("/:id", withChannelID(func(c *gin.Context, id int64) {
		in, err := readChannelConfig(c, false)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		view, err := service.Update(c.Request.Context(), id, updateInput(in))
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 200, view)
	}))
	g.DELETE("/:id", withChannelID(func(c *gin.Context, id int64) {
		if err := service.Delete(c.Request.Context(), id); err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 204, nil)
	}))
	g.POST("/:id/keys", withChannelID(func(c *gin.Context, id int64) {
		fields, err := readAdminObject(c, []string{"key"}, []string{"key"})
		var raw string
		if err == nil && (decodeField(fields, "key", &raw) != nil || !validRawKey(raw)) {
			err = errInvalidInput
		}
		if err != nil {
			writeAdminError(c, err)
			return
		}
		view, err := service.AddKey(c.Request.Context(), id, raw)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 201, view)
	}))
	g.PUT("/:id/keys/:key_id", withChannelID(func(c *gin.Context, id int64) {
		keyID, err := adminID(c.Param("key_id"))
		if err != nil {
			writeAdminError(c, err)
			return
		}
		fields, err := readAdminObject(c, []string{"weight", "status"}, []string{"weight", "status"})
		var weight int32
		var status int8
		if err == nil && (decodeField(fields, "weight", &weight) != nil || decodeField(fields, "status", &status) != nil ||
			weight < 1 || weight > 10000 || (status != 0 && status != 1)) {
			err = errInvalidInput
		}
		if err != nil {
			writeAdminError(c, err)
			return
		}
		view, err := service.UpdateKey(c.Request.Context(), id, keyID, weight, status)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 200, view)
	}))
	g.DELETE("/:id/keys/:key_id", withChannelID(func(c *gin.Context, id int64) {
		keyID, err := adminID(c.Param("key_id"))
		if err == nil {
			err = service.RemoveKey(c.Request.Context(), id, keyID)
		}
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 204, nil)
	}))
	g.POST("/:id/test", withChannelID(func(c *gin.Context, id int64) {
		fields, err := readAdminObject(c, []string{"model"}, []string{"model"})
		var model string
		if err == nil && (decodeField(fields, "model", &model) != nil || !validName(model)) {
			err = errInvalidInput
		}
		if err != nil {
			writeAdminError(c, err)
			return
		}
		detail, err := service.Get(c.Request.Context(), id)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		if detail == nil || detail.ID != id {
			writeAdminError(c, failure("invalid probe channel", nil))
			return
		}
		mapped := false
		for _, mapping := range detail.Models {
			mapped = mapped || mapping.Model == model
		}
		if !mapped {
			writeAdminError(c, errInvalidInput)
			return
		}
		// The service exposes only a safe management view. Reconstitute the
		// explicit endpoint configuration needed by the injected tester.
		ch := &store.Channel{ID: uint64(id), Provider: detail.Provider, BaseURL: detail.BaseURL, Enabled: detail.Enabled}
		result, err := tester.Test(c.Request.Context(), ch, model)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		respond(c, 200, result)
	}))
}

func adminID(raw string) (int64, error) {
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 || strings.Trim(raw, "0123456789") != "" {
		return 0, errInvalidInput
	}
	return id, nil
}

func withChannelID(handler func(*gin.Context, int64)) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, err := adminID(c.Param("id"))
		if err != nil {
			writeAdminError(c, err)
			return
		}
		handler(c, id)
	}
}

func respond(c *gin.Context, status int, value any) {
	if c.Request.Context().Err() != nil {
		// Abort alone still lets Gin commit its default success response.
		// Unwind through the existing Recover to net/http without any write.
		panic(http.ErrAbortHandler)
	}
	if status == http.StatusNoContent {
		c.Status(status)
		return
	}
	c.JSON(status, value)
}

func writeAdminEnvelope(c *gin.Context, status int, code, message string) {
	if c.Request.Context().Err() != nil {
		panic(http.ErrAbortHandler)
	}
	requestID, _ := middleware.RequestIDFromContext(c.Request.Context())
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"message": message, "type": "gaigate_error", "code": code}, "request_id": requestID})
}

func writeAdminError(c *gin.Context, err error) {
	status, code, message := 500, "gaigate_internal", "internal server error"
	switch {
	case errors.Is(err, errInvalidInput), errors.Is(err, errProbeMapping):
		status, code, message = 400, "gaigate_invalid_request", "invalid channel input"
	case errors.Is(err, errBodyTooLarge):
		status, code, message = 413, "gaigate_request_too_large", "request body too large"
	case errors.Is(err, errMediaType):
		status, code, message = 415, "gaigate_unsupported_media_type", "application/json required"
	case errors.Is(err, store.ErrNotFound):
		status, code, message = 404, "gaigate_not_found", "channel resource not found"
	case errors.Is(err, provider.ErrNoCandidate):
		status, code, message = 503, "gaigate_no_candidate", "no upstream candidate"
	}
	if status == 500 && c.Request.Context().Err() == nil {
		requestID, _ := middleware.RequestIDFromContext(c.Request.Context())
		slog.Default().ErrorContext(c.Request.Context(), "channel administration failed", "request_id", requestID, "code", code)
	}
	writeAdminEnvelope(c, status, code, message)
}
