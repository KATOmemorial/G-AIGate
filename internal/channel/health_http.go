package channel

import (
	"context"

	"gaigate/internal/store"

	"github.com/gin-gonic/gin"
)

// The real-time endpoint uses the same injected auth and admin predicate as
// management. It reads Redis through Read, never through the bool-only API.
func RegisterHealthRoutes(r gin.IRouter, auth gin.HandlerFunc, isAdmin func(context.Context) bool,
	channels store.ChannelRepo, health HealthService) {
	for _, dependency := range []any{r, auth, isAdmin, channels, health} {
		if nilDependency(dependency) {
			panic("channel: health routes require dependencies")
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
	g.GET("/:id/health", withChannelID(func(c *gin.Context, id int64) {
		ch, err := channels.Get(c.Request.Context(), id)
		if err != nil {
			writeAdminError(c, err)
			return
		}
		if ch == nil || ch.ID != uint64(id) {
			writeAdminError(c, failure("invalid health channel", nil))
			return
		}
		snapshot, err := health.Read(c.Request.Context(), id)
		if err != nil {
			writeAdminEnvelope(c, 503, "gaigate_health_unavailable", "channel health unavailable")
			return
		}
		respond(c, 200, healthResponse{id, ch.Enabled, ch.Enabled && snapshot.DisabledUntil == 0,
			snapshot.ConsecutiveFailures, snapshot.DisabledUntil, snapshot.LastProbeAt, snapshot.LastProbeOk})
	}))
}

type healthResponse struct {
	ChannelID           int64 `json:"channel_id"`
	Enabled             bool  `json:"enabled"`
	Available           bool  `json:"available"`
	ConsecutiveFailures int32 `json:"consecutive_failures"`
	DisabledUntil       int64 `json:"disabled_until"`
	LastProbeAt         int64 `json:"last_probe_at"`
	LastProbeOK         bool  `json:"last_probe_ok"`
}
