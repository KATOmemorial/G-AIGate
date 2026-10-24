package channel

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"gaigate/internal/middleware"
	"gaigate/internal/store"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type healthReadStub struct {
	HealthService
	read func(context.Context, int64) (store.HealthSnapshot, error)
}

func (s *healthReadStub) Read(ctx context.Context, id int64) (store.HealthSnapshot, error) {
	return s.read(ctx, id)
}

func TestRealTimeHealthHTTPWhitelistAuthAndFailure(t *testing.T) {
	for _, mode := range []string{"unknown", "healthy", "disabled", "enabled_false", "anonymous", "non_admin", "missing", "sql_error", "redis_error", "invalid_id"} {
		t.Run(mode, func(t *testing.T) {
			reads, gets := 0, 0
			r, _ := serviceRepo()
			r.get = func(context.Context, int64) (*store.Channel, error) {
				gets++
				if mode == "missing" {
					return nil, store.ErrNotFound
				}
				if mode == "sql_error" {
					return nil, errors.New("private SQL credentials")
				}
				return &store.Channel{ID: 1, Enabled: mode != "enabled_false", HealthStatus: "disabled"}, nil
			}
			snapshot := store.HealthSnapshot{}
			if mode == "healthy" {
				snapshot = store.HealthSnapshot{ConsecutiveFailures: 2, LastProbeAt: 50, LastProbeOk: true}
			}
			if mode == "disabled" {
				snapshot = store.HealthSnapshot{ConsecutiveFailures: 5, DisabledUntil: 1, LastProbeAt: 50}
			}
			health := &healthReadStub{read: func(context.Context, int64) (store.HealthSnapshot, error) {
				reads++
				if mode == "redis_error" {
					return store.HealthSnapshot{}, errors.New("private Redis credentials")
				}
				return snapshot, nil
			}}
			engine := gin.New()
			engine.Use(middleware.RequestID())
			auth := func(c *gin.Context) {
				if mode == "anonymous" {
					writeAdminEnvelope(c, 401, "gaigate_unauthorized", "unauthorized")
					return
				}
				c.Next()
			}
			RegisterHealthRoutes(engine, auth, func(context.Context) bool { return mode != "non_admin" }, r, health)
			path := "/api/v1/admin/channels/1/health"
			if mode == "invalid_id" {
				path = "/api/v1/admin/channels/0/health"
			}
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			status := map[string]int{"anonymous": 401, "non_admin": 403, "missing": 404, "sql_error": 500, "redis_error": 503, "invalid_id": 400}[mode]
			if status == 0 {
				status = 200
			}
			require.Equal(t, status, w.Code)
			require.NotContains(t, w.Body.String(), "credentials")
			if status != 200 {
				if mode == "redis_error" {
					require.Contains(t, w.Body.String(), "gaigate_health_unavailable")
					require.Equal(t, 1, reads)
				} else {
					require.Zero(t, reads)
				}
				if mode == "anonymous" || mode == "non_admin" || mode == "invalid_id" {
					require.Zero(t, gets)
				}
				return
			}
			var fields map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &fields))
			require.Len(t, fields, 7)
			require.Equal(t, mode != "enabled_false" && snapshot.DisabledUntil == 0, fields["available"])
			require.Equal(t, float64(snapshot.ConsecutiveFailures), fields["consecutive_failures"])
			require.Equal(t, float64(snapshot.DisabledUntil), fields["disabled_until"])
			require.Equal(t, float64(snapshot.LastProbeAt), fields["last_probe_at"])
			require.Equal(t, snapshot.LastProbeOk, fields["last_probe_ok"])
		})
	}
}
