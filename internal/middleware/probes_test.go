package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 用例⑤（10.2 §5）：readyz 动态注册——0 检查项与全通过 → 200；
// 任一失败 → 503 且列出失败项。
func TestProbesReadyHandler(t *testing.T) {
	tests := []struct {
		name         string
		register     func(p *Probes)
		wantCode     int
		wantStatus   string
		wantFailedOf []string // 期望出现在 failed 列表中的检查项名
	}{
		{
			name:       "空注册器（store 未建，空转）→ 200",
			register:   func(p *Probes) {},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
		},
		{
			name: "单检查通过 → 200",
			register: func(p *Probes) {
				p.Register("mysql", func(ctx context.Context) error { return nil })
			},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
		},
		{
			name: "多检查全部通过 → 200",
			register: func(p *Probes) {
				p.Register("mysql", func(ctx context.Context) error { return nil })
				p.Register("redis", func(ctx context.Context) error { return nil })
			},
			wantCode:   http.StatusOK,
			wantStatus: "ok",
		},
		{
			name: "任一失败 → 503 且列出失败项（用例⑤核心）",
			register: func(p *Probes) {
				p.Register("mysql", func(ctx context.Context) error { return nil })
				p.Register("redis", func(ctx context.Context) error {
					return errors.New("connection refused")
				})
			},
			wantCode:     http.StatusServiceUnavailable,
			wantStatus:   "unavailable",
			wantFailedOf: []string{"redis"},
		},
		{
			name: "多项失败 → 503 全部列出",
			register: func(p *Probes) {
				p.Register("mysql", func(ctx context.Context) error {
					return errors.New("dial timeout")
				})
				p.Register("redis", func(ctx context.Context) error {
					return errors.New("connection refused")
				})
			},
			wantCode:     http.StatusServiceUnavailable,
			wantStatus:   "unavailable",
			wantFailedOf: []string{"mysql", "redis"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &Probes{}
			tt.register(p)

			r := gin.New()
			r.GET("/readyz", p.ReadyHandler())
			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			require.Equal(t, tt.wantCode, w.Code)
			var body struct {
				Status string   `json:"status"`
				Failed []string `json:"failed"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, tt.wantStatus, body.Status)
			for _, name := range tt.wantFailedOf {
				// 失败项格式为 "name: err"，按前缀定位
				found := false
				for _, f := range body.Failed {
					if strings.HasPrefix(f, name+":") {
						found = true
						break
					}
				}
				assert.True(t, found, "失败项列表 %v 须含 %q 前缀项", body.Failed, name)
			}
			if tt.wantCode == http.StatusOK {
				assert.Empty(t, body.Failed)
			}
		})
	}
}

// 失败项格式为 "name: err"，可直接定位失败依赖。
func TestProbesFailedEntryFormat(t *testing.T) {
	p := &Probes{}
	p.Register("redis", func(ctx context.Context) error { return errors.New("boom") })

	r := gin.New()
	r.GET("/readyz", p.ReadyHandler())
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Contains(t, w.Body.String(), "redis: boom")
}
