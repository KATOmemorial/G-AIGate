package access

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"sort"

	"github.com/gin-gonic/gin"

	"gaigate/internal/middleware"
	"gaigate/internal/provider"
	"gaigate/internal/store"
)

var ErrModelNotFound = errors.New("model alias not found")

// These are entry-point consumption seams. B5/B6 adapt their own interfaces at
// assembly time; access must not depend on router, meter, proxy or bootstrap.
type Prechecker interface {
	Precheck(ctx context.Context, p *Principal) error
}

type ChatExecutor interface {
	Execute(ctx context.Context, w http.ResponseWriter, p *Principal, req *provider.ChatRequest) error
}

type ModelLister interface {
	List(ctx context.Context) ([]store.ModelAlias, error)
}

// ExecutionError carries the actual commit boundary and the already classified
// budget result. Its public text never formats an untrusted cause.
type ExecutionError struct {
	Cause       error
	WroteHeader bool
	TimedOut    bool
}

func (e *ExecutionError) Error() string { return "access: execution failed" }
func (e *ExecutionError) Unwrap() error { return e.Cause }

// RegisterV1Routes owns the complete /v1 paths; auth always runs before reading
// a body. GET models is configuration discovery, without chat prechecking.
func RegisterV1Routes(r gin.IRouter, auth gin.HandlerFunc, precheck Prechecker, executor ChatExecutor, models ModelLister) {
	if auth == nil || nilV1Dependency(precheck) || nilV1Dependency(executor) || nilV1Dependency(models) {
		panic("access: v1 routes require auth, precheck, executor and models")
	}
	g := r.Group("/v1", auth, func(c *gin.Context) {
		p, ok := PrincipalFromContext(c.Request.Context())
		if !ok || p.UserID <= 0 || p.APIKeyID <= 0 || p.GroupID <= 0 {
			writeAuthError(c, ErrUnauthorized)
			return
		}
		c.Next()
	})
	g.POST("/chat/completions", func(c *gin.Context) {
		req, status, code := readChatRequest(c)
		if status != 0 {
			writeError(c, status, code, "invalid chat request")
			return
		}
		ctx := middleware.WithModel(c.Request.Context(), req.Model)
		c.Request = c.Request.WithContext(ctx)
		p, _ := PrincipalFromContext(ctx)
		if err := precheck.Precheck(ctx, p); err != nil {
			writeV1Error(c, err)
			return
		}
		if err := executor.Execute(ctx, c.Writer, p, req); err != nil {
			writeV1Error(c, err)
		}
	})
	g.GET("/models", func(c *gin.Context) {
		aliases, err := models.List(c.Request.Context())
		if err != nil {
			writeV1Error(c, err)
			return
		}
		type model struct {
			ID      string `json:"id"`
			Object  string `json:"object"`
			OwnedBy string `json:"owned_by"`
		}
		data := make([]model, 0, len(aliases))
		for _, alias := range aliases {
			data = append(data, model{ID: alias.Name, Object: "model", OwnedBy: "gaigate"})
		}
		sort.Slice(data, func(i, j int) bool { return data[i].ID < data[j].ID })
		c.JSON(http.StatusOK, gin.H{"object": "list", "data": data})
	})
}

func nilV1Dependency(value any) bool {
	if value == nil {
		return true
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflect.ValueOf(value).IsNil()
	default:
		return false
	}
}
