// Package bootstrap provides assembly adapters, consumed only by cmd.
package bootstrap

import (
	"context"
	"errors"
	"gaigate/internal/access"
	"gaigate/internal/provider"
	"gaigate/internal/router"
	"net/http"
	"reflect"
	"time"
)

// AllowAll remains the explicit precheck stub.
// TODO(17.2): replace at assembly with real quota checks/settlement (台账#22).
type AllowAll struct{}

func (AllowAll) Precheck(context.Context, *access.Principal) error { return nil }

type executor struct {
	planner  router.Router
	executor router.Executor
	overall  time.Duration
}

// NewExecutor covers Route and Execute with one logical budget. Selection and
// attempt policy belong entirely to router.
func NewExecutor(planner router.Router, execution router.Executor, overall time.Duration) (access.ChatExecutor, error) {
	if nilDependency(planner) || nilDependency(execution) || overall <= 0 {
		return nil, errors.New("bootstrap: invalid executor dependencies or budget")
	}
	return &executor{planner, execution, overall}, nil
}
func (e *executor) Execute(parent context.Context, w http.ResponseWriter, p *access.Principal, req *provider.ChatRequest) error {
	if parent == nil || nilDependency(w) || p == nil || req == nil {
		return errors.New("bootstrap: invalid execution input")
	}
	ctx, cancel := context.WithTimeout(parent, e.overall)
	defer cancel()
	boundary := func(cause error) error {
		if ctx.Err() != nil {
			return &access.ExecutionError{Cause: errors.Join(cause, ctx.Err(), context.Cause(ctx)), TimedOut: ctx.Err() == context.DeadlineExceeded}
		}
		return cause
	}
	if err := boundary(nil); err != nil {
		return err
	}
	plan, err := e.planner.Route(ctx, req.Model, router.Auto)
	if err = boundary(err); err != nil {
		return err
	}
	// Preserve the actual executor classification, including latched Overall
	// followed by net/http Canceled and any already committed response.
	return e.executor.Execute(ctx, w, p, plan, req)
}
func nilDependency(value any) bool {
	if value == nil {
		return true
	}
	switch reflect.ValueOf(value).Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflect.ValueOf(value).IsNil()
	}
	return false
}
