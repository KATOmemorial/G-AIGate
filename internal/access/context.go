package access

import "context"

type principalContextKey struct{}

// WithPrincipal keeps a value snapshot so changes to the caller's pointer do not
// mutate an identity already placed in context. A nil identity masks any parent.
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	if p == nil {
		return context.WithValue(ctx, principalContextKey{}, (*Principal)(nil))
	}
	return context.WithValue(ctx, principalContextKey{}, *p)
}

func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	p, ok := ctx.Value(principalContextKey{}).(Principal)
	if !ok {
		return nil, false
	}
	return &p, true
}
