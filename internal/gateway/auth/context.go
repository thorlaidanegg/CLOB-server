package auth

import "context"

// contextKey is the unexported key type for storing AuthContext in a context.Context.
// Using a private type prevents collisions with other packages' context values.
type contextKey struct{}

// WithContext returns a new context carrying ac.
func WithContext(ctx context.Context, ac AuthContext) context.Context {
	return context.WithValue(ctx, contextKey{}, ac)
}

// FromContext extracts the AuthContext stored by Middleware or WithContext.
// Returns the zero value and false if no auth context is present.
func FromContext(ctx context.Context) (AuthContext, bool) {
	ac, ok := ctx.Value(contextKey{}).(AuthContext)
	return ac, ok
}
