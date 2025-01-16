package passport

import (
	"context"

	"github.com/golly-go/golly"
)

var identCtxKey golly.ContextKey = "identity"

type Identity interface {
	Valid() error
	IsLoggedIn() bool
	UserID() string
}

// IdentityToContext set the identity in a context
func ToContext(ctx context.Context, ident Identity) *golly.Context {
	return golly.WithValue(ctx, identCtxKey, ident)
}

// FromContext retrieves identity from a context
func FromContext(ctx golly.Context) (Identity, bool) {
	if i, ok := ctx.Value(identCtxKey).(Identity); ok {
		return i, true
	}
	return nil, false
}
