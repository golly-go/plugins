package passport

import "github.com/golly-go/golly"

type identCtx string

var identCtxKey identCtx = "identity"

type Identity interface {
	Valid() error
	IsLoggedIn() bool
	UserID() string
}

// IdentityToContext set the identity in a context
func ToContext(ctx golly.Context, ident Identity) golly.Context {
	return ctx.Set(identCtxKey, ident)
}

// FromContext retrieves identity from a context
func FromContext(ctx golly.Context) (Identity, bool) {
	if i, ok := ctx.Get(identCtxKey); ok {
		if ident, ok := i.(Identity); ok {
			return ident, true
		}
	}
	return nil, false
}
