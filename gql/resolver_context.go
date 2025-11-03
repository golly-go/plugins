package gql

import "github.com/golly-go/golly"

type ResolverContext struct {
	*golly.WebContext
	identity golly.Identity
}

func newResolverContext(wctx *golly.WebContext, identity golly.Identity) *ResolverContext {
	return &ResolverContext{
		WebContext: wctx,
		identity:   identity,
	}
}

// NewResolverContext constructs a ResolverContext for manual or test execution paths.
func NewResolverContext(wctx *golly.WebContext, identity golly.Identity) *ResolverContext {
	return newResolverContext(wctx, identity)
}

func (rc *ResolverContext) Identity() golly.Identity {
	return rc.identity
}

func (rc *ResolverContext) WithIdentity(identity golly.Identity) *ResolverContext {
	rc.identity = identity
	return rc
}
