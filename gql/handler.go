package gql

import (
	"fmt"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/passport"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// This plugins provides golly wrappers to allow easy to use GQL integration

type HandlerFunc func(*golly.WebContext, Params) (interface{}, error)

type Options struct {
	Public bool

	Roles  []string
	Scopes []string

	Handler HandlerFunc
}

type Params struct {
	graphql.ResolveParams

	Identity passport.Identity
}

func (p Params) Metadata() map[string]interface{} {
	var name string

	switch definition := p.ResolveParams.Info.Operation.(type) {
	case *ast.OperationDefinition:
		if definition.Name != nil {
			name = definition.GetName().Value
		}
	default:
		name = "anonymous"
	}

	return map[string]interface{}{
		"gql.operation.type": p.ResolveParams.Info.Operation.GetOperation(),
		"gql.operation.name": name,
	}
}

// NewHandler creates a GraphQL field resolver with WebContext and identity handling.
func NewHandler(options Options) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		// Ensure the context is of type WebContext
		wctx, ok := p.Context.(*golly.WebContext)
		if !ok {
			return nil, fmt.Errorf("invalid context type")
		}

		// Retrieve identity from context
		ident, _ := passport.FromContext(*wctx.Context)

		// Prepare GraphQL parameters
		params := Params{ResolveParams: p, Identity: ident}

		// Enrich logging with metadata
		wctx.Context = golly.WithLoggerFields(wctx.Context, params.Metadata())

		if !options.Public && (ident == nil || !ident.IsLoggedIn()) {
			return nil, fmt.Errorf("authentication required for this action")
		}

		// Execute the handler
		result, err := options.Handler(wctx, params)
		if err != nil {
			wctx.Logger().Errorf("error in GQL handler: %v", err)
			return nil, err
		}

		// Ensure the modified context is passed back
		p.Context = wctx

		return result, nil
	}
}
