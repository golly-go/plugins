package gql

import (
	"fmt"

	"github.com/golly-go/golly"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// This plugins provides golly wrappers to allow easy to use GQL integration

type HandlerFunc func(*golly.WebContext, graphql.ResolveParams) (interface{}, error)

type Option func(*Options)

type Options struct {
	Public bool
}

func WithPublic(public bool) Option {
	return func(options *Options) {
		options.Public = public
	}
}

func metadata(p graphql.ResolveParams) map[string]interface{} {
	var name string

	switch definition := p.Info.Operation.(type) {
	case *ast.OperationDefinition:
		if definition.Name != nil {
			name = definition.GetName().Value
		}
	default:
		name = "anonymous"
	}

	return map[string]interface{}{
		"gql.operation.type": p.Info.Operation.GetOperation(),
		"gql.operation.name": name,
	}
}

// NewHandler creates a GraphQL field resolver with WebContext and identity handling.
func NewHandler(handler HandlerFunc, options ...Option) graphql.FieldResolveFn {

	cfg := &Options{}
	for _, option := range options {
		option(cfg)
	}

	return func(p graphql.ResolveParams) (interface{}, error) {
		// Ensure the context is of type WebContext
		wctx, ok := p.Context.(*golly.WebContext)
		if !ok {
			return nil, fmt.Errorf("invalid context type")
		}

		// Retrieve identity from context
		if identityFunc == nil {
			if !cfg.Public {
				wctx.Logger().Warn("identity function not skipping - skipping auth check for non-public handler")
			}

			result, err := handler(wctx, p)
			if err != nil {
				wctx.Logger().Errorf("error in GQL handler: %v", err)
				return nil, err
			}
			return result, nil
		}

		ident := identityFunc(wctx.Context)

		// Enrich logging with metadata
		wctx.Context = golly.WithLoggerFields(wctx.Context, metadata(p))

		if !cfg.Public && (ident == nil || ident.IsValid() != nil) {
			return nil, fmt.Errorf("authentication required for this action")
		}

		// Execute the handler
		result, err := handler(wctx, p)
		if err != nil {
			wctx.Logger().Errorf("error in GQL handler: %v", err)
			return nil, err
		}

		// Ensure the modified context is passed back
		p.Context = wctx

		return result, nil
	}
}
