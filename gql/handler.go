package gql

import (
	"fmt"

	"github.com/golly-go/golly"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
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
		var ident golly.Identity

		// Ensure the context is of type WebContext
		wctx, ok := p.Context.(*golly.WebContext)
		if !ok {
			return nil, BadUserInput(fmt.Errorf("invalid context type"))
		}

		// Retrieve identity from context
		if identityFunc == nil {
			if !cfg.Public {
				// If identity function is not set and the handler is not public, return unauthenticated error
				return nil, Unauthenticated(nil)
			}
			goto execute

		}

		ident = identityFunc(wctx.Context)

		// Enrich logging with metadata
		wctx = wctx.WithContext(golly.WithLoggerFields(wctx.Context, metadata(p)))

		if !cfg.Public && (ident == nil || ident.IsValid() != nil) {
			return nil, Unauthenticated(nil)
		}

		goto execute

	execute:
		// Execute the handler
		result, err := handler(wctx, p)
		if err != nil {
			wctx.Logger().Errorf("error in GQL handler: %v", err)
			if gqlErr, ok := err.(*gqlerrors.Error); ok {
				return nil, gqlErr
			}
			return nil, InternalServerError(err)
		}

		// Ensure the modified context is passed back
		p.Context = wctx

		return result, nil
	}
}
