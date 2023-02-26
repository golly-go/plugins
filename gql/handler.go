package gql

import (
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/slimloans/golly"
)

// This plugins provides golly wrappers to allow easy to use GQL integration

type HandlerFunc func(golly.Context, Params) (interface{}, error)

type Options struct {
	Public bool

	Roles  []string
	Scopes []string

	Handler func(golly.Context, Params) (interface{}, error)
}

type Params struct {
	graphql.ResolveParams

	HasInput bool

	Input map[string]interface{}
}

func (p Params) Metadata() map[string]interface{} {
	var name string

	switch definition := p.Info.Operation.(type) {
	case *ast.OperationDefinition:
		if definition.Name != nil {
			name = definition.GetName().Value
		}
	default:
		name = "anonymous"
	}

	metaData := map[string]interface{}{
		"gql.operation.type": p.Info.Operation.GetOperation(),
		"gql.operation.name": name,
	}

	// if u := p.Identity.UserID(); u == uuid.Nil {
	// 	metaData["usr.id"] = p.Identity.UserID().String()
	// }

	return metaData
}

func NewHandler(options Options) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		ctx := golly.FromContext(p.Context)

		params := Params{ResolveParams: p, HasInput: false} //, Identity: passport.FromContext(ctx)}

		// Add additional GQL logging to the lines
		ctx.SetLogger(
			ctx.
				Logger().
				WithFields(params.Metadata()),
		)

		if p.Args["input"] != nil {
			if inp, ok := p.Args["input"].(map[string]interface{}); ok {
				params.HasInput = true
				params.Input = inp
			}
		}

		// TODO bring back the passport integration here
		// if !options.Public {
		// 	if !params.Identity.IsLoggedIn() {
		// 		return nil, errors.WrapForbidden(fmt.Errorf("must be logged in to view this"))
		// 	}
		// }

		// if len(options.Roles) > 0 && !params.Identity.HasRoles(options.Roles) {
		// 	return nil, errors.WrapForbidden(fmt.Errorf("missing required roled to view this"))
		// }

		// if len(options.Scopes) > 0 && !params.Identity.HasRoles(options.Scopes) {
		// 	return nil, errors.WrapForbidden(fmt.Errorf("missing required scopes to view this"))
		// }

		ret, err := options.Handler(ctx, params)
		if err != nil {
			return nil, err
		}

		return ret, nil
	}
}
