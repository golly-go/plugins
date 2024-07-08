package gql

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"github.com/golly-go/plugins/passport"
	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
)

// This plugins provides golly wrappers to allow easy to use GQL integration

type HandlerFunc func(golly.WebContext, Params) (interface{}, error)

type Options struct {
	Public bool

	Roles  []string
	Scopes []string

	Handler func(golly.WebContext, Params) (interface{}, error)
}

type Params struct {
	graphql.ResolveParams

	Request *http.Request

	HasInput bool

	Input map[string]interface{}

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

	metaData := map[string]interface{}{
		"gql.operation.type": p.ResolveParams.Info.Operation.GetOperation(),
		"gql.operation.name": name,
	}

	return metaData
}

func NewHandler(options Options) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (interface{}, error) {
		wctx := golly.WebContextFromGoContext(p.Context)

		defer func(ctx golly.WebContext) {
			if err := recover(); err != nil {
				ctx.Logger().Errorln("panic occurred:", err, string(debug.Stack()))
				panic(err)
			}
		}(*wctx)

		ident, _ := passport.FromContext(wctx.Context)
		params := Params{ResolveParams: p, HasInput: false, Identity: ident}

		// Add additional GQL logging to the lines
		wctx.Context.SetLogger(
			wctx.Context.
				Logger().
				WithFields(params.Metadata()),
		)

		if p.Args["input"] != nil {
			params.Input = map[string]interface{}{}
			if inp, ok := p.Args["input"].(map[string]interface{}); ok {
				params.HasInput = true
				params.Input = inp
			}
		}

		// TODO bring back the passport integration here
		if !options.Public {
			if params.Identity == nil || !params.Identity.IsLoggedIn() {
				return nil, errors.WrapForbidden(fmt.Errorf("must be logged in to view/perform this action"))
			}
		}

		ret, err := options.Handler(*wctx, params)
		if err != nil {
			wctx.Logger().Errorf("error in GQL request %s", err.Error())
			return nil, err
		}

		p.Context = golly.WebContextToGoContext(p.Context, *wctx)

		return ret, nil
	}
}
