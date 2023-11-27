package gql

import (
	"sync"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"github.com/graphql-go/graphql"
)

type gqlHandler struct {
	schema graphql.Schema
	err    error
}

var mutations = graphql.Fields{}
var queries = graphql.Fields{
	"empty": &graphql.Field{
		Name:        "empty",
		Type:        graphql.String,
		Description: "empty",
		Resolve: NewHandler(Options{
			Handler: func(ctx golly.WebContext, p Params) (interface{}, error) {
				return "empty", nil
			},
		}),
	},
}

var lock sync.RWMutex

func RegisterQuery(inFields ...graphql.Fields) {
	defer lock.Unlock()
	lock.Lock()

	for _, fields := range inFields {
		for name, field := range fields {
			queries[name] = field
		}
	}
}

func RegisterMutation(inFields ...graphql.Fields) {
	for _, fields := range inFields {
		for name, field := range fields {
			mutations[name] = field
		}
	}
}

func NewGraphQL() gqlHandler {
	sc := graphql.SchemaConfig{}

	sc.Query = graphql.NewObject(graphql.ObjectConfig{
		Name:   "Query",
		Fields: queries,
	})

	if len(mutations) > 0 {
		sc.Mutation = graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: mutations,
		})
	}

	schema, err := graphql.NewSchema(sc)

	return gqlHandler{schema, errors.WrapGeneric(err)}
}

func (gql gqlHandler) Routes(r *golly.Route) {
	r.Post("/", gql.Perform)
}

type postData struct {
	Query     string                 `json:"query"`
	Operation string                 `json:"operation"`
	Variables map[string]interface{} `json:"variables"`
}

func (gql gqlHandler) Perform(wctx golly.WebContext) {

	var p postData

	if gql.err != nil {
		wctx.Logger().Error(gql.err)
		return
	}

	if err := wctx.Params(&p); err != nil {
		wctx.Logger().Error(err)
		return
	}

	ctx := golly.WebContextToGoContext(wctx.Request().Context(), wctx)

	result := graphql.Do(graphql.Params{
		Schema:         gql.schema,
		RequestString:  p.Query,
		VariableValues: p.Variables,
		OperationName:  p.Operation,
		Context:        ctx,
	})

	wctx.RenderJSON(result)
}
