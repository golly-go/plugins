package gql

import (
	"net/http"
	"net/http/httptest"
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
	sc := schemaConfig()

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

func schemaConfig() graphql.SchemaConfig {
	sc := graphql.SchemaConfig{}

	if len(queries) > 0 {
		sc.Query = graphql.NewObject(graphql.ObjectConfig{
			Name:   "Query",
			Fields: queries,
		})
	}

	if len(mutations) > 0 {
		sc.Mutation = graphql.NewObject(graphql.ObjectConfig{
			Name:   "Mutation",
			Fields: mutations,
		})
	}

	return sc
}

func ExecuteGraphQLQuery(gctx golly.Context, graphqlQueries graphql.Fields, query string, variables map[string]interface{}) (*graphql.Result, error) {
	sc := graphql.SchemaConfig{}

	sc.Query = graphql.NewObject(graphql.ObjectConfig{
		Name:   "Query",
		Fields: graphqlQueries,
	})

	return ExecuteGraphQL(gctx, sc, query, variables)

}

func ExecuteGraphQLMutation(gctx golly.Context, mutationsFields graphql.Fields, mutation string, variables map[string]interface{}) (*graphql.Result, error) {
	sc := graphql.SchemaConfig{}

	sc.Query = graphql.NewObject(graphql.ObjectConfig{
		Name:   "Query",
		Fields: queries,
	})

	sc.Mutation = graphql.NewObject(graphql.ObjectConfig{
		Name:   "Mutations",
		Fields: mutationsFields,
	})

	return ExecuteGraphQL(gctx, sc, mutation, variables)
}

// Helper method to execute a GraphQL query
func ExecuteGraphQL(gctx golly.Context, sc graphql.SchemaConfig, query string, variables map[string]interface{}) (*graphql.Result, error) {
	schema, err := graphql.NewSchema(sc)
	if err != nil {
		return nil, err
	}

	req, _ := http.NewRequest("POST", "/graphql", nil)
	resp := httptest.NewRecorder()
	wctx := golly.NewWebContext(gctx, req, resp, "test")
	ctx := golly.WebContextToGoContext(gctx.Context(), wctx)

	params := graphql.Params{
		Schema:         schema,
		RequestString:  query,
		Context:        ctx,
		VariableValues: variables,
	}

	return graphql.Do(params), nil
}
