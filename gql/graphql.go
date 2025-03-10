package gql

import (
	"net/http"
	"sync"

	"github.com/golly-go/golly"
	"github.com/graphql-go/graphql"
)

type gqlHandler struct {
	schema graphql.Schema
	err    error
}

var (
	queryRegistry    = graphql.Fields{}
	mutationRegistry = graphql.Fields{}
	lock             sync.RWMutex
)

// RegisterQuery registers query fields to the schema.
func RegisterQuery(fields graphql.Fields) {
	lock.Lock()
	defer lock.Unlock()
	for name, field := range fields {
		queryRegistry[name] = field
	}
}

// RegisterMutation registers mutation fields to the schema.
func RegisterMutation(fields graphql.Fields) {
	lock.Lock()
	defer lock.Unlock()
	for name, field := range fields {
		mutationRegistry[name] = field
	}
}

// NewGraphQL initializes a new GraphQL handler with the current registry.
func NewGraphQL() gqlHandler {
	schema, err := graphql.NewSchema(graphql.SchemaConfig{
		Query:    graphql.NewObject(graphql.ObjectConfig{Name: "Query", Fields: queryRegistry}),
		Mutation: graphql.NewObject(graphql.ObjectConfig{Name: "Mutation", Fields: mutationRegistry}),
	})

	return gqlHandler{schema: schema, err: err}
}

// Routes registers GraphQL-related routes.
func (gql gqlHandler) Routes(r *golly.Route) {
	r.Post("/", gql.Perform)
}

type postData struct {
	Query     string                 `json:"query"`
	Operation string                 `json:"operation"`
	Variables map[string]interface{} `json:"variables"`
}

// Perform executes a GraphQL query or mutation.
func (gql gqlHandler) Perform(wctx *golly.WebContext) {
	if gql.err != nil {
		wctx.Logger().Error("GraphQL schema initialization error: ", gql.err)
		wctx.Response().WriteHeader(http.StatusInternalServerError)
		return
	}

	var p postData
	if err := wctx.Marshal(&p); err != nil {
		wctx.Logger().Error("Failed to parse request body: ", err)
		wctx.Response().WriteHeader(http.StatusBadRequest)
		return
	}

	result := graphql.Do(graphql.Params{
		Schema:         gql.schema,
		RequestString:  p.Query,
		VariableValues: p.Variables,
		OperationName:  p.Operation,
		Context:        wctx,
	})

	wctx.RenderJSON(result)
}

// ExecuteGraphQL executes a standalone GraphQL query with a specified schema configuration.
func ExecuteGraphQL(gctx *golly.Context, sc graphql.SchemaConfig, query string, variables map[string]interface{}) (*graphql.Result, error) {
	schema, err := graphql.NewSchema(sc)
	if err != nil {
		return nil, err
	}

	params := graphql.Params{
		Schema:         schema,
		RequestString:  query,
		VariableValues: variables,
		Context:        gctx,
	}

	return graphql.Do(params), nil
}
