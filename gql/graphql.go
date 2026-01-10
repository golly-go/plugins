package gql

import (
	"context"
	"net/http"
	"sync"

	"github.com/golly-go/golly"
	"github.com/graphql-go/graphql"
)

var identityFunc func(context.Context) golly.Identity

// SchemaOption allows customization of the schema before it is compiled.
type SchemaOption func(*graphql.SchemaConfig)

// GraphQL represents a configurable GraphQL server instance.
type GraphQL struct {
	mu                 sync.RWMutex
	queryFields        graphql.Fields
	mutationFields     graphql.Fields
	subscriptionFields graphql.Fields
	schema             graphql.Schema
	schemaErr          error
	schemaDirty        bool
	schemaOptions      []SchemaOption
}

// NewGraphQL initializes a new GraphQL server with its own registry of fields and configuration.
func NewGraphQL(opts ...ConfigOption) *GraphQL {
	cfg := &Config{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.identityFunc != nil {
		RegisterIdentityFunc(cfg.identityFunc)
	}

	return &GraphQL{
		queryFields:        graphql.Fields{},
		mutationFields:     graphql.Fields{},
		subscriptionFields: graphql.Fields{},
		schemaDirty:        true,
		schemaOptions:      cfg.schemaOpts,
	}
}

// RegisterQuery registers query fields to the instance schema.
func (g *GraphQL) RegisterQuery(flds ...graphql.Fields) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, fields := range flds {
		for name, field := range fields {
			g.queryFields[name] = field
		}
	}

	g.schemaDirty = true
}

// RegisterSubscription registers subscription fields to the instance schema.
func (g *GraphQL) RegisterSubscription(flds ...graphql.Fields) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.subscriptionFields == nil {
		g.subscriptionFields = graphql.Fields{}
	}

	for _, fields := range flds {
		for name, field := range fields {
			g.subscriptionFields[name] = field
		}
	}

	g.schemaDirty = true
}

// ApplySchemaOptions appends schema customization options and rebuilds the schema on next access.
func (g *GraphQL) ApplySchemaOptions(opts ...SchemaOption) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.schemaOptions = append(g.schemaOptions, opts...)
	g.schemaDirty = true
}

// RegisterMutation registers mutation fields to the instance schema.
func (g *GraphQL) RegisterMutation(flds ...graphql.Fields) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, fields := range flds {
		for name, field := range fields {
			g.mutationFields[name] = field
		}
	}

	g.schemaDirty = true
}

// Schema returns the compiled GraphQL schema for the instance.
func (g *GraphQL) Schema() (graphql.Schema, error) {
	g.ensureSchema()

	g.mu.RLock()
	defer g.mu.RUnlock()

	if g.schemaErr != nil {
		return graphql.Schema{}, g.schemaErr
	}

	return g.schema, nil
}

// Routes registers GraphQL-related routes for the instance.
func (g *GraphQL) Routes(r *golly.Route) {
	r.Post("/", g.Perform)
}

type postData struct {
	Query     string                 `json:"query"`
	Operation string                 `json:"operation"`
	Variables map[string]interface{} `json:"variables"`
}

// Perform executes a GraphQL query or mutation using the instance schema.
func (g *GraphQL) Perform(wctx *golly.WebContext) {
	schema, err := g.Schema()
	if err != nil {
		wctx.Logger().Error("GraphQL schema initialization error: ", err)
		wctx.Response().WriteHeader(http.StatusInternalServerError)
		return
	}

	var ident golly.Identity
	if identityFunc != nil {
		ident = identityFunc(wctx)
	}

	resolverCtx := newResolverContext(wctx, ident)

	var p postData
	if err := wctx.Marshal(&p); err != nil {
		wctx.Logger().Error("Failed to parse request body: ", err)
		wctx.Response().WriteHeader(http.StatusBadRequest)
		return
	}

	result := graphql.Do(graphql.Params{
		Schema:         schema,
		RequestString:  p.Query,
		VariableValues: p.Variables,
		OperationName:  p.Operation,
		Context:        resolverCtx,
	})

	wctx.RenderJSON(result)
}

// Execute runs a GraphQL operation against the instance schema without HTTP helpers.
func (g *GraphQL) Execute(ctx context.Context, query string, variables map[string]interface{}, operation string) (*graphql.Result, error) {
	schema, err := g.Schema()
	if err != nil {
		return nil, err
	}

	params := graphql.Params{
		Schema:         schema,
		RequestString:  query,
		VariableValues: variables,
		OperationName:  operation,
		Context:        ctx,
	}

	return graphql.Do(params), nil
}

func (g *GraphQL) ensureSchema() {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.schemaDirty {
		return
	}

	queries := copyFields(g.queryFields)
	if len(queries) == 0 {
		queries = emptyField()
	}

	mutations := copyFields(g.mutationFields)
	if len(mutations) == 0 {
		mutations = emptyField()
	}

	subscriptions := copyFields(g.subscriptionFields)

	schemaConfig := graphql.SchemaConfig{
		Query:    graphql.NewObject(graphql.ObjectConfig{Name: "Query", Fields: queries}),
		Mutation: graphql.NewObject(graphql.ObjectConfig{Name: "Mutation", Fields: mutations}),
	}

	if len(subscriptions) > 0 {
		schemaConfig.Subscription = graphql.NewObject(graphql.ObjectConfig{Name: "Subscription", Fields: subscriptions})
	}

	for _, opt := range g.schemaOptions {
		if opt != nil {
			opt(&schemaConfig)
		}
	}

	schema, err := graphql.NewSchema(schemaConfig)

	g.schema = schema
	g.schemaErr = err
	g.schemaDirty = false
}

func copyFields(src graphql.Fields) graphql.Fields {
	if len(src) == 0 {
		return graphql.Fields{}
	}

	clone := make(graphql.Fields, len(src))
	for name, field := range src {
		clone[name] = field
	}

	return clone
}

func emptyField() graphql.Fields {
	return graphql.Fields{"empty": &graphql.Field{Type: graphql.String, Resolve: func(p graphql.ResolveParams) (interface{}, error) {
		return "empty", nil
	}}}
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
