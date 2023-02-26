package gql

import (
	"sync"

	"github.com/graphql-go/graphql"
	"github.com/slimloans/golly"
	"github.com/slimloans/golly/errors"
	"github.com/slimloans/golly/plugins/orm"
)

type gqlHandler struct {
	schema graphql.Schema
	err    error
}

var mutations = graphql.Fields{}
var queries = graphql.Fields{}

var lock sync.RWMutex

func RegisterQuery(fields graphql.Fields) {
	defer lock.Unlock()
	lock.Lock()

	for name, field := range fields {
		queries[name] = field
	}
}

func RegisterMutation(fields graphql.Fields) {
	for name, field := range fields {
		mutations[name] = field
	}
}

func NewGraphQL() gqlHandler {
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

	result := graphql.Do(graphql.Params{
		Schema:         gql.schema,
		RequestString:  p.Query,
		VariableValues: p.Variables,
		OperationName:  p.Operation,

		// TODO Clean this up need a better way of passing golly.Context down
		Context: orm.ToContext(wctx.Context.ToContext(), orm.DB(wctx.Context)),
	})

	wctx.RenderJSON(result)
}
