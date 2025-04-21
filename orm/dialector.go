package orm

import (
	"database/sql"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"
)

// CustomDialector wraps the stdlib DB and dynamically generates IAM tokens per connection.
type CustomDialector struct {
	DB        *sql.DB
	Dialector gorm.Dialector
}

func (dialect CustomDialector) Name() string {
	return "postgres"
}

func (dialect CustomDialector) Initialize(db *gorm.DB) error {
	db.ConnPool = dialect.DB
	dialect.Dialector = postgres.New(postgres.Config{Conn: dialect.DB})

	return nil
}

func (dialect CustomDialector) Migrator(db *gorm.DB) gorm.Migrator {
	return dialect.Dialector.Migrator(db)
}

func (dialect CustomDialector) DataTypeOf(field *schema.Field) string {
	return dialect.Dialector.DataTypeOf(field)
}

func (dialect CustomDialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return dialect.Dialector.DefaultValueOf(field)
}

func (dialect CustomDialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	dialect.Dialector.BindVarTo(writer, stmt, v)
}

func (dialect CustomDialector) QuoteTo(writer clause.Writer, s string) {
	dialect.Dialector.QuoteTo(writer, s)
}

func (dialect CustomDialector) Explain(sql string, vars ...interface{}) string {
	return dialect.Dialector.Explain(sql, vars...)
}
