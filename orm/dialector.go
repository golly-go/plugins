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

// ensureDialector ensures that the Dialector is initialized
func (dialect *CustomDialector) ensureDialector() {
	if dialect.Dialector == nil && dialect.DB != nil {
		dialect.Dialector = postgres.New(postgres.Config{Conn: dialect.DB})
	}
}

func (dialect *CustomDialector) Name() string {
	return "postgres"
}

func (dialect *CustomDialector) Initialize(db *gorm.DB) error {
	trace("Dialector.Initialize: start")
	db.ConnPool = dialect.DB
	trace("Dialector.Initialize: set ConnPool")
	dialect.Dialector = postgres.New(postgres.Config{Conn: dialect.DB})
	trace("Dialector.Initialize: created inner postgres dialector")
	err := dialect.Dialector.Initialize(db)
	if err != nil {
		trace("Dialector.Initialize: inner initialize error: %v", err)
		return err
	}
	trace("Dialector.Initialize: end")
	return nil
}

func (dialect *CustomDialector) Migrator(db *gorm.DB) gorm.Migrator {
	dialect.ensureDialector()
	return dialect.Dialector.Migrator(db)
}

func (dialect *CustomDialector) DataTypeOf(field *schema.Field) string {
	dialect.ensureDialector()
	return dialect.Dialector.DataTypeOf(field)
}

func (dialect *CustomDialector) DefaultValueOf(field *schema.Field) clause.Expression {
	dialect.ensureDialector()
	return dialect.Dialector.DefaultValueOf(field)
}

func (dialect *CustomDialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	dialect.ensureDialector()
	dialect.Dialector.BindVarTo(writer, stmt, v)
}

func (dialect *CustomDialector) QuoteTo(writer clause.Writer, s string) {
	dialect.ensureDialector()
	dialect.Dialector.QuoteTo(writer, s)
}

func (dialect *CustomDialector) Explain(sql string, vars ...interface{}) string {
	dialect.ensureDialector()
	return dialect.Dialector.Explain(sql, vars...)
}
