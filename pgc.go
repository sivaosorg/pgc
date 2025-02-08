package pgc

import (
	"context"
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"

	_ "github.com/lib/pq"
)

// NewClient creates and returns a new Datasource configured for PostgreSQL based on the provided RConf.
// When the configuration is disabled, it returns a Datasource flagged with a service unavailable wrap.
// Otherwise, it attempts to open a connection using the connection string derived from RConf. It then verifies
// connectivity by pinging the database within the configured timeout. On a successful ping, it sets up the
// connection pool with parameters for maximum idle connections, maximum open connections, and connection lifetime.
// Any failure during these steps results in the Datasource being wrapped with an appropriate error message.
func NewClient(conf RConf) *Datasource {
	datasource := &Datasource{
		conf: conf,
	}
	if !conf.IsEnabled() {
		datasource.SetWrap(wrapify.WrapServiceUnavailable("postgresql service unavailable", nil).Reply())
		return datasource
	}
	c, err := sqlx.Open("postgres", conf.String(false))
	if err != nil {
		datasource.SetWrap(
			wrapify.WrapInternalServerError("unable to connect to the postgresql database", nil).
				WithDebuggingKV("pgsql_conn_str", conf.String(true)).
				WithErrSck(err).Reply(),
		)
		return datasource
	}
	ctx, cancel := context.WithTimeout(context.Background(), conf.ConnTimeout())
	defer cancel()
	err = c.PingContext(ctx)
	if err != nil {
		datasource.SetWrap(
			wrapify.WrapInternalServerError("the postgresql database is unreachable", nil).
				WithDebuggingKV("pgsql_conn_str", conf.String(true)).
				WithErrSck(err).Reply(),
		)
		return datasource
	}
	c.SetMaxIdleConns(conf.MaxIdleConn())
	c.SetMaxOpenConns(conf.MaxOpenConn())
	c.SetConnMaxLifetime(conf.ConnMaxLifetime())
	datasource.SetConn(c)
	datasource.SetWrap(wrapify.New().
		WithStatusCode(http.StatusOK).
		WithDebuggingKV("pgsql_conn_str", conf.String(true)).
		WithMessagef("successfully connected to the postgresql database: '%s'", conf.ConnString()).Reply())
	return datasource
}
