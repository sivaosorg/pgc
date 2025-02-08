package pgc

import (
	"context"
	"net/http"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"

	_ "github.com/lib/pq"
)

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
