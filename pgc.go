package pgc

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"

	_ "github.com/lib/pq"
)

// NewClient creates and returns a fully configured Datasource instance for PostgreSQL based on
// the provided RConf configuration. This function attempts to establish an initial connection,
// validate connectivity via a ping, and configure connection pool parameters (max idle, max open,
// and connection lifetime). In addition, if keepalive is enabled, it starts a background routine
// to continuously monitor the connection health and trigger reconnection when necessary.
//
// Returns:
//   - A pointer to a Datasource instance that encapsulates the PostgreSQL connection and its configuration.
func NewClient(conf RConf) *Datasource {
	datasource := &Datasource{
		conf: conf,
	}
	if !conf.IsEnabled() {
		datasource.SetWrap(wrapify.WrapServiceUnavailable("Postgresql service unavailable", nil).Reply())
		return datasource
	}

	// Attempt to open a connection to PostgreSQL using the provided connection string.
	c, err := sqlx.Open("postgres", conf.String(false))
	if err != nil {
		datasource.SetWrap(
			wrapify.WrapInternalServerError("Unable to connect to the postgresql database", nil).
				WithDebuggingKV("pgsql_conn_str", conf.String(true)).
				WithErrSck(err).Reply(),
		)
		return datasource
	}

	// Use a context with timeout to verify the connection via PingContext.
	ctx, cancel := context.WithTimeout(context.Background(), conf.ConnTimeout())
	defer cancel()
	err = c.PingContext(ctx)
	if err != nil {
		datasource.SetWrap(
			wrapify.WrapInternalServerError("The postgresql database is unreachable", nil).
				WithDebuggingKV("pgsql_conn_str", conf.String(true)).
				WithErrSck(err).Reply(),
		)
		return datasource
	}
	// Configure the connection pool based on the provided configuration.
	c.SetMaxIdleConns(conf.MaxIdleConn())
	c.SetMaxOpenConns(conf.MaxOpenConn())
	c.SetConnMaxLifetime(conf.ConnMaxLifetime())

	// Set the established connection and update the wrap response to indicate success.
	datasource.SetConn(c)
	datasource.SetWrap(wrapify.New().
		WithStatusCode(http.StatusOK).
		WithDebuggingKV("pgsql_conn_str", conf.String(true)).
		WithMessagef("Successfully connected to the postgresql database: '%s'", conf.ConnString()).Reply())

	// If keepalive is enabled, initiate the background routine to monitor connection health.
	if conf.keepalive {
		datasource.keepalive()
	}
	return datasource
}

// keepalive initiates a background goroutine that periodically pings the PostgreSQL database
// to monitor connection health. Upon detecting a failure in the ping, it attempts to reconnect
// and subsequently invokes a callback (if set) with the updated connection status. This mechanism
// ensures that the Datasource remains current with respect to the connection state.
//
// The ping interval is determined by the configuration's PingInterval; if it is not properly set,
// a default interval is used.
func (d *Datasource) keepalive() {
	interval := d.conf.PingInterval()
	if interval <= 0 {
		interval = defaultPingInterval
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			if err := d.ping(); err != nil {
				pingWrapper := wrapify.WrapInternalServerError("The postgresql database is currently unreachable. Initiating reconnection process...", nil).
					WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
					WithErrSck(err).Reply()
				d.SetWrap(pingWrapper)
				d.invoke(pingWrapper)

				if err := d.reconnect(); err != nil {
					reconnectWrapper := wrapify.WrapInternalServerError("The postgresql database remains unreachable. The reconnection attempt has failed", nil).
						WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
						WithErrSck(err).Reply()
					d.SetWrap(reconnectWrapper)
					d.invoke(reconnectWrapper)
				} else {
					successWrapper := wrapify.New().
						WithStatusCode(http.StatusOK).
						WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
						WithMessagef("The connection to the postgresql database has been successfully re-established: '%s'", d.conf.ConnString()).Reply()
					d.SetWrap(successWrapper)
					d.invoke(successWrapper)
				}
			} else {
				successWrapper := wrapify.New().
					WithStatusCode(http.StatusOK).
					WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
					WithMessagef("The connection to the postgresql database has been successfully re-established: '%s'", d.conf.ConnString()).Reply()
				d.SetWrap(successWrapper)
				d.invoke(successWrapper)
			}
		}
	}()
}

// ping performs a health check on the current PostgreSQL connection by issuing a PingContext
// request within the constraints of a timeout. It returns an error if the connection is nil
// or if the ping operation fails.
//
// Returns:
//   - nil if the connection is healthy;
//   - an error if the connection is nil or the ping fails.
func (d *Datasource) ping() error {
	d.mu.RLock()
	conn := d.conn
	d.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("the postgresql connection is currently unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.conf.ConnTimeout())
	defer cancel()
	return conn.PingContext(ctx)
}

// reconnect attempts to establish a new connection to the PostgreSQL database using the current configuration.
// If the new connection is successfully verified via PingContext, it replaces the existing connection in the Datasource.
// In the event that a previous connection exists, it is closed to release associated resources.
//
// Returns:
//   - nil if reconnection is successful;
//   - an error if the reconnection fails at any stage.
func (d *Datasource) reconnect() error {
	current, err := sqlx.Open("postgres", d.conf.String(false))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), d.conf.ConnTimeout())
	defer cancel()
	if err := current.PingContext(ctx); err != nil {
		current.Close()
		return err
	}
	current.SetMaxIdleConns(d.conf.MaxIdleConn())
	current.SetMaxOpenConns(d.conf.MaxOpenConn())
	current.SetConnMaxLifetime(d.conf.ConnMaxLifetime())

	d.mu.Lock()
	previous := d.conn
	d.conn = current
	d.mu.Unlock()
	if previous != nil {
		previous.Close()
	}
	return nil
}

// invoke safely retrieves the registered callback function and, if one is set,
// invokes it asynchronously with the provided wrapify.R response. This ensures that
// external consumers are notified of connection status changes without blocking the
// calling goroutine.
func (d *Datasource) invoke(response wrapify.R) {
	d.mu.RLock()
	callback := d.on
	d.mu.RUnlock()
	if callback != nil {
		go callback(response)
	}
}
