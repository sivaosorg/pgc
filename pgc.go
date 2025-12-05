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

// NewSettings initializes and returns a pointer to a new settings instance
// with default values pre-configured for PostgreSQL connections.
func NewSettings() *settings {
	return &settings{}
}

// NewClient creates and returns a fully configured Datasource instance for PostgreSQL based on
// the provided Settings configuration. This function attempts to establish an initial connection,
// validate connectivity via a ping, and configure connection pool parameters (max idle, max open,
// and connection lifetime). In addition, if keepalive is enabled, it starts a background routine
// to continuously monitor the connection health and trigger reconnection when necessary.
//
// Returns:
//   - A pointer to a Datasource instance that encapsulates the PostgreSQL connection and its configuration.
func NewClient(conf settings) *Datasource {
	datasource := &Datasource{
		conf: conf,
	}
	start := time.Now()
	if !conf.IsEnabled() {
		datasource.SetState(wrapify.
			WrapServiceUnavailable("Postgresql service unavailable", nil).
			WithDebuggingKV("executed_in", time.Since(start).String()).
			WithHeader(wrapify.ServiceUnavailable).
			Reply())
		return datasource
	}

	// Attempt to open a connection to PostgreSQL using the provided connection string.
	c, err := sqlx.Open("postgres", conf.String(false))
	if err != nil {
		datasource.SetState(
			wrapify.WrapInternalServerError("Unable to connect to the postgresql database", nil).
				WithDebuggingKV("pgsql_conn_str", conf.String(true)).
				WithDebuggingKV("executed_in", time.Since(start).String()).
				WithHeader(wrapify.InternalServerError).
				WithErrSck(err).
				Reply(),
		)
		return datasource
	}

	// Use a context with timeout to verify the connection via PingContext.
	ctx, cancel := context.WithTimeout(context.Background(), conf.ConnTimeout())
	defer cancel()
	err = c.PingContext(ctx)
	if err != nil {
		datasource.SetState(
			wrapify.WrapInternalServerError("The postgresql database is unreachable", nil).
				WithDebuggingKV("pgsql_conn_str", conf.String(true)).
				WithDebuggingKV("executed_in", time.Since(start).String()).
				WithHeader(wrapify.InternalServerError).
				WithErrSck(err).
				Reply(),
		)
		return datasource
	}
	// Configure the connection pool based on the provided configuration.
	c.SetMaxIdleConns(conf.MaxIdleConn())
	c.SetMaxOpenConns(conf.MaxOpenConn())
	c.SetConnMaxLifetime(conf.ConnMaxLifetime())

	// Set the established connection and update the wrap response to indicate success.
	datasource.SetConn(c)
	datasource.SetState(wrapify.New().
		WithStatusCode(http.StatusOK).
		WithDebuggingKV("pgsql_conn_str", conf.String(true)).
		WithDebuggingKV("executed_in", time.Since(start).String()).
		WithMessagef("Successfully connected to the postgresql database: '%s'", conf.ConnString()).
		WithHeader(wrapify.OK).
		Reply())

	// If keepalive is enabled, initiate the background routine to monitor connection health.
	if conf.keepalive {
		datasource.keepalive()
	}
	return datasource
}

// BeginTx starts a new database transaction within the context of the Datasource.
// If the Datasource is not connected, it returns a Transaction instance with an appropriate
// error response. Otherwise, it attempts to begin a transaction using the underlying
// sqlx connection and returns a Transaction instance representing the active transaction.
func (d *Datasource) BeginTx(ctx context.Context) *Transaction {
	if !d.IsConnected() {
		response := wrapify.WrapServiceUnavailable("Datasource is not connected", nil).BindCause().WithHeader(wrapify.ServiceUnavailable).Reply()
		d.dispatch_event(EventConnClose, EventLevelError, response)
		t := &Transaction{
			ds:     d,
			tx:     nil,
			active: false,
			wrap:   response,
		}
		return t
	}

	d.dispatch_event(EventTxBegin, EventLevelInfo, wrapify.WrapProcessing("Starting transaction", nil).WithHeader(wrapify.Processing).Reply())

	tx, err := d.Conn().BeginTxx(ctx, nil)
	if err != nil {
		response := wrapify.WrapInternalServerError("Failed to start transaction", nil).WithHeader(wrapify.InternalServerError).WithErrSck(err).Reply()
		d.dispatch_event(EventTxStartedAbort, EventLevelError, response)
		t := &Transaction{
			ds:     d,
			tx:     nil,
			active: false,
			wrap:   response,
		}
		return t
	}
	response := wrapify.WrapOk("Transaction started successfully", nil).WithHeader(wrapify.OK).Reply()
	t := &Transaction{
		ds:     d,
		tx:     tx,
		active: true,
		wrap:   response,
	}
	d.dispatch_event(EventTxStarted, EventLevelSuccess, response)
	return t
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
	var response wrapify.R
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		reconnectAttempt := 0 // Initialize reconnect attempt count
		for range ticker.C {
			ps := time.Now()
			if err := d.ping(); err != nil {
				duration := time.Since(ps)
				response = wrapify.WrapInternalServerError("The postgresql database is currently unreachable. Initiating reconnection process...", nil).
					WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
					WithDebuggingKV("ping_executed_in", duration.String()).
					WithDebuggingKV("ping_start_at", ps.Format(defaultTimeFormat)).
					WithDebuggingKV("ping_end_at", ps.Add(duration).Format(defaultTimeFormat)).
					WithErrSck(err).
					WithHeader(wrapify.InternalServerError).
					Reply()

				ps = time.Now()
				if err := d.reconnect(); err != nil {
					duration := time.Since(ps)
					reconnectAttempt++ // Increment reconnect count on failure reconnect
					response = wrapify.WrapInternalServerError("The postgresql database remains unreachable. The reconnection attempt has failed", nil).
						WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
						WithDebuggingKV("reconnect_executed_in", duration.String()).
						WithDebuggingKV("reconnect_start_at", ps.Format(defaultTimeFormat)).
						WithDebuggingKV("reconnect_end_at", ps.Add(duration).Format(defaultTimeFormat)).
						WithDebuggingKV("reconnect_attempt", reconnectAttempt).
						WithErrSck(err).
						WithHeader(wrapify.InternalServerError).
						Reply()
				} else {
					duration := time.Since(ps)
					reconnectAttempt = 0
					response = wrapify.New().
						WithStatusCode(http.StatusOK).
						WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
						WithDebuggingKV("reconnect_executed_in", duration.String()).
						WithDebuggingKV("reconnect_start_at", ps.Format(defaultTimeFormat)).
						WithDebuggingKV("reconnect_end_at", ps.Add(duration).Format(defaultTimeFormat)).
						WithMessagef("The connection to the postgresql database has been successfully re-established: '%s'", d.conf.ConnString()).
						WithHeader(wrapify.OK).
						Reply()
				}
			} else {
				duration := time.Since(ps)
				reconnectAttempt = 0
				response = wrapify.New().
					WithStatusCode(http.StatusOK).
					WithDebuggingKV("pgsql_conn_str", d.conf.String(true)).
					WithDebuggingKV("ping_executed_in", duration.String()).
					WithDebuggingKV("ping_start_at", ps.Format(defaultTimeFormat)).
					WithDebuggingKV("ping_end_at", ps.Add(duration).Format(defaultTimeFormat)).
					WithMessagef("The connection to the postgresql database has been successfully established: '%s'", d.conf.ConnString()).
					WithHeader(wrapify.OK).
					Reply()
			}
			d.SetState(response)
			d.dispatch_reconnect(response)
			d.dispatch_reconnect_chain(response, d)
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

// dispatch_reconnect safely retrieves the registered callback function and, if one is set,
// invokes it asynchronously with the provided wrapify.R response. This ensures that
// external consumers are notified of connection status changes without blocking the
// calling goroutine.
func (d *Datasource) dispatch_reconnect(response wrapify.R) {
	d.mu.RLock()
	callback := d.on_reconnect
	d.mu.RUnlock()
	if callback != nil {
		go callback(response)
	}
}

// dispatch_reconnect_chain safely retrieves the registered replica callback function and, if one is set,
// invokes it asynchronously with the provided wrapify.R response and a pointer to the replica Datasource.
// This ensures that external consumers are notified of replica-specific connection status changes,
// such as replica failovers, reconnection attempts, or health updates, without blocking the calling goroutine.
func (d *Datasource) dispatch_reconnect_chain(response wrapify.R, chain *Datasource) {
	d.mu.RLock()
	callback := d.on_reconnect_chain
	d.mu.RUnlock()
	if callback != nil {
		go callback(response, chain)
	}
}

// dispatch_event safely retrieves the registered notifier callback function and, if one is set,
// invokes it asynchronously with the provided wrapify.R response. This method allows the Datasource
// to dispatch_event external components of significant events (e.g., transaction starts, commits, rollbacks)
// without blocking the calling goroutine, ensuring that notification handling is performed concurrently.
func (d *Datasource) dispatch_event(event EventKey, level EventLevel, response wrapify.R) {
	d.mu.RLock()
	callback := d.on_event
	d.mu.RUnlock()
	if callback != nil {
		go callback(event, level, response)
	}
}
