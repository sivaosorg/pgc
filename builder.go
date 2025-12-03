package pgc

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"
)

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Getter Settings
//_______________________________________________________________________

// IsEnabled returns true if the configuration is enabled, indicating that
// a connection to PostgreSQL should be attempted.
func (c *settings) IsEnabled() bool {
	return c.enabled
}

// IsDebugging returns true if debugging is enabled in the configuration,
// which may allow more verbose logging.
func (c *settings) IsDebugging() bool {
	return c.debugging
}

// Host returns the hostname or IP address specified in the configuration.
func (c *settings) Host() string {
	return c.host
}

// Port returns the port number on which the PostgreSQL server is expected to listen.
func (c *settings) Port() int {
	return c.port
}

// User returns the username used for authenticating with the PostgreSQL database.
func (c *settings) User() string {
	return c.user
}

// Database returns the name of the PostgreSQL database to connect to.
func (c *settings) Database() string {
	return c.database
}

// SslMode returns the SSL mode (e.g., disable, require, verify-ca, verify-full) used for the connection.
func (c *settings) SslMode() string {
	return c.sslmode
}

// SslCert returns the path to the SSL client certificate file.
func (c *settings) SslCert() string {
	return c.sslcert
}

// SslKey returns the path to the SSL client key file.
func (c *settings) SslKey() string {
	return c.sslkey
}

// SslRootCert returns the path to the SSL root certificate file used for server certificate verification.
func (c *settings) SslRootCert() string {
	return c.sslrootcert
}

// ConnTimeout returns the maximum duration to wait when establishing a connection.
func (c *settings) ConnTimeout() time.Duration {
	return c.connTimeout
}

// Application returns the application name configured for the PostgreSQL connection.
func (c *settings) Application() string {
	return c.application
}

// MaxOpenConn returns the maximum number of open connections allowed to the database.
func (c *settings) MaxOpenConn() int {
	return c.maxOpenConn
}

// MaxIdleConn returns the maximum number of idle connections maintained in the connection pool.
func (c *settings) MaxIdleConn() int {
	return c.maxIdleConn
}

// ConnMaxLifetime returns the maximum duration a connection may be reused before it is closed.
func (c *settings) ConnMaxLifetime() time.Duration {
	return c.connMaxLifetime
}

// PingInterval returns the interval at which the database connection is pinged.
// This value is used by the keepalive mechanism.
func (c *settings) PingInterval() time.Duration {
	return c.pingInterval
}

// IsSsl returns true if the SSL mode is enabled (i.e., not "disable"), false otherwise.
func (c *settings) IsSsl() bool {
	return !strings.EqualFold(c.sslmode, "disable")
}

// IsConnTimeout returns true if a non-zero connection timeout is specified.
func (c *settings) IsConnTimeout() bool {
	return c.connTimeout != 0
}

// IsPingInterval returns true if keepalive is enabled and a ping interval is specified.
func (c *settings) IsPingInterval() bool {
	return c.keepalive && c.pingInterval != 0
}

// ConnString returns a concise connection string in the format: "user@host:port/database".
// This is mainly used for display or logging purposes.
func (c *settings) ConnString() string {
	if isNotEmpty(c.connectionStrings) {
		return c.connectionStrings
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%s@%s:%d/%s", c.user, c.host, c.port, c.database))
	return builder.String()
}

// String returns the full PostgreSQL connection string with all parameters.
// If safe is true, the password is masked to protect sensitive information.
func (c *settings) String(safe bool) string {
	if isNotEmpty(c.connectionStrings) {
		return c.connectionStrings
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("host=%s ", c.host))
	builder.WriteString(fmt.Sprintf("port=%d ", c.port))
	builder.WriteString(fmt.Sprintf("user=%s ", c.user))
	if safe {
		builder.WriteString(fmt.Sprintf("password=%s ", "******"))
	} else {
		builder.WriteString(fmt.Sprintf("password=%s ", c.password))
	}
	builder.WriteString(fmt.Sprintf("dbname=%s ", c.database))
	builder.WriteString(fmt.Sprintf("sslmode=%s ", c.sslmode))
	if isNotEmpty(c.application) {
		builder.WriteString(fmt.Sprintf("application_name=%s ", c.application))
	}
	if c.IsConnTimeout() {
		builder.WriteString(fmt.Sprintf("connect_timeout=%d ", c.connTimeout))
	}
	if c.IsSsl() {
		if isNotEmpty(c.sslcert) {
			builder.WriteString(fmt.Sprintf("sslcert=%s ", c.sslcert))
		}
		if isNotEmpty(c.sslkey) {
			builder.WriteString(fmt.Sprintf("sslkey=%s ", c.sslkey))
		}
		if isNotEmpty(c.sslrootcert) {
			builder.WriteString(fmt.Sprintf("sslrootcert=%s ", c.sslrootcert))
		}
	}
	if c.optional {
		var subs strings.Builder
		if isNotEmpty(c.schema) {
			subs.WriteString(fmt.Sprintf("search_path=%s ", c.schema))
		}
		// adding new configuration options.
		// final options
		if isNotEmpty(subs.String()) {
			builder.WriteString(fmt.Sprintf("options='-c %s'", subs.String()))
		}
	}
	return builder.String()
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Getter Datasource
//_______________________________________________________________________

// Conn returns the underlying sqlx.DB connection instance in a thread-safe manner.
func (d *Datasource) Conn() *sqlx.DB {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.conn
}

// Wrap returns the current wrapify.R instance, which encapsulates the connection status,
// any error messages, and debugging information in a thread-safe manner.
func (d *Datasource) Wrap() wrapify.R {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.wrap
}

// Conf returns the Settings configuration associated with the Datasource.
func (d *Datasource) Conf() settings {
	return d.conf
}

// IsConnected returns true if the current wrap indicates a successful connection to PostgreSQL,
// otherwise it returns false.
func (d *Datasource) IsConnected() bool {
	return d.Wrap().IsSuccess()
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Setter Settings
//_______________________________________________________________________

// SetEnable sets the enabled flag in the configuration and returns the updated Settings,
// allowing for method chaining.
func (c *settings) SetEnable(value bool) *settings {
	c.enabled = value
	return c
}

// SetDebug sets the debugging flag in the configuration and returns the updated Settings.
func (c *settings) SetDebug(value bool) *settings {
	c.debugging = value
	return c
}

// SetHost sets the hostname or IP address for the database connection and returns the updated Settings.
func (c *settings) SetHost(value string) *settings {
	c.host = value
	return c
}

// SetPort sets the port number for the database connection and returns the updated Settings.
func (c *settings) SetPort(value int) *settings {
	c.port = value
	return c
}

// SetUser sets the username for authenticating with the database and returns the updated Settings.
func (c *settings) SetUser(value string) *settings {
	c.user = value
	return c
}

// SetPassword sets the password for the database connection and returns the updated Settings.
func (c *settings) SetPassword(value string) *settings {
	c.password = value
	return c
}

// SetDatabase sets the target database name for the connection and returns the updated Settings.
func (c *settings) SetDatabase(value string) *settings {
	c.database = value
	return c
}

// SetSslMode sets the SSL mode (as a string) for the connection and returns the updated Settings.
func (c *settings) SetSslMode(value string) *settings {
	c.sslmode = value
	return c
}

// SetSslModeVarious sets the SSL mode using the SslmodeVarious type and returns the updated Settings.
func (c *settings) SetSslModeVarious(value SslmodeVarious) *settings {
	c.sslmode = string(value)
	return c
}

// SetSslCert sets the path to the SSL client certificate and returns the updated Settings.
func (c *settings) SetSslCert(value string) *settings {
	c.sslcert = value
	return c
}

// SetSslKey sets the path to the SSL client key and returns the updated Settings.
func (c *settings) SetSslKey(value string) *settings {
	c.sslkey = value
	return c
}

// SetSslRootCert sets the path to the SSL root certificate and returns the updated Settings.
func (c *settings) SetSslRootCert(value string) *settings {
	c.sslrootcert = value
	return c
}

// SetConnTimeout sets the connection timeout duration and returns the updated Settings.
func (c *settings) SetConnTimeout(value time.Duration) *settings {
	c.connTimeout = value
	return c
}

// SetApplication sets the application name for the connection and returns the updated Settings.
func (c *settings) SetApplication(value string) *settings {
	c.application = value
	return c
}

// SetMaxOpenConn sets the maximum number of open connections and returns the updated Settings.
func (c *settings) SetMaxOpenConn(value int) *settings {
	c.maxOpenConn = value
	return c
}

// SetMaxIdleConn sets the maximum number of idle connections and returns the updated Settings.
func (c *settings) SetMaxIdleConn(value int) *settings {
	c.maxIdleConn = value
	return c
}

// SetConnMaxLifetime sets the maximum lifetime for a connection and returns the updated Settings.
func (c *settings) SetConnMaxLifetime(value time.Duration) *settings {
	c.connMaxLifetime = value
	return c
}

// SetPingInterval sets the interval at which the connection is pinged for keepalive
// and returns the updated Settings.
func (c *settings) SetPingInterval(value time.Duration) *settings {
	c.pingInterval = value
	return c
}

// SetKeepalive enables or disables the automatic keepalive mechanism and returns the updated Settings.
func (c *settings) SetKeepalive(value bool) *settings {
	c.keepalive = value
	return c
}

// SetConnectionStrings updates the connectionStrings field in the Settings structure with the specified value.
// This field stores the complete connection string that aggregates all necessary configuration parameters
// (e.g., host, port, user, password, database, SSL settings, etc.) into a single formatted string recognized
// by the PostgreSQL driver.
//
// Returns:
//   - A pointer to the updated Settings instance to allow method chaining.
func (c *settings) SetConnectionStrings(value string) *settings {
	c.connectionStrings = value
	return c
}

// SetSchema updates the schema field in the Settings structure with the specified value.
// This field determines the PostgreSQL schema to be used by default when connecting to the database.
// By setting the schema, you can direct the connection to use a non-default schema (other than "public")
// for unqualified table references. This is especially useful when your database objects are organized
// under a specific schema and you want to avoid prefixing table names with the schema in your SQL queries.
//
// Returns:
//   - A pointer to the updated Settings instance to allow method chaining.
func (c *settings) SetSchema(value string) *settings {
	c.schema = value
	return c
}

// SetOptions updates the optional field in the Settings structure with the specified value.
// This field determines whether the database connection is considered optional.
// When set to true, the application may tolerate the absence of a database connection,
// allowing it to continue operating even if database-dependent operations are skipped.
// Conversely, a false value implies that a successful connection is mandatory for proper operation.
//
// Returns:
//   - A pointer to the updated Settings instance to allow method chaining.
func (c *settings) SetOptions(value bool) *settings {
	c.optional = value
	return c
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Setter Datasource
//_______________________________________________________________________

// SetConn safely updates the internal sqlx.DB connection of the Datasource and returns
// the updated Datasource for method chaining.
func (d *Datasource) SetConn(value *sqlx.DB) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.conn = value
	return d
}

// SetWrap safely updates the wrapify.R instance (which holds connection status and error info)
// of the Datasource and returns the updated Datasource.
func (d *Datasource) SetWrap(value wrapify.R) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.wrap = value
	return d
}

// OnReconnect sets the callback function that is invoked upon connection state changes (e.g., during keepalive events)
// and returns the updated Datasource for method chaining.
func (d *Datasource) OnReconnect(fnc func(response wrapify.R)) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.on_reconnect = fnc
	return d
}

// OnReconnectChain sets the callback function that is invoked for events specific to replica connections,
// such as replica failovers, reconnection attempts, or health status updates.
// This function accepts a callback that receives both the current status (encapsulated in wrapify.R)
// and a pointer to the Datasource representing the replica connection (replicator), allowing external
// components to implement custom logic for replica management. The updated Datasource instance is returned
// to support method chaining.
func (d *Datasource) OnReconnectChain(fnc func(response wrapify.R, chain *Datasource)) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.on_reconnect_chain = fnc
	return d
}

// OnEvent sets the callback function that is invoked for significant datasource events,
// such as reconnection attempts, keepalive signals, or other diagnostic updates.
// This function stores the provided notifier, which can be used to asynchronously notify
// external components of changes in the connection's status, and returns the updated Datasource instance
// to support method chaining.
func (d *Datasource) OnEvent(fnc func(response wrapify.R)) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.on_event = fnc
	return d
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Getter Transaction
//_______________________________________________________________________

func (t *Transaction) Tx() *sqlx.Tx {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tx
}

func (t *Transaction) Ds() *Datasource {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ds
}

func (t *Transaction) Wrap() wrapify.R {
	return t.wrap
}

func (t *Transaction) IsActivated() bool {
	return t.active
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Getter Event Keys
//_______________________________________________________________________

// String returns the string representation of the EventKey.
func (e EventKey) String() string {
	return string(e)
}

// IsValid checks if the EventKey is not empty.
func (e EventKey) IsValid() bool {
	return isNotEmpty(string(e))
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Bind Configs
//_______________________________________________________________________

// Bind converts a WConf (wrapper configuration loaded from YAML)
// into an Settings (runtime configuration) instance by mapping each field.
func Bind(c *WConf) *settings {
	if c == nil {
		return &settings{}
	}
	conf := &settings{}
	conf.
		SetEnable(c.IsEnabled).
		SetDebug(c.IsDebugging).
		SetHost(c.Host).
		SetPort(c.Port).
		SetUser(c.User).
		SetPassword(c.Password).
		SetDatabase(c.Database).
		SetSslMode(c.SslMode).
		SetConnTimeout(c.ConnTimeout).
		SetApplication(c.Application).
		SetMaxOpenConn(c.MaxOpenConn).
		SetMaxIdleConn(c.MaxIdleConn).
		SetConnMaxLifetime(c.ConnMaxLifetime).
		SetPingInterval(c.PingInterval).
		SetKeepalive(c.KeepAlive).
		SetConnectionStrings(c.ConnectionStrings).
		SetOptions(c.Optional).
		SetSchema(c.Schema)
	return conf
}
