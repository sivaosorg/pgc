package pgc

import (
	"fmt"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"
)

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Getter RConf
//_______________________________________________________________________

// IsEnabled returns true if the configuration is enabled, indicating that
// a connection to PostgreSQL should be attempted.
func (c *RConf) IsEnabled() bool {
	return c.enabled
}

// IsDebugging returns true if debugging is enabled in the configuration,
// which may allow more verbose logging.
func (c *RConf) IsDebugging() bool {
	return c.debugging
}

// Host returns the hostname or IP address specified in the configuration.
func (c *RConf) Host() string {
	return c.host
}

// Port returns the port number on which the PostgreSQL server is expected to listen.
func (c *RConf) Port() int {
	return c.port
}

// User returns the username used for authenticating with the PostgreSQL database.
func (c *RConf) User() string {
	return c.user
}

// Database returns the name of the PostgreSQL database to connect to.
func (c *RConf) Database() string {
	return c.database
}

// SslMode returns the SSL mode (e.g., disable, require, verify-ca, verify-full) used for the connection.
func (c *RConf) SslMode() string {
	return c.sslmode
}

// SslCert returns the path to the SSL client certificate file.
func (c *RConf) SslCert() string {
	return c.sslcert
}

// SslKey returns the path to the SSL client key file.
func (c *RConf) SslKey() string {
	return c.sslkey
}

// SslRootCert returns the path to the SSL root certificate file used for server certificate verification.
func (c *RConf) SslRootCert() string {
	return c.sslrootcert
}

// ConnTimeout returns the maximum duration to wait when establishing a connection.
func (c *RConf) ConnTimeout() time.Duration {
	return c.connTimeout
}

// Application returns the application name configured for the PostgreSQL connection.
func (c *RConf) Application() string {
	return c.application
}

// MaxOpenConn returns the maximum number of open connections allowed to the database.
func (c *RConf) MaxOpenConn() int {
	return c.maxOpenConn
}

// MaxIdleConn returns the maximum number of idle connections maintained in the connection pool.
func (c *RConf) MaxIdleConn() int {
	return c.maxIdleConn
}

// ConnMaxLifetime returns the maximum duration a connection may be reused before it is closed.
func (c *RConf) ConnMaxLifetime() time.Duration {
	return c.connMaxLifetime
}

// PingInterval returns the interval at which the database connection is pinged.
// This value is used by the keepalive mechanism.
func (c *RConf) PingInterval() time.Duration {
	return c.pingInterval
}

// IsSsl returns true if the SSL mode is enabled (i.e., not "disable"), false otherwise.
func (c *RConf) IsSsl() bool {
	return !strings.EqualFold(c.sslmode, "disable")
}

// IsConnTimeout returns true if a non-zero connection timeout is specified.
func (c *RConf) IsConnTimeout() bool {
	return c.connTimeout != 0
}

// IsPingInterval returns true if keepalive is enabled and a ping interval is specified.
func (c *RConf) IsPingInterval() bool {
	return c.keepalive && c.pingInterval != 0
}

// ConnString returns a concise connection string in the format: "user@host:port/database".
// This is mainly used for display or logging purposes.
func (c *RConf) ConnString() string {
	if isNotEmpty(c.connectionStrings) {
		return c.connectionStrings
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("%s@%s:%d/%s", c.user, c.host, c.port, c.database))
	return builder.String()
}

// String returns the full PostgreSQL connection string with all parameters.
// If safe is true, the password is masked to protect sensitive information.
func (c *RConf) String(safe bool) string {
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

// Conf returns the RConf configuration associated with the Datasource.
func (d *Datasource) Conf() RConf {
	return d.conf
}

// IsConnected returns true if the current wrap indicates a successful connection to PostgreSQL,
// otherwise it returns false.
func (d *Datasource) IsConnected() bool {
	return d.Wrap().IsSuccess()
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Setter RConf
//_______________________________________________________________________

// SetEnable sets the enabled flag in the configuration and returns the updated RConf,
// allowing for method chaining.
func (c *RConf) SetEnable(value bool) *RConf {
	c.enabled = value
	return c
}

// SetDebug sets the debugging flag in the configuration and returns the updated RConf.
func (c *RConf) SetDebug(value bool) *RConf {
	c.debugging = value
	return c
}

// SetHost sets the hostname or IP address for the database connection and returns the updated RConf.
func (c *RConf) SetHost(value string) *RConf {
	c.host = value
	return c
}

// SetPort sets the port number for the database connection and returns the updated RConf.
func (c *RConf) SetPort(value int) *RConf {
	c.port = value
	return c
}

// SetUser sets the username for authenticating with the database and returns the updated RConf.
func (c *RConf) SetUser(value string) *RConf {
	c.user = value
	return c
}

// SetPassword sets the password for the database connection and returns the updated RConf.
func (c *RConf) SetPassword(value string) *RConf {
	c.password = value
	return c
}

// SetDatabase sets the target database name for the connection and returns the updated RConf.
func (c *RConf) SetDatabase(value string) *RConf {
	c.database = value
	return c
}

// SetSslMode sets the SSL mode (as a string) for the connection and returns the updated RConf.
func (c *RConf) SetSslMode(value string) *RConf {
	c.sslmode = value
	return c
}

// SetSslModeVarious sets the SSL mode using the SslmodeVarious type and returns the updated RConf.
func (c *RConf) SetSslModeVarious(value SslmodeVarious) *RConf {
	c.sslmode = string(value)
	return c
}

// SetSslCert sets the path to the SSL client certificate and returns the updated RConf.
func (c *RConf) SetSslCert(value string) *RConf {
	c.sslcert = value
	return c
}

// SetSslKey sets the path to the SSL client key and returns the updated RConf.
func (c *RConf) SetSslKey(value string) *RConf {
	c.sslkey = value
	return c
}

// SetSslRootCert sets the path to the SSL root certificate and returns the updated RConf.
func (c *RConf) SetSslRootCert(value string) *RConf {
	c.sslrootcert = value
	return c
}

// SetConnTimeout sets the connection timeout duration and returns the updated RConf.
func (c *RConf) SetConnTimeout(value time.Duration) *RConf {
	c.connTimeout = value
	return c
}

// SetApplication sets the application name for the connection and returns the updated RConf.
func (c *RConf) SetApplication(value string) *RConf {
	c.application = value
	return c
}

// SetMaxOpenConn sets the maximum number of open connections and returns the updated RConf.
func (c *RConf) SetMaxOpenConn(value int) *RConf {
	c.maxOpenConn = value
	return c
}

// SetMaxIdleConn sets the maximum number of idle connections and returns the updated RConf.
func (c *RConf) SetMaxIdleConn(value int) *RConf {
	c.maxIdleConn = value
	return c
}

// SetConnMaxLifetime sets the maximum lifetime for a connection and returns the updated RConf.
func (c *RConf) SetConnMaxLifetime(value time.Duration) *RConf {
	c.connMaxLifetime = value
	return c
}

// SetPingInterval sets the interval at which the connection is pinged for keepalive
// and returns the updated RConf.
func (c *RConf) SetPingInterval(value time.Duration) *RConf {
	c.pingInterval = value
	return c
}

// SetKeepalive enables or disables the automatic keepalive mechanism and returns the updated RConf.
func (c *RConf) SetKeepalive(value bool) *RConf {
	c.keepalive = value
	return c
}

// SetConnectionStrings updates the connectionStrings field in the RConf structure with the specified value.
// This field stores the complete connection string that aggregates all necessary configuration parameters
// (e.g., host, port, user, password, database, SSL settings, etc.) into a single formatted string recognized
// by the PostgreSQL driver.
//
// Returns:
//   - A pointer to the updated RConf instance to allow method chaining.
func (c *RConf) SetConnectionStrings(value string) *RConf {
	c.connectionStrings = value
	return c
}

// SetSchema updates the schema field in the RConf structure with the specified value.
// This field determines the PostgreSQL schema to be used by default when connecting to the database.
// By setting the schema, you can direct the connection to use a non-default schema (other than "public")
// for unqualified table references. This is especially useful when your database objects are organized
// under a specific schema and you want to avoid prefixing table names with the schema in your SQL queries.
//
// Returns:
//   - A pointer to the updated RConf instance to allow method chaining.
func (c *RConf) SetSchema(value string) *RConf {
	c.schema = value
	return c
}

// SetOptions updates the optional field in the RConf structure with the specified value.
// This field determines whether the database connection is considered optional.
// When set to true, the application may tolerate the absence of a database connection,
// allowing it to continue operating even if database-dependent operations are skipped.
// Conversely, a false value implies that a successful connection is mandatory for proper operation.
//
// Returns:
//   - A pointer to the updated RConf instance to allow method chaining.
func (c *RConf) SetOptions(value bool) *RConf {
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

// SetOn sets the callback function that is invoked upon connection state changes (e.g., during keepalive events)
// and returns the updated Datasource for method chaining.
func (d *Datasource) SetOn(fnc func(response wrapify.R)) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.on = fnc
	return d
}

// SetOnReplica sets the callback function that is invoked for events specific to replica connections,
// such as replica failovers, reconnection attempts, or health status updates.
// This function accepts a callback that receives both the current status (encapsulated in wrapify.R)
// and a pointer to the Datasource representing the replica connection (replicator), allowing external
// components to implement custom logic for replica management. The updated Datasource instance is returned
// to support method chaining.
func (d *Datasource) SetOnReplica(fnc func(response wrapify.R, replicator *Datasource)) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onReplica = fnc
	return d
}

// SetNotifier sets the callback function that is invoked for significant datasource events,
// such as reconnection attempts, keepalive signals, or other diagnostic updates.
// This function stores the provided notifier, which can be used to asynchronously notify
// external components of changes in the connection's status, and returns the updated Datasource instance
// to support method chaining.
func (d *Datasource) SetNotifier(fnc func(response wrapify.R)) *Datasource {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.notifier = fnc
	return d
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Bind Configs
//_______________________________________________________________________

// Bind converts a WConf (wrapper configuration loaded from YAML)
// into an RConf (runtime configuration) instance by mapping each field.
func Bind(c *WConf) *RConf {
	if c == nil {
		return &RConf{}
	}
	conf := &RConf{}
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
