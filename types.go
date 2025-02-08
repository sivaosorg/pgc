package pgc

import (
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"
)

type WConf struct {
}

type RConf struct {
	enabled   bool
	debugging bool

	// The hostname or IP address of the Postgres server (e.g., "127.0.0.1").
	host string

	// The port number on which the Postgres server is listening (e.g., 5432).
	port int

	// The username for authenticating with the database.
	user string

	// The password for the given user.
	password string

	// The name of the database to connect to.
	database string

	// The SSL mode to use (options include disable, require, verify-ca, verify-full). e.g: "sslmode=disable"
	sslmode string

	// Path to the SSL client certificate file (if SSL is enabled).
	sslcert string

	// Path to the SSL client key file (if SSL is enabled).
	sslkey string

	// Path to the SSL root certificate file, used to verify the server's certificate.
	sslrootcert string

	// Maximum wait time (in seconds) for establishing a connection before timing out.
	connTimeout time.Duration

	// An arbitrary name for the application connecting to Postgres, useful for logging and monitoring purposes.
	application string

	// The maximum number of open connections to the database.
	maxOpenConn int

	// The maximum number of connections in the idle connection pool.
	maxIdleConn int

	// The maximum amount of time a connection may be reused.
	connMaxLifetime time.Duration

	// Defines the frequency at which the connection is pinged.
	// This interval is used by the keepalive mechanism to periodically check the health of the
	// database connection. If a ping fails, a reconnection attempt may be triggered.
	pingInterval time.Duration

	// Indicates whether automatic keepalive is enabled for the PostgreSQL connection.
	// When set to true, a background process will periodically ping the database and attempt
	// to reconnect if the connection is lost.
	keepalive bool
}

// SslmodeVarious represents the SSL mode used for connecting to the database.
type SslmodeVarious string

// Datasource encapsulates the PostgreSQL connection and its associated configuration,
// connection status, and event callback mechanism. It provides thread-safe access to its fields
// and supports automatic keepalive and reconnection features.
type Datasource struct {
	// A read-write mutex that ensures safe concurrent access to the Datasource fields.
	mu sync.RWMutex
	// An instance of RConf containing all the configuration parameters for the PostgreSQL connection.
	conf RConf
	// A wrapify.R instance that holds the current connection status, error messages, and debugging information.
	wrap wrapify.R
	// A pointer to an sqlx.DB object representing the active connection to the PostgreSQL database.
	conn *sqlx.DB
	// A callback function that is invoked asynchronously when there is a change in connection status,
	//  such as when the connection is lost, re-established, or its health is updated.
	on func(response wrapify.R)
}
