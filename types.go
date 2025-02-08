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
}

// SslmodeVarious represents the SSL mode used for connecting to the database.
type SslmodeVarious string

type Datasource struct {
	mu   sync.RWMutex
	conf RConf
	wrap wrapify.R
	conn *sqlx.DB
}
