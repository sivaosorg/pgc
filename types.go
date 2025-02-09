package pgc

import (
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"
	"gopkg.in/guregu/null.v3"
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

	// connectionStrings holds the generated connection string used to establish a connection
	// to the PostgreSQL database. This string typically combines all the configuration parameters
	// (such as host, port, user, password, database, SSL settings, etc.) into a formatted string
	// that is recognized by the PostgreSQL driver.
	connectionStrings string

	// schema specifies the PostgreSQL schema to use by default for this connection.
	// When the connection is established, this schema is typically set in the search_path,
	// so that any unqualified table references will resolve to tables within this schema
	// rather than the default "public" schema.
	schema string

	// optional indicates whether the database connection is considered optional.
	// When set to true, the application may tolerate the absence of a database connection
	// (for example, proceeding without performing database-dependent operations),
	// whereas a value of false implies that a successful connection is mandatory.
	optional bool
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

// FuncMetadata represents the metadata for a function parameter retrieved from the PostgreSQL database.
//
// Fields:
//   - DataType:    The data type of the function parameter.
//   - RoutineName: The name of the function (routine) to which the parameter belongs.
//   - ParamName:   The name of the parameter.
//   - ParamMode:   The mode of the parameter (e.g., IN, OUT, INOUT).
type FuncMetadata struct {
	DataType    string `db:"data_type" json:"type,omitempty"`
	RoutineName string `db:"routine_name" json:"routine_name,omitempty"`
	ParamName   string `db:"parameter_name" json:"param_name,omitempty"`
	ParamMode   string `db:"parameter_mode" json:"param_mode,omitempty"`
}

// TableMetadata represents a single metadata record for a table in the PostgreSQL database.
//
// Fields:
//   - Name: The name of the constraint or index.
//   - Type: The type of metadata (e.g., "Primary Key", "Unique Key", or "Index").
//   - Desc: Additional details, such as the index definition, if applicable.
type TableMetadata struct {
	Name string `json:"name,omitempty" db:"c_name"`
	Type string `json:"type,omitempty" db:"type"`
	Desc string `json:"desc,omitempty" db:"descriptor"`
}

// ColumnMetadata represents metadata information for a column in a PostgreSQL table.
//
// Fields:
//   - Column:    The name of the column.
//   - Type:      The data type of the column.
//   - MaxLength: The maximum character length allowed for the column (if applicable).
type ColumnMetadata struct {
	Column    string   `json:"column" db:"column_name"`
	Type      string   `json:"type" db:"data_type"`
	MaxLength null.Int `json:"max_length" db:"character_maximum_length"`
}
