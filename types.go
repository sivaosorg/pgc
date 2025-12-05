package pgc

import (
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/sivaosorg/wrapify"
	"gopkg.in/guregu/null.v3"
)

// WConf represents the configuration for the PostgreSQL connection.
//
// Fields:
//   - IsEnabled:         Indicates whether the Postgres connection is enabled.
//   - IsDebugging:       Indicates whether debugging mode is enabled.
//   - Host:              The hostname or IP address of the Postgres server.
//   - Port:              The port number on which the Postgres server listens.
//   - User:              The username used to authenticate with the Postgres server.
//   - Password:          The password corresponding to the specified user.
//   - Database:          The name of the database to connect to.
//   - SslMode:           The SSL mode for the connection (e.g., "disable", "require", "verify-ca", "verify-full").
//   - ConnTimeout:       The duration to wait before timing out a connection attempt (e.g., "30s", "1m").
//   - Application:       The name of the application connecting to the database (useful for logging or monitoring).
//   - MaxOpenConn:       The maximum number of open connections allowed in the connection pool.
//   - MaxIdleConn:       The maximum number of idle connections maintained in the pool.
//   - ConnMaxLifetime:   The maximum lifetime of a connection before it is recycled (e.g., "1h", "30m").
//   - PingInterval:      The interval between health-check pings to the database.
//   - KeepAlive:         Enables TCP keepalive to maintain persistent connections.
//   - ConnectionStrings: Full connection string example; alternative to specifying individual connection parameters.
//   - Optional:          Set to true if the connection is optional (won't cause the application to fail if unavailable).
//   - Schema:            Default database schema to use.
type WConf struct {
	IsEnabled         bool          `yaml:"enabled"`            // Enables or disables the Postgres connection.
	IsDebugging       bool          `yaml:"debugging"`          // Turns on/off debugging mode for more verbose logging.
	Host              string        `yaml:"host"`               // Hostname or IP address of the Postgres server.
	Port              int           `yaml:"port"`               // Port number on which the Postgres server listens.
	User              string        `yaml:"user"`               // Username used to authenticate with the Postgres server.
	Password          string        `yaml:"password"`           // Password corresponding to the specified user.
	Database          string        `yaml:"database"`           // Name of the database to connect to.
	SslMode           string        `yaml:"ssl_mode"`           // SSL mode for the connection (e.g., "disable", "require", "verify-ca", "verify-full").
	ConnTimeout       time.Duration `yaml:"conn_timeout"`       // Duration to wait before timing out a connection attempt (e.g., "30s", "1m").
	Application       string        `yaml:"application"`        // Name of the application connecting to the database (useful for logging or monitoring).
	MaxOpenConn       int           `yaml:"max_open_conn"`      // Maximum number of open connections allowed in the connection pool.
	MaxIdleConn       int           `yaml:"max_idle_conn"`      // Maximum number of idle connections maintained in the pool.
	ConnMaxLifetime   time.Duration `yaml:"conn_max_lifetime"`  // Maximum lifetime of a connection before it is recycled (e.g., "1h", "30m").
	PingInterval      time.Duration `yaml:"ping_interval"`      // Interval between health-check pings to the database.
	KeepAlive         bool          `yaml:"keep_alive"`         // Enables TCP keepalive to maintain persistent connections.
	ConnectionStrings string        `yaml:"connection_strings"` // Full connection string example; alternative to specifying individual connection parameters.
	Optional          bool          `yaml:"optional"`           // Set to true if the connection is optional (won't cause the application to fail if unavailable).
	Schema            string        `yaml:"schema"`             // Default database schema to use.
}

// settings represents the runtime configuration for the PostgreSQL connection.
//
// Fields:
//   - Enabled:         Indicates whether the Postgres connection is enabled.
//   - Debugging:       Indicates whether debugging mode is enabled.
//   - Host:              The hostname or IP address of the Postgres server.
//   - Port:              The port number on which the Postgres server listens.
//   - User:              The username used to authenticate with the Postgres server.
//   - Password:          The password corresponding to the specified user.
//   - Database:          The name of the database to connect to.
//   - SslMode:           The SSL mode for the connection (e.g., "disable", "require", "verify-ca", "verify-full").
//   - ConnTimeout:       The duration to wait before timing out a connection attempt (e.g., "30s", "1m").
//   - Application:       The name of the application connecting to the database (useful for logging or monitoring).
//   - MaxOpenConn:       The maximum number of open connections allowed in the connection pool.
//   - MaxIdleConn:       The maximum number of idle connections maintained in the pool.
//   - ConnMaxLifetime:   The maximum lifetime of a connection before it is recycled (e.g., "1h", "30m").
//   - PingInterval:      The interval between health-check pings to the database.
//   - KeepAlive:         Enables TCP keepalive to maintain persistent connections.
//   - ConnectionStrings: Full connection string example; alternative to specifying individual connection parameters.
//   - Optional:          Set to true if the connection is optional (won't cause the application to fail if unavailable).
//   - Schema:            Default database schema to use.
type settings struct {
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

// EventKey represents a unique identifier for events dispatched by the Datasource.
// It is used to classify and identify specific operations or notifications
// when the dispatch_event callback is invoked.
type EventKey string

// EventLevel represents the severity level of an event.
// It is used to indicate the importance or type of the event being dispatched.
type EventLevel string

// Datasource encapsulates the PostgreSQL connection and its associated configuration,
// connection status, and event callback mechanism. It provides thread-safe access to its fields
// and supports automatic keepalive and reconnection features.
type Datasource struct {
	// A read-write mutex that ensures safe concurrent access to the Datasource fields.
	mu sync.RWMutex

	// An instance of Settings containing all the configuration parameters for the PostgreSQL connection.
	conf settings

	// A wrapify.R instance that holds the current connection status, error messages, and debugging information.
	state wrapify.R

	// A pointer to an sqlx.DB object representing the active connection to the PostgreSQL database.
	conn *sqlx.DB

	// inspector is an optional QueryInspector that receives inspection information for each executed query.
	// When set, it allows external components to log, monitor, or analyze SQL queries.
	inspector QueryInspector

	// inspectEnabled indicates whether query inspection is enabled.
	// When true, all executed queries will be inspected and sent to the inspector callback.
	inspectEnabled bool

	// lastInspect holds the most recent query inspection for debugging purposes.
	lastInspect *QueryInspect

	// A callback function that is invoked asynchronously when there is a change in connection status,
	//  such as when the connection is lost, re-established, or its health is updated.
	on_reconnect func(response wrapify.R)

	// on_reconnect_chain is a callback function that is invoked asynchronously to handle events related to replica connections.
	// When the status of a replica datasource changes (e.g., during failover, reconnection, or health updates),
	// this function is triggered with the current status (encapsulated in wrapify.R) and a pointer to the Datasource
	// representing the replica connection. This allows external components to implement replica-specific logic
	// for tasks such as load balancing, monitoring, or failover handling independently of the primary connection.
	on_reconnect_chain func(response wrapify.R, chain *Datasource)

	// on_event is an optional callback function used to propagate notifications for significant datasource events.
	// It is invoked with the current status (encapsulated in wrapify.R) whenever notable events occur,
	// such as reconnection attempts, keepalive signals, or other diagnostic updates.
	// This allows external components to receive and handle these notifications independently of the primary connection status callback.
	on_event func(event EventKey, level EventLevel, response wrapify.R)
}

type Transaction struct {
	// A read-write mutex that ensures safe concurrent access to the Datasource fields.
	mu sync.RWMutex

	// tx is the underlying *sqlx.Tx object that represents the active PostgreSQL transaction.
	// It provides the core functionality for executing SQL statements within the transaction boundaries,
	// ensuring atomicity and isolation as managed by the database. This field is used by methods like
	// Commit, Rollback, and Save.point to interact with the database transaction.
	tx *sqlx.Tx

	// ds is a reference to the parent Datasource instance that initiated this transaction.
	// It links the transaction back to the connection pool and configuration, allowing access to
	// the Datasource's methods (e.g., notify) and state (e.g., connection health). This ensures that
	// transaction operations can interact with the broader connection management system, such as
	// logging or notifying external callbacks about transaction status changes.
	ds *Datasource

	// wrap holds the wrapify.R response object that encapsulates the current status or result
	// of the transaction operations (e.g., begin, commit, rollback, save.point actions). It provides
	// a consistent way to report success or failure, including detailed error messages, debugging
	// information, and HTTP-like status codes, aligning with the error-handling approach used
	// throughout the Datasource implementation. This field is updated by each transaction method
	// to reflect the latest outcome.
	wrap wrapify.R

	// active is a boolean flag indicating whether the transaction is currently active and usable.
	// It is set to true when the transaction begins and remains true until either Commit or Rollback
	// is successfully called, at which point it is set to false. This flag prevents operations on
	// a completed or aborted transaction, ensuring that methods like Commit, Rollback, or Save.point
	// are only executed on a valid, ongoing transaction, thus maintaining consistency and preventing
	// misuse.
	active bool
}

// FuncsSpec represents the metadata for a function parameter retrieved from the PostgreSQL database.
//
// Fields:
//   - DataType:    The data type of the function parameter.
//   - RoutineName: The name of the function (routine) to which the parameter belongs.
//   - ParamName:   The name of the parameter.
//   - ParamMode:   The mode of the parameter (e.g., IN, OUT, INOUT).
type FuncsSpec struct {
	DataType    string `db:"data_type" json:"type,omitempty"`
	RoutineName string `db:"routine_name" json:"routine_name,omitempty"`
	ParamName   string `db:"parameter_name" json:"param_name,omitempty"`
	ParamMode   string `db:"parameter_mode" json:"param_mode,omitempty"`
}

// TableKeysDef represents a single metadata record for a table in the PostgreSQL database.
//
// Fields:
//   - Name: The name of the constraint or index.
//   - Type: The type of metadata (e.g., "Primary Key", "Unique Key", or "Index").
//   - Desc: Additional details, such as the index definition, if applicable.
type TableKeysDef struct {
	Name string `json:"name,omitempty" db:"c_name"`
	Type string `json:"type,omitempty" db:"type"`
	Desc string `json:"desc,omitempty" db:"descriptor"`
}

// ColsSpec represents metadata information for a column in a PostgreSQL table.
//
// Fields:
//   - Column:    The name of the column.
//   - Type:      The data type of the column.
//   - MaxLength: The maximum character length allowed for the column (if applicable).
type ColsSpec struct {
	Column    string   `json:"column" db:"column_name"`
	Type      string   `json:"type" db:"data_type"`
	MaxLength null.Int `json:"max_length" db:"character_maximum_length"`
}

// ColsDef represents the result of checking if a column exists in a specific table.
//
// Fields:
//   - TableName:  The name of the table containing the column.
//   - SchemaName: The schema name where the table resides.
//   - ColumnName: The name of the column being checked.
//   - DataType:   The data type of the column.
//   - IsNullable: Indicates whether the column allows NULL values.
type ColsDef struct {
	TableName  string `json:"table_name" db:"table_name"`
	SchemaName string `json:"schema_name" db:"table_schema"`
	ColumnName string `json:"column_name" db:"column_name"`
	DataType   string `json:"data_type" db:"data_type"`
	IsNullable string `json:"is_nullable" db:"is_nullable"`
}

// TableColsSpec represents a table that contains specified columns.
//
// Fields:
//   - TableName:   The name of the table.
//   - SchemaName:  The schema name where the table resides.
//   - MatchedColumns: List of columns that were found in this table.
//   - TotalColumns:   Total number of columns requested.
//   - MatchedCount:   Number of columns that matched.
type TableColsSpec struct {
	TableName      string   `json:"table_name" db:"table_name"`
	SchemaName     string   `json:"schema_name" db:"table_schema"`
	MatchedColumns []string `json:"matched_columns"`
	TotalColumns   int      `json:"total_columns"`
	MatchedCount   int      `json:"matched_count"`
}

// TablesByColsPlus searches for tables and returns detailed information about column matches.
//
// This function provides comprehensive information including which columns were found,
// which were missing, and detailed metadata for each matched column.
//
// Parameters:
//   - columns: A slice of column names to search for.
//
// Returns:
//   - A wrapify. R instance containing detailed matching information.
type TableColsSpecMeta struct {
	TableName      string    `json:"table_name"`
	SchemaName     string    `json:"schema_name"`
	MatchedColumns []ColsDef `json:"matched_columns"`
	MissingColumns []string  `json:"missing_columns"`
	TotalRequested int       `json:"total_requested"`
	MatchedCount   int       `json:"matched_count"`
	IsFullMatch    bool      `json:"is_full_match"`
}

// PrivsDef represents a single privilege grant for a table in PostgreSQL.
//
// Fields:
//   - TableName:     The name of the table.
//   - PrivilegeType: The type of privilege (e.g., SELECT, INSERT, UPDATE, DELETE).
//   - Grantee:       The user or role that has been granted the privilege.
type PrivsDef struct {
	TableName     string `json:"table_name" db:"table_name"`
	PrivilegeType string `json:"privilege_type" db:"privilege_type"`
	Grantee       string `json:"grantee" db:"grantee"`
}

// TablePrivsSpec provides statistics about privilege checks across tables.
//
// Fields:
//   - TablesWithPrivileges:    Tables that have at least one of the requested privileges.
//   - TablesWithoutPrivilege: Tables that have none of the requested privileges.
//   - TotalRequested:     Total number of tables requested to check.
//   - TotalWithPrivilege:     Count of tables with at least one privilege.
//   - TotalWithoutPrivilege:  Count of tables without any of the requested privileges.
type TablePrivsSpec struct {
	TablesWithPrivileges   []string `json:"tables_with_privileges"`
	TablesWithoutPrivilege []string `json:"tables_without_privileges"`
	TotalRequested         int      `json:"total_requested"`
	TotalWithPrivilege     int      `json:"total_with_privileges"`
	TotalWithoutPrivilege  int      `json:"total_without_privileges"`
}

// TablePrivsSpecMeta holds the complete result of a privilege check operation,
// including detailed privilege grants and summary statistics.
//
// Fields:
//   - Privileges: All the privilege grants found.
//   - Stats: Summary statistics about the privilege check.
type TablePrivsSpecMeta struct {
	Privileges []PrivsDef     `json:"privileges"`
	Stats      TablePrivsSpec `json:"stats"`
}

// ColExistsDef represents the existence status of a column in a specific table.
//
// Fields:
//   - TableName:  The name of the table being checked.
//   - ColumnName: The name of the column being checked.
//   - Exists:     Whether the column exists in the table.
//   - Status:     Human-readable status ("Exists" or "Does not exist").
type ColExistsDef struct {
	TableName  string `json:"table_name" db:"table_name"`
	ColumnName string `json:"column_name" db:"column_name"`
	Exists     bool   `json:"exists"`
	Status     string `json:"status" db:"status"`
}

// ColExistsSpec provides statistics about column existence checks across tables.
//
// Fields:
//   - ExistingCols:    List of table-column pairs that exist.
//   - MissingCols:     List of table-column pairs that do not exist.
//   - TotalChecked:    Total number of table-column combinations checked.
//   - TotalExisting:   Count of existing columns.
//   - TotalMissing:    Count of missing columns.
type ColExistsSpec struct {
	ExistingCols  []ColExistsDef `json:"existing_cols"`
	MissingCols   []ColExistsDef `json:"missing_cols"`
	TotalChecked  int            `json:"total_checked"`
	TotalExisting int            `json:"total_existing"`
	TotalMissing  int            `json:"total_missing"`
}

// ColExistsSpecMeta holds the complete result of a column existence check operation,
// including all check results and summary statistics.
//
// Fields:
//   - Results: All column existence check results.
//   - Stats:   Summary statistics about the existence check.
type ColExistsSpecMeta struct {
	Cols  []ColExistsDef `json:"cols"`
	Stats ColExistsSpec  `json:"stats"`
}

// QueryInspect represents the inspection information of an executed SQL query.
//
// Fields:
//   - Query:       The raw SQL query string with placeholders.
//   - Args:        The arguments passed to the query.
//   - Completed:   The fully interpolated SQL query with arguments replaced.
//   - ExecutedAt:  The timestamp when the query was executed.
//   - Duration:    The duration of the query execution.
//   - FuncName:    The name of the function that executed the query.
type QueryInspect struct {
	Query      string        `json:"query"`
	Args       []any         `json:"args,omitempty"`
	Completed  string        `json:"completed"`
	ExecutedAt time.Time     `json:"executed_at"`
	Duration   time.Duration `json:"duration,omitempty"`
	FuncName   string        `json:"func_name,omitempty"`
}

// QueryInspector is an interface for inspecting SQL queries.
// Implementations can log, store, or process query inspections as needed.
type QueryInspector interface {
	Inspect(ins QueryInspect)
}

// QueryInspectorFunc is a function adapter that implements QueryInspector.
type QueryInspectorFunc func(ins QueryInspect)
