package pgc

import "time"

const (
	// SslmodeDisable disables SSL.
	// In this mode, SSL is not used at all, and the connection is established in plain text.
	SslmodeDisable SslmodeVarious = "disable"

	// SslmodeRequire requires SSL.
	// In this mode, the connection is encrypted using SSL, but the server's certificate is not verified.
	SslmodeRequire SslmodeVarious = "require"

	// SslmodeVerifyCa enables SSL and verifies the certificate authority (CA).
	// In this mode, the connection is encrypted, and the server's certificate is verified to be signed by a trusted CA.
	// However, the host name in the certificate is not checked.
	SslmodeVerifyCa SslmodeVarious = "verify-ca"

	// SslmodeVerifyFull enables full SSL verification.
	// In this mode, the connection is encrypted, the server's certificate is verified to be signed by a trusted CA,
	// and the host name in the certificate is also validated against the server's host name.
	SslmodeVerifyFull SslmodeVarious = "verify-full"
)

const (
	// defaultPingInterval defines the frequency at which the connection is pinged.
	defaultPingInterval = 30 * time.Second
	defaultTimeFormat   = "2006-01-02 15:04:05.000000"
)

// EventKey represents a type for event keys used in the package.
// It is defined as a string type to provide better type safety and clarity when dealing with event keys.
// This type can be used to define constants for various event keys that are relevant to the package's functionality.
const (
	// Transaction events
	EventTxBegin           = EventKey("event_tx_begin")            // Transaction begin event
	EventTxCommit          = EventKey("event_tx_commit")           // Transaction commit event
	EventTxRollback        = EventKey("event_tx_rollback")         // Transaction rollback event
	EventTxSavepointCreate = EventKey("event_tx_savepoint_create") // Transaction savepoint creation event
	EventTxStarted         = EventKey("event_tx_started")          // Transaction started event
	EventTxStartedAbort    = EventKey("event_tx_started_abort")    // Transaction started with abort event

	// Function events
	EventFunctionListing    = EventKey("event_function_listing")
	EventFunctionMetadata   = EventKey("event_function_metadata")
	EventFunctionDefinition = EventKey("event_function_definition")

	// Procedure events
	EventProcedureListing    = EventKey("event_procedure_listing")
	EventProcedureDefinition = EventKey("event_procedure_definition")

	// Table events
	EventTableListing         = EventKey("event_table_listing")
	EventTableDefinition      = EventKey("event_table_definition")
	EventTableKeysIndexes     = EventKey("event_table_keys_indexes")
	EventTableSearchByCols    = EventKey("event_table_search_by_cols")
	EventTableSearchByAnyCols = EventKey("event_table_search_by_any_cols")
	EventTableColsSpec        = EventKey("event_table_cols_spec")
	EventTablesByColsIn       = EventKey("event_tables_by_cols_in")
	EventTablePrivileges      = EventKey("event_table_privs")
	EventTableColsExists      = EventKey("event_table_cols_exists")
	EventQueryInspect         = EventKey("event_query_inspect")

	// Connection events
	EventConnOpen  = EventKey("event_conn_open")
	EventConnClose = EventKey("event_conn_close")
	EventConnRetry = EventKey("event_conn_retry")
	EventConnPing  = EventKey("event_conn_ping")
)

// EventLevel represents the severity level of an event.
// It is defined as a string type to provide better type safety and clarity when dealing with event levels.
// This type can be used to define constants for various event levels that indicate the importance or severity of events.
const (
	EventLevelInfo    = EventLevel("info")    // Info event level
	EventLevelError   = EventLevel("error")   // Error event level
	EventLevelWarn    = EventLevel("warn")    // Warning event level
	EventLevelDebug   = EventLevel("debug")   // Debug event level
	EventLevelSuccess = EventLevel("success") // Success event level
)
