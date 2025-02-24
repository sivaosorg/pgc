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

func NewSettings() *Settings {
	return &Settings{}
}

// NewClient creates and returns a fully configured Datasource instance for PostgreSQL based on
// the provided Settings configuration. This function attempts to establish an initial connection,
// validate connectivity via a ping, and configure connection pool parameters (max idle, max open,
// and connection lifetime). In addition, if keepalive is enabled, it starts a background routine
// to continuously monitor the connection health and trigger reconnection when necessary.
//
// Returns:
//   - A pointer to a Datasource instance that encapsulates the PostgreSQL connection and its configuration.
func NewClient(conf Settings) *Datasource {
	datasource := &Datasource{
		conf: conf,
	}
	start := time.Now()
	if !conf.IsEnabled() {
		datasource.SetWrap(wrapify.
			WrapServiceUnavailable("Postgresql service unavailable", nil).
			WithDebuggingKV("executed_in", time.Since(start).String()).
			WithHeader(wrapify.ServiceUnavailable).
			Reply())
		return datasource
	}

	// Attempt to open a connection to PostgreSQL using the provided connection string.
	c, err := sqlx.Open("postgres", conf.String(false))
	if err != nil {
		datasource.SetWrap(
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
		datasource.SetWrap(
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
	datasource.SetWrap(wrapify.New().
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

// AllTables retrieves the names of all base tables in the "public" schema of the connected PostgreSQL database.
//
// This function first verifies whether the Datasource is currently connected. If not, it returns the current wrap
// response (which typically contains the connection status or error details).
//
// It then executes a SQL query against the information_schema to retrieve the names of all tables where the schema
// is 'public' and the table type is 'BASE TABLE'. The results are stored in a slice of strings.
//
// In case of an error during the query execution, the function wraps the error using wrapify.WrapInternalServerError,
// attaches any partial results if available, and returns the resulting error response.
//
// If the query executes successfully, it wraps the list of table names using wrapify.WrapOk, includes the total count
// of tables, and returns the successful response.
//
// Returns:
//   - A wrapify.R instance encapsulating either the successful retrieval of table names or the error encountered.
func (d *Datasource) AllTables() wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var tables []string
	err := d.Conn().Select(&tables, "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'")
	if err != nil {
		response := wrapify.WrapInternalServerError("An error occurred while retrieving the list of tables", tables).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk("Retrieved all tables successfully", tables).WithTotal(len(tables)).Reply()
}

// AllFunctions retrieves the names of all stored functions from the "public" schema of the connected PostgreSQL database.
//
// This function first verifies that the Datasource is currently connected. If the connection is not available,
// it immediately returns the existing wrap response which indicates the connection status or any related error.
//
// It then executes a SQL query against the "information_schema.routines" table to obtain the names of all routines
// that are classified as functions. The query filters results based on the current database (using the database name
// from the configuration), the schema ('public'), and the routine type ('FUNCTION'). The retrieved function names
// are stored in a slice of strings.
//
// In the event of an error during query execution, the error is wrapped using wrapify.WrapInternalServerError,
// any partial results are attached, and the resulting error response is returned.
//
// If the query executes successfully, the function wraps the list of function names using wrapify.WrapOk,
// attaches the total count of retrieved functions, and returns the successful response.
//
// Returns:
//   - A wrapify.R instance that encapsulates either the list of function names or an error message,
//     along with metadata such as the total count of functions.
func (d *Datasource) AllFunctions() wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var functions []string
	err := d.Conn().Select(&functions, "SELECT routine_name FROM information_schema.routines WHERE routine_catalog = $1 AND routine_schema = 'public' AND routine_type = 'FUNCTION'", d.conf.Database())
	if err != nil {
		response := wrapify.WrapInternalServerError("An error occurred while retrieving the list of functions", functions).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk("Retrieved all functions successfully", functions).WithTotal(len(functions)).Reply()
}

// AllProcedures retrieves the names of all stored procedures from the "public" schema of the connected PostgreSQL database.
//
// The function first verifies that the Datasource is currently connected. If the connection is not active,
// it immediately returns the current wrap response (which may contain status or error details).
//
// It then executes a SQL query against the "information_schema.routines" table to obtain the names of all routines
// classified as procedures. The query filters results based on the database name (using the configuration's database),
// the schema ('public'), and the routine type ('PROCEDURE'). The retrieved procedure names are stored in a slice of strings.
//
// In the event of a query error, the function wraps the error using wrapify.WrapInternalServerError, attaches any partial
// results if available, and returns the resulting error response. If the query is successful, it wraps the list of procedure names
// using wrapify.WrapOk, includes the total count of procedures, and returns the successful response.
//
// Returns:
//   - A wrapify.R instance that encapsulates either the list of procedure names or an error message, along with metadata
//     such as the total count of procedures.
func (d *Datasource) AllProcedures() wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var procedures []string
	err := d.Conn().Select(&procedures, "SELECT routine_name FROM information_schema.routines WHERE routine_catalog = $1 AND routine_schema = 'public' AND routine_type = 'PROCEDURE'", d.conf.Database())
	if err != nil {
		response := wrapify.WrapInternalServerError("An error occurred while retrieving the list of procedures", procedures).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk("Retrieved all procedures successfully", procedures).WithTotal(len(procedures)).Reply()
}

// GetFuncMetadata retrieves detailed metadata for a specified function from the PostgreSQL database.
//
// This method first checks whether the Datasource is currently connected. If the connection is not available,
// it immediately returns the current wrap response, which typically includes connection status or error details.
//
// The function then executes a SQL query that joins the information_schema.routines and information_schema.parameters
// tables. This query retrieves the following metadata for the specified function:
//   - The routine name,
//   - The data type of each parameter,
//   - The parameter name, and
//   - The parameter mode (e.g., IN, OUT).
//
// The query filters results based on the current database (as provided in the configuration), the "public" schema,
// and the function name provided as an argument. The retrieved data is stored in a slice of FuncMetadata structures.
//
// If an error occurs during query execution, the error is wrapped with a detailed message indicating that the
// retrieval of the function metadata failed. Any partial results are included in the response, and the error response
// is returned immediately.
//
// On success, the method wraps the retrieved metadata in a successful response, appending the total count of metadata
// segments, and then returns this response.
//
// Parameters:
//   - function: The name of the function for which metadata is to be retrieved.
//
// Returns:
//   - A wrapify.R instance that encapsulates either the retrieved function metadata or an error message,
//     along with additional metadata such as the total count of metadata segments.
func (d *Datasource) GetFuncMetadata(function string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var segments []FuncMetadata
	err := d.Conn().Select(&segments, `
			SELECT 
				r.routine_name, 
				p.data_type, 
				p.parameter_name, 
				p.parameter_mode 
			FROM information_schema.routines r 
			JOIN information_schema.parameters p 
				ON r.specific_name = p.specific_name 
			WHERE r.routine_catalog = $1 
				AND r.routine_schema = 'public' 
				AND r.routine_name = $2
				`,
		d.conf.Database(), function)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the function '%s' metadata", function), segments).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved function '%s' metadata successfully", function), segments).WithTotal(len(segments)).Reply()
}

// GetFuncBrief retrieves the complete definition of a specified PostgreSQL function.
//
// This function uses the PostgreSQL built-in function pg_get_functiondef to obtain the
// full SQL definition of the function identified by the provided name. It queries the database
// for the function's definition and scans the resulting output into a string.
//
// The function first checks if the Datasource is connected. If the connection is not active,
// it returns the current wrap response that contains the connection status or error details.
//
// In the event of an error during the query execution, such as if the function cannot be found or
// another database error occurs, the error is wrapped using wrapify.WrapInternalServerError, along with
// the partial content (if any), and the resulting error response is returned.
//
// If the query succeeds, the function wraps the retrieved function definition in a successful response,
// sets the total count to 1 (since a single definition is returned), and then returns this response.
//
// Parameters:
//   - function: The name of the PostgreSQL function whose definition is to be retrieved.
//
// Returns:
//   - A wrapify.R instance that encapsulates either the function's complete definition or an error message,
//     along with additional metadata.
func (d *Datasource) GetFuncBrief(function string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var content string
	err := d.Conn().QueryRow("SELECT pg_get_functiondef($1::regproc)", function).Scan(&content)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the function '%s' metadata", function), content).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved function '%s' metadata successfully", function), content).WithTotal(1).Reply()
}

// GetProcedureBrief retrieves the complete definition of a specified PostgreSQL procedure.
//
// This method leverages the PostgreSQL built-in function pg_get_functiondef to obtain the SQL definition
// of the procedure identified by the given name. Although pg_get_functiondef is primarily used for functions,
// it can also be used to retrieve definitions of procedures registered in the system catalog.
//
// The function first checks whether the Datasource is currently connected. If the connection is not active,
// it immediately returns the existing wrap response containing the connection status or error details.
//
// It then executes a SQL query that calls pg_get_functiondef, passing the procedure's identifier (cast as regproc)
// to retrieve its definition. The resulting definition is scanned into a string variable named content.
//
// If an error occurs during query execution (e.g., if the procedure does not exist or a database error occurs),
// the error is wrapped using wrapify.WrapInternalServerError, along with any partial content, and the resulting
// error response is returned.
//
// On success, the function wraps the retrieved procedure definition in a successful response, sets the total
// count to 1 (since a single definition is returned), and then returns this response.
//
// Parameters:
//   - procedure: The name of the PostgreSQL procedure whose definition is to be retrieved.
//
// Returns:
//   - A wrapify.R instance that encapsulates either the procedure's complete definition or an error message,
//     along with additional metadata such as the total count (1 in this case).
func (d *Datasource) GetProcedureBrief(procedure string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var content string
	err := d.Conn().QueryRow("SELECT pg_get_functiondef($1::regproc)", procedure).Scan(&content)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the procedure '%s' metadata", procedure), content).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved procedure '%s' metadata successfully", procedure), content).WithTotal(1).Reply()
}

// GetTableBrief retrieves metadata information for the specified table from the connected PostgreSQL database.
//
// This function constructs and executes a SQL query that gathers various types of metadata for the given table,
// including primary key constraints, unique key constraints, and index definitions. It does so by performing a
// UNION of three queries:
//  1. The first query retrieves the name of the primary key constraint (labeled as "Primary Key") from the pg_constraint table.
//  2. The second query retrieves the name of any unique key constraint (labeled as "Unique Key") from the pg_constraint table.
//  3. The third query retrieves index information (labeled as "Index") from the pg_indexes view, including the index definition.
//
// The query uses PostgreSQL's regclass type conversion to reference the table by name and filters for constraints
// and indexes that belong to the 'public' schema. The results are then scanned into a slice of TableMetadata structures.
//
// If the Datasource is not connected, the function immediately returns the existing wrap response which indicates the
// connection status. If an error occurs during query execution or while scanning the result rows, the error is wrapped
// using wrapify.WrapInternalServerError and the error response is returned. Upon successful execution, the function returns
// a successful wrapify.R response containing the list of metadata records along with the total count of records retrieved.
//
// Parameters:
//   - table: The name of the table for which metadata is to be retrieved.
//
// Returns:
//   - A wrapify.R instance encapsulating either the retrieved metadata (on success) or an error message (on failure).
func (d *Datasource) GetTableBrief(table string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	s := `
		SELECT conname AS c_name, 'Primary Key' AS type, '' as descriptor
		FROM pg_constraint
		WHERE conrelid = regclass($1)
		AND confrelid = 0
		AND contype = 'p'
		UNION
		SELECT conname AS c_name, 'Unique Key' AS type, '' as descriptor
		FROM pg_constraint
		WHERE conrelid = regclass($1)
		AND confrelid = 0
		AND contype = 'u'
		UNION
		SELECT indexname AS c_name, 'Index' AS type, indexdef as descriptor
		FROM pg_indexes
		WHERE tablename = $1;
	`
	rows, err := d.Conn().Query(s, table)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the table '%s' metadata", table), nil).WithErrSck(err)
		return response.Reply()
	}
	defer rows.Close()
	var results []TableMetadata
	for rows.Next() {
		var m TableMetadata
		if err := rows.Scan(&m.Name, &m.Type, &m.Desc); err != nil {
			response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while scanning rows the table '%s' metadata", table), nil).WithErrSck(err)
			d.notify(response.Reply())
			return response.Reply()
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving rows the table '%s' metadata", table), nil).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved table '%s' metadata successfully", table), results).WithTotal(len(results)).Reply()
}

// GetColumnsBrief retrieves metadata for all columns of the specified table from the PostgreSQL database.
//
// This function queries the information_schema.columns view to collect details about each column in the
// specified table. The retrieved metadata includes the column name, data type, and the maximum character
// length (if applicable). The SQL query filters the columns based on the provided table name.
//
// Initially, the function verifies that the Datasource is connected; if not, it returns the existing wrap
// response which indicates the connection status. It then executes the query and iterates over the result rows,
// scanning each row into a ColumnMetadata structure. If an error occurs during query execution or while scanning
// the rows, the error is wrapped using wrapify.WrapInternalServerError and an error response is returned.
// On successful execution, the function wraps the resulting slice of column metadata using wrapify.WrapOk,
// attaches the total number of columns retrieved, and returns the successful response.
//
// Parameters:
//   - table: The name of the table for which to retrieve column metadata.
//
// Returns:
//   - A wrapify.R instance that encapsulates either the retrieved column metadata or an error message,
//     along with additional metadata (e.g., the total count of columns).
func (d *Datasource) GetColumnsBrief(table string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	s := `
		SELECT
			column_name,
			data_type,
			character_maximum_length
		FROM
			information_schema.columns
		WHERE
			table_name = $1;
	`
	rows, err := d.Conn().Query(s, table)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the columns metadata by table '%s'", table), nil).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	defer rows.Close()
	var results []ColumnMetadata
	for rows.Next() {
		var m ColumnMetadata
		if err := rows.Scan(&m.Column, &m.Type, &m.MaxLength); err != nil {
			response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while scanning the columns metadata by table '%s' ", table), nil).WithErrSck(err)
			d.notify(response.Reply())
			return response.Reply()
		}
		results = append(results, m)
	}
	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving rows and mapping the columns' metadata for the table '%s'", table), nil).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved columns metadata by table '%s' successfully", table), results).WithTotal(len(results)).Reply()
}

// GetTableDDL generates the Data Definition Language (DDL) statement for creating the specified table
// in the connected PostgreSQL database.
//
// This function constructs a DDL statement by querying the system catalog tables. It retrieves the table's name
// and column information from pg_class, pg_namespace, and pg_attribute. The resulting query concatenates the
// column definitions—including data types and NOT NULL constraints—into a formatted CREATE TABLE statement.
//
// The function first checks whether the Datasource is connected. If not, it returns the existing wrap response,
// which includes connection status or error details. If the connection is active, it executes the query with the
// specified table name, scans the generated DDL into a string variable, and handles any errors encountered during
// query execution or scanning by wrapping them in a detailed error response.
//
// Upon success, the function returns a successful wrap response containing the generated DDL statement and the total
// count (which is 1, as only one DDL statement is generated).
//
// Parameters:
//   - table: The name of the table for which the DDL creation statement is to be generated.
//
// Returns:
//   - A wrapify.R instance that encapsulates either the generated DDL statement (on success) or an error message
//     (on failure), along with additional metadata.
func (d *Datasource) GetTableDDL(table string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var ddl string
	query := `
		SELECT 'CREATE TABLE ' || quote_ident(c.relname) || E'\n(\n' ||
			array_to_string(
				array_agg(
					'    ' || quote_ident(a.attname) || ' ' ||
					pg_catalog.format_type(a.atttypid, a.atttypmod) ||
					CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
				), E',\n'
			) || E'\n);\n' AS ddl
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		WHERE c.relname = $1
			AND n.nspname = 'public'
			AND a.attnum > 0
		GROUP BY c.relname;
	`
	err := d.Conn().QueryRow(query, table).Scan(&ddl)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while generating the DDL for table '%s'", table), ddl).
			WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("DDL for table '%s' generated successfully", table), ddl).
		WithTotal(1).
		Reply()
}

// GetTableFullDDL generates a comprehensive Data Definition Language (DDL) script for the specified table,
// including its creation statement as well as its relationships (foreign key constraints), other constraints,
// and indexes.
//
// This function performs multiple queries to construct the full DDL:
//  1. It first retrieves the standard CREATE TABLE statement by querying the PostgreSQL system catalogs.
//  2. It then retrieves any foreign key constraints defined on the table by querying the information_schema,
//     and constructs ALTER TABLE statements for these relationships.
//  3. It also retrieves the definitions of all indexes associated with the table from the pg_indexes view.
//
// The function first verifies that the Datasource is connected. If not, it returns the current wrap response.
// If connected, it sequentially executes the queries to obtain the table DDL, foreign key constraints DDL, and indexes DDL.
// In the event of any errors during the retrieval of the table DDL, an error response is returned immediately.
// For foreign keys and indexes, if no definitions exist or an error occurs, those sections are simply omitted.
// Finally, the function concatenates all retrieved parts into a single DDL script and returns it in a successful response.
//
// Parameters:
//   - table: The name of the table for which the full DDL is to be generated.
//
// Returns:
//   - A wrapify.R instance that encapsulates the complete DDL script for the table (on success) or an error message
//     (on failure), along with additional metadata (e.g., a total count of 1).
func (d *Datasource) GetTableFullDDL(table string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	// Retrieve the basic CREATE TABLE DDL from the system catalogs.
	var tableDDL string
	// ddlQuery := `
	// 	SELECT 'CREATE TABLE ' || quote_ident(c.relname) || E'\n(\n' ||
	// 		array_to_string(
	// 			array_agg(
	// 				'    ' || quote_ident(a.attname) || ' ' ||
	// 				pg_catalog.format_type(a.atttypid, a.atttypmod) ||
	// 				CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
	// 			), E',\n'
	// 		) || E'\n);\n' AS ddl
	// 	FROM pg_class c
	// 	JOIN pg_namespace n ON n.oid = c.relnamespace
	// 	JOIN pg_attribute a ON a.attrelid = c.oid
	// 	WHERE c.relname = $1
	// 		AND n.nspname = 'public'
	// 		AND a.attnum > 0
	// 	GROUP BY c.relname;
	// `

	// ddlQuery := `
	// 	SELECT 'CREATE TABLE ' || quote_ident(c.relname) || E'\n(\n' ||
	// 		array_to_string(
	// 			array_agg(
	// 				'    ' || quote_ident(a.attname) || ' ' ||
	// 				UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)) ||
	// 				CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
	// 			), E',\n'
	// 		) || E'\n);\n' AS ddl
	// 	FROM pg_class c
	// 	JOIN pg_namespace n ON n.oid = c.relnamespace
	// 	JOIN pg_attribute a ON a.attrelid = c.oid
	// 	WHERE c.relname = $1
	// 		AND n.nspname = 'public'
	// 		AND a.attnum > 0
	// 	GROUP BY c.relname;
	// `

	// ddlQuery := `
	// 	SELECT 'CREATE TABLE ' || quote_ident(c.relname) || E'\n(\n' ||
	// 		array_to_string(
	// 			array_agg(
	// 				'    ' || quote_ident(a.attname) || ' ' ||
	// 				(
	// 					CASE
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'integer' THEN 'INT4'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'bigint' THEN 'INT8'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'smallint' THEN 'INT16'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'real' THEN 'FLOAT32'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'double precision' THEN 'FLOAT64'
	// 						ELSE UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod))
	// 					END
	// 				) ||
	// 				CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
	// 			), E',\n'
	// 		) || E'\n);\n' AS ddl
	// 	FROM pg_class c
	// 	JOIN pg_namespace n ON n.oid = c.relnamespace
	// 	JOIN pg_attribute a ON a.attrelid = c.oid
	// 	WHERE c.relname = $1
	// 		AND n.nspname = 'public'
	// 		AND a.attnum > 0
	// 	GROUP BY c.relname;
	// `

	// ddlQuery := `
	// 	SELECT 'CREATE TABLE ' || quote_ident(c.relname) || E'\n(\n' ||
	// 		array_to_string(
	// 			array_agg(
	// 				'    ' || quote_ident(a.attname) || ' ' ||
	// 				(
	// 					CASE
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'integer' THEN 'INT4'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'bigint' THEN 'INT8'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'smallint' THEN 'INT16'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'real' THEN 'FLOAT32'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'double precision' THEN 'FLOAT64'
	// 						WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) ILIKE 'character varying%' THEN 'VARCHAR'
	// 						ELSE UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod))
	// 					END
	// 				) ||
	// 				CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
	// 			), E',\n'
	// 		) || E'\n);\n' AS ddl
	// 	FROM pg_class c
	// 	JOIN pg_namespace n ON n.oid = c.relnamespace
	// 	JOIN pg_attribute a ON a.attrelid = c.oid
	// 	WHERE c.relname = $1
	// 		AND n.nspname = 'public'
	// 		AND a.attnum > 0
	// 	GROUP BY c.relname;
	// `

	ddlQuery := `
		SELECT 'CREATE TABLE ' || quote_ident(c.relname) || E'\n(\n' ||
			array_to_string(
				array_agg(
					'    ' || quote_ident(a.attname) || ' ' ||
					(
						CASE 
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'integer' THEN 'INT4'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'bigint' THEN 'INT8'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'smallint' THEN 'INT16'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'real' THEN 'FLOAT32'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'double precision' THEN 'FLOAT64'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) ILIKE 'character varying%' THEN 
								'VARCHAR' || CASE 
									WHEN a.atttypmod > 0 THEN '(' || (a.atttypmod - 4)::text || ')'
									ELSE ''
								END
							ELSE UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod))
						END
					) ||
					CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END
				), E',\n'
			) || E'\n);\n' AS ddl
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		WHERE c.relname = $1
			AND n.nspname = 'public'
			AND a.attnum > 0
		GROUP BY c.relname;
	`

	err := d.Conn().QueryRow(ddlQuery, table).Scan(&tableDDL)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while generating the DDL for table '%s'", table), tableDDL).
			WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}

	// Retrieve foreign key constraints DDL.
	// This query constructs ALTER TABLE statements for each foreign key constraint defined on the table.
	var fkDDL string
	fkQuery := `
		SELECT COALESCE(string_agg(fk_statement, E';\n'), '') as fk_ddl
		FROM (
			SELECT 'ALTER TABLE ' || quote_ident(tc.table_name) ||
				' ADD CONSTRAINT ' || quote_ident(tc.constraint_name) ||
				' FOREIGN KEY (' || string_agg(quote_ident(kcu.column_name), ', ') || ')' ||
				' REFERENCES ' || quote_ident(ccu.table_name) ||
				' (' || string_agg(quote_ident(ccu.column_name), ', ') || ')' AS fk_statement
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
			WHERE tc.constraint_type = 'FOREIGN KEY'
				AND tc.table_name = $1
			GROUP BY tc.table_name, tc.constraint_name, ccu.table_name
		) sub;
	`
	err = d.Conn().QueryRow(fkQuery, table).Scan(&fkDDL)
	if err != nil {
		// If an error occurs (e.g. no foreign key constraints exist), default to an empty string.
		fkDDL = ""
	}
	// Retrieve indexes DDL.
	// This query aggregates the index definitions into a single string.
	var indexes string
	indexQuery := `
		SELECT COALESCE(string_agg(indexdef, E';\n'), '') as indexes
		FROM pg_indexes
		WHERE tablename = $1;
	`
	err = d.Conn().QueryRow(indexQuery, table).Scan(&indexes)
	if err != nil {
		// If an error occurs (e.g. no indexes exist), default to an empty string.
		indexes = ""
	}
	// Concatenate the various parts of the DDL into one comprehensive script.
	fullDDL := tableDDL
	if isNotEmpty(fkDDL) {
		fullDDL += "\n\n-- Foreign Key Constraints\n" + fkDDL + ";"
	}
	if isNotEmpty(indexes) {
		fullDDL += "\n\n-- Indexes\n" + indexes + ";"
	}
	return wrapify.WrapOk(fmt.Sprintf("Generated full DDL for table '%s' including relationships, constraints, and indexes", table), fullDDL).
		WithTotal(1).
		Reply()
}

// GetTableFullDDL generates a comprehensive Data Definition Language (DDL) script for the specified table,
// including its creation statement as well as its relationships (foreign key constraints), other constraints,
// and indexes. In this version, the column definitions include explicit uppercase type identifiers, default values,
// primary key markers, and an indication if a column's default is generated by a sequence.
//
// Specifically, for CHARACTER VARYING columns, the type is output as VARCHAR with its length (e.g., VARCHAR(255)).
// For each column:
//   - The data type is determined via pg_catalog.format_type and then mapped to an uppercase label (e.g., INT4, INT8).
//   - If the column has a default value, it is appended to the column definition. If the default expression
//     indicates the use of a sequence (i.e. contains "nextval("), a marker (/* SEQUENCE */) is appended.
//   - If the column is part of the primary key, the string " PRIMARY KEY" is appended.
//
// The function also retrieves foreign key constraints and index definitions in separate queries and appends them
// to the DDL script. If no constraints or indexes are found, those sections are omitted.
//
// The function first verifies that the Datasource is connected. If not, it returns the current wrap response.
// Otherwise, it sequentially executes the queries to obtain the table DDL, foreign key constraints DDL, and indexes DDL.
// In the event of an error during any query, an error response is returned immediately.
// Finally, the function concatenates all parts into a complete DDL script and returns it in a successful response.
//
// Parameters:
//   - table: The name of the table for which the full DDL is to be generated.
//
// Returns:
//   - A wrapify.R instance that encapsulates the complete DDL script for the table (on success) or an error message
//     (on failure), along with additional metadata.
func (d *Datasource) GetTableFullDDLDepth(table string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	// Retrieve the basic CREATE TABLE DDL from the system catalogs.
	// For each column, the data type is mapped to an uppercase label with explicit adjustments:
	//   - INTEGER, BIGINT, SMALLINT, REAL, and DOUBLE PRECISION are mapped to INT4, INT8, INT16, FLOAT32, and FLOAT64 respectively.
	//   - CHARACTER VARYING columns are mapped to VARCHAR with their defined length.
	// Additionally, default values are appended; if the default contains a nextval() call, a sequence marker is added.
	// If a column is part of the primary key, " PRIMARY KEY" is appended.
	var tableDDL string
	ddlQuery := `
		SELECT 'CREATE TABLE ' || quote_ident(c.relname) || E'\n(\n' ||
			array_to_string(
				array_agg(
					'    ' || quote_ident(a.attname) || ' ' ||
					(
						CASE 
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'integer' THEN 'INT4'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'bigint' THEN 'INT8'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'smallint' THEN 'INT16'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'real' THEN 'FLOAT32'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) = 'double precision' THEN 'FLOAT64'
							WHEN pg_catalog.format_type(a.atttypid, a.atttypmod) ILIKE 'character varying%' THEN 
								'VARCHAR' || CASE 
									WHEN a.atttypmod > 0 THEN '(' || (a.atttypmod - 4)::text || ')'
									ELSE ''
								END
							ELSE UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod))
						END
					) ||
					CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END ||
					COALESCE(
						' DEFAULT ' || ad.adsrc ||
						CASE WHEN ad.adsrc ILIKE 'nextval(%' THEN ' /* SEQUENCE */' ELSE '' END,
						''
					) ||
					CASE WHEN EXISTS (
						SELECT 1 FROM pg_constraint con 
						WHERE con.conrelid = c.oid 
						  AND con.contype = 'p' 
						  AND a.attnum = ANY(con.conkey)
					) THEN ' PRIMARY KEY' ELSE '' END
				), E',\n'
			) || E'\n);\n' AS ddl
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid
		LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
		WHERE c.relname = $1
			AND n.nspname = 'public'
			AND a.attnum > 0
		GROUP BY c.relname;
	`
	err := d.Conn().QueryRow(ddlQuery, table).Scan(&tableDDL)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while generating the DDL for table '%s'", table), tableDDL).
			WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}

	// Retrieve foreign key constraints DDL.
	// This query constructs ALTER TABLE statements for each foreign key constraint defined on the table.
	var fkDDL string
	fkQuery := `
		SELECT COALESCE(string_agg(fk_statement, E';\n'), '') as fk_ddl
		FROM (
			SELECT 'ALTER TABLE ' || quote_ident(tc.table_name) ||
				' ADD CONSTRAINT ' || quote_ident(tc.constraint_name) ||
				' FOREIGN KEY (' || string_agg(quote_ident(kcu.column_name), ', ') || ')' ||
				' REFERENCES ' || quote_ident(ccu.table_name) ||
				' (' || string_agg(quote_ident(ccu.column_name), ', ') || ')' AS fk_statement
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
			WHERE tc.constraint_type = 'FOREIGN KEY'
				AND tc.table_name = $1
			GROUP BY tc.table_name, tc.constraint_name, ccu.table_name
		) sub;
	`
	err = d.Conn().QueryRow(fkQuery, table).Scan(&fkDDL)
	if err != nil {
		// If an error occurs (e.g., no foreign key constraints exist), default to an empty string.
		fkDDL = ""
	}

	// Retrieve indexes DDL.
	// This query aggregates the index definitions into a single string.
	var indexes string
	indexQuery := `
		SELECT COALESCE(string_agg(indexdef, E';\n'), '') as indexes
		FROM pg_indexes
		WHERE tablename = $1;
	`
	err = d.Conn().QueryRow(indexQuery, table).Scan(&indexes)
	if err != nil {
		// If an error occurs (e.g., no indexes exist), default to an empty string.
		indexes = ""
	}
	// Concatenate the various parts of the DDL into one comprehensive script.
	fullDDL := tableDDL
	if isNotEmpty(fkDDL) {
		fullDDL += "\n\n-- Foreign Key Constraints\n" + fkDDL + ";"
	}
	if isNotEmpty(indexes) {
		fullDDL += "\n\n-- Indexes\n" + indexes + ";"
	}
	return wrapify.WrapOk(fmt.Sprintf("Generated full DDL for table '%s' including relationships, constraints, and indexes", table), fullDDL).
		WithTotal(1).
		Reply()
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
			d.SetWrap(response)
			d.invoke(response)
			d.invokeReplica(response, d)
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

// invokeReplica safely retrieves the registered replica callback function and, if one is set,
// invokes it asynchronously with the provided wrapify.R response and a pointer to the replica Datasource.
// This ensures that external consumers are notified of replica-specific connection status changes,
// such as replica failovers, reconnection attempts, or health updates, without blocking the calling goroutine.
func (d *Datasource) invokeReplica(response wrapify.R, replicator *Datasource) {
	d.mu.RLock()
	callback := d.onReplica
	d.mu.RUnlock()
	if callback != nil {
		go callback(response, replicator)
	}
}

// notify safely retrieves the registered notifier callback function and, if one is set,
// invokes it asynchronously with the provided wrapify.R response. This method allows the Datasource
// to notify external components of significant events (e.g., reconnection, keepalive updates)
// without blocking the calling goroutine, ensuring that notification handling is performed concurrently.
func (d *Datasource) notify(response wrapify.R) {
	d.mu.RLock()
	callback := d.notifier
	d.mu.RUnlock()
	if callback != nil {
		go callback(response)
	}
}
