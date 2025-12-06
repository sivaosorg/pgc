package pgc

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sivaosorg/wrapify"

	"github.com/lib/pq"
)

// Tables retrieves the names of all base tables in the "public" schema of the connected PostgreSQL database.
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
func (d *Datasource) Tables() (tables []string, response wrapify.R) {
	if !d.IsConnected() {
		return tables, d.State()
	}

	query := "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';"

	// Start inspection
	done := d.inspectQuery("Tables", query)
	err := d.Conn().Select(&tables, query)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError("An error occurred while retrieving the list of tables", tables).WithErrSck(err)
		d.dispatch_event(EventTableListing, EventLevelError, response.Reply())
		return tables, response.Reply()
	}

	if len(tables) == 0 {
		response := wrapify.WrapNotFound("No tables found", tables).BindCause()
		d.dispatch_event(EventTableListing, EventLevelError, response.Reply())
		return tables, response.Reply()
	}
	d.dispatch_event(EventTableListing, EventLevelSuccess, response.Reply())
	return tables, wrapify.WrapOk("Retrieved all tables successfully", tables).WithTotal(len(tables)).Reply()
}

// Functions retrieves the names of all stored functions from the "public" schema of the connected PostgreSQL database.
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
func (d *Datasource) Functions() (functions []string, response wrapify.R) {
	if !d.IsConnected() {
		return functions, d.State()
	}

	query := `
	SELECT routine_name FROM information_schema.routines 
	WHERE routine_catalog = $1 
	AND routine_schema = 'public' 
	AND routine_type = 'FUNCTION';
	`

	// Start inspection
	done := d.inspectQuery("Functions", query, d.conf.Database())
	err := d.Conn().Select(&functions, query, d.conf.Database())
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError("An error occurred while retrieving the list of functions", functions).WithErrSck(err)
		d.dispatch_event(EventFunctionListing, EventLevelError, response.Reply())
		return functions, response.Reply()
	}

	if len(functions) == 0 {
		response := wrapify.WrapNotFound("No functions found", functions).BindCause()
		d.dispatch_event(EventFunctionListing, EventLevelError, response.Reply())
		return functions, response.Reply()
	}

	d.dispatch_event(EventFunctionListing, EventLevelSuccess, response.Reply())
	return functions, wrapify.WrapOk("Retrieved all functions successfully", functions).WithTotal(len(functions)).Reply()
}

// Procedures retrieves the names of all stored procedures from the "public" schema of the connected PostgreSQL database.
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
func (d *Datasource) Procedures() (procedures []string, response wrapify.R) {
	if !d.IsConnected() {
		return procedures, d.State()
	}

	query := `
	SELECT routine_name FROM information_schema.routines 
	WHERE routine_catalog = $1 
	AND routine_schema = 'public' 
	AND routine_type = 'PROCEDURE';
	`

	// Start inspection
	done := d.inspectQuery("Procedures", query, d.conf.Database())
	err := d.Conn().Select(&procedures, query, d.conf.Database())
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError("An error occurred while retrieving the list of procedures", procedures).WithErrSck(err)
		d.dispatch_event(EventProcedureListing, EventLevelError, response.Reply())
		return procedures, response.Reply()
	}

	if len(procedures) == 0 {
		response := wrapify.WrapNotFound("No procedures found", procedures).BindCause()
		d.dispatch_event(EventProcedureListing, EventLevelError, response.Reply())
		return procedures, response.Reply()
	}
	d.dispatch_event(EventProcedureListing, EventLevelSuccess, response.Reply())
	return procedures, wrapify.WrapOk("Retrieved all procedures successfully", procedures).WithTotal(len(procedures)).Reply()
}

// FuncSpec retrieves detailed metadata for a specified function from the PostgreSQL database.
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
func (d *Datasource) FuncSpec(function string) (fsm []FuncsSpec, response wrapify.R) {
	if !d.IsConnected() {
		return fsm, d.State()
	}
	if isEmpty(function) {
		response := wrapify.WrapBadRequest("Function name is required", fsm).BindCause()
		d.dispatch_event(EventFunctionMetadata, EventLevelError, response.Reply())
		return fsm, response.Reply()
	}

	query := `
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
				AND r.routine_name = $2;
	`

	// Start inspection
	done := d.inspectQuery("FuncSpec", query, d.conf.Database(), function)
	err := d.Conn().Select(&fsm, query, d.conf.Database(), function)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the function '%s' metadata", function), fsm).WithErrSck(err)
		d.dispatch_event(EventFunctionMetadata, EventLevelError, response.Reply())
		return fsm, response.Reply()
	}

	if len(fsm) == 0 {
		response := wrapify.WrapNotFound(fmt.Sprintf("Function '%s' not found", function), fsm).BindCause()
		d.dispatch_event(EventFunctionMetadata, EventLevelError, response.Reply())
		return fsm, response.Reply()
	}

	d.dispatch_event(EventFunctionMetadata, EventLevelSuccess, response.Reply())
	return fsm, wrapify.WrapOk(fmt.Sprintf("Retrieved function '%s' metadata successfully", function), fsm).WithTotal(len(fsm)).Reply()
}

// FuncDef retrieves the complete definition of a specified PostgreSQL function.
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
func (d *Datasource) FuncDef(function string) (def string, response wrapify.R) {
	if !d.IsConnected() {
		return def, d.State()
	}
	if isEmpty(function) {
		response := wrapify.WrapBadRequest("Function name is required", def).BindCause()
		d.dispatch_event(EventFunctionDefinition, EventLevelError, response.Reply())
		return def, response.Reply()
	}

	query := "SELECT pg_get_functiondef($1::regproc)"

	// Start inspection
	done := d.inspectQuery("FuncDef", query, function)
	err := d.Conn().QueryRow(query, function).Scan(&def)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the function '%s' metadata", function), def).WithErrSck(err)
		d.dispatch_event(EventFunctionDefinition, EventLevelError, response.Reply())
		return def, response.Reply()
	}

	if isEmpty(def) {
		response := wrapify.WrapNotFound(fmt.Sprintf("Function '%s' not found", function), def).BindCause()
		d.dispatch_event(EventFunctionDefinition, EventLevelError, response.Reply())
		return def, response.Reply()
	}

	d.dispatch_event(EventFunctionDefinition, EventLevelSuccess, response.Reply())
	return def, wrapify.WrapOk(fmt.Sprintf("Retrieved function '%s' metadata successfully", function), def).WithTotal(1).Reply()
}

// ProcDef retrieves the complete definition of a specified PostgreSQL procedure.
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
func (d *Datasource) ProcDef(procedure string) (def string, response wrapify.R) {
	if !d.IsConnected() {
		return def, d.State()
	}
	if isEmpty(procedure) {
		response := wrapify.WrapBadRequest("Procedure name is required", def).BindCause()
		d.dispatch_event(EventProcedureDefinition, EventLevelError, response.Reply())
		return def, response.Reply()
	}

	// Use regprocedure for procedures and verify it's actually a procedure (prokind = 'p')
	query := `
	SELECT pg_get_functiondef(p.oid)
	FROM pg_proc p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	WHERE p. proname = $1 AND p.prokind = 'p'
	LIMIT 1
	`

	// Start inspection
	done := d.inspectQuery("ProcDef", query, procedure)
	err := d.Conn().QueryRow(query, procedure).Scan(&def)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the procedure '%s' metadata", procedure), def).WithErrSck(err)
		d.dispatch_event(EventProcedureDefinition, EventLevelError, response.Reply())
		return def, response.Reply()
	}

	if isEmpty(def) {
		response := wrapify.WrapNotFound(fmt.Sprintf("Procedure '%s' not found", procedure), def).BindCause()
		d.dispatch_event(EventProcedureDefinition, EventLevelError, response.Reply())
		return def, response.Reply()
	}

	d.dispatch_event(EventProcedureDefinition, EventLevelSuccess, response.Reply())
	return def, wrapify.WrapOk(fmt.Sprintf("Retrieved procedure '%s' metadata successfully", procedure), def).WithTotal(1).Reply()
}

// TableDef generates the Data Definition Language (DDL) statement for creating the specified table
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
func (d *Datasource) TableDef(table string) (ddl string, response wrapify.R) {
	if !d.IsConnected() {
		return ddl, d.State()
	}
	if isEmpty(table) {
		response := wrapify.WrapBadRequest("Table name is required", ddl).BindCause()
		d.dispatch_event(EventTableDefinition, EventLevelError, response.Reply())
		return ddl, response.Reply()
	}

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

	// Start inspection
	done := d.inspectQuery("TableDef", query, table)
	err := d.Conn().QueryRow(query, table).Scan(&ddl)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while generating the table definition for table '%s'", table), ddl).
			WithErrSck(err)
		d.dispatch_event(EventTableDefinition, EventLevelError, response.Reply())
		return ddl, response.Reply()
	}

	if isEmpty(ddl) {
		response := wrapify.WrapNotFound(fmt.Sprintf("Table '%s' not found", table), ddl).BindCause()
		d.dispatch_event(EventTableDefinition, EventLevelError, response.Reply())
		return ddl, response.Reply()
	}

	d.dispatch_event(EventTableDefinition, EventLevelSuccess, response.Reply())
	return ddl, wrapify.WrapOk(fmt.Sprintf("Table definition for table '%s' generated successfully", table), ddl).WithTotal(1).Reply()
}

// TableDefPlus generates a comprehensive Data Definition Language (DDL) script for the specified table,
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
func (d *Datasource) TableDefPlus(table string) (ddl string, response wrapify.R) {
	if !d.IsConnected() {
		return ddl, d.State()
	}
	if isEmpty(table) {
		response := wrapify.WrapBadRequest("Table name is required", ddl).BindCause()
		d.dispatch_event(EventTableDefinition, EventLevelError, response.Reply())
		return ddl, response.Reply()
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
	// Start inspection
	done := d.inspectQuery("TableDefPlus-ddl", ddlQuery, table)
	err := d.Conn().QueryRow(ddlQuery, table).Scan(&tableDDL)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while generating the table definition for table '%s'", table), tableDDL).
			WithErrSck(err)
		d.dispatch_event(EventTableDefinition, EventLevelError, response.Reply())
		return ddl, response.Reply()
	}

	if isEmpty(tableDDL) {
		response := wrapify.WrapNotFound(fmt.Sprintf("Table '%s' not found", table), tableDDL).BindCause()
		d.dispatch_event(EventTableDefinition, EventLevelError, response.Reply())
		return ddl, response.Reply()
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
	// Start inspection
	done = d.inspectQuery("TableDefPlus-fk", fkQuery, table)
	err = d.Conn().QueryRow(fkQuery, table).Scan(&fkDDL)
	// End inspection
	done()

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

	// Start inspection
	done = d.inspectQuery("TableDefPlus-indexes", indexQuery, table)
	err = d.Conn().QueryRow(indexQuery, table).Scan(&indexes)
	// End inspection
	done()

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

	if isEmpty(fullDDL) {
		response := wrapify.WrapNotFound(fmt.Sprintf("Table '%s' not found", table), fullDDL).BindCause()
		d.dispatch_event(EventTableDefinition, EventLevelError, response.Reply())
		return ddl, response.Reply()
	}

	d.dispatch_event(EventTableDefinition, EventLevelSuccess, response.Reply())
	return fullDDL, wrapify.WrapOk(fmt.Sprintf("Table definition for table '%s' including relationships, constraints, and indexes", table), fullDDL).WithTotal(1).Reply()
}

// TableKeys retrieves metadata information for the specified table from the connected PostgreSQL database.
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
func (d *Datasource) TableKeys(table string) (keys []TableKeysDef, response wrapify.R) {
	if !d.IsConnected() {
		return keys, d.State()
	}
	if isEmpty(table) {
		response := wrapify.WrapBadRequest("Table name is required", keys).BindCause()
		d.dispatch_event(EventTableKeysIndexes, EventLevelError, response.Reply())
		return keys, response.Reply()
	}

	query := `
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
	// Start inspection
	done := d.inspectQuery("TableKeys", query, table)
	rows, err := d.Conn().Query(query, table)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the table '%s' metadata", table), nil).WithErrSck(err)
		return keys, response.Reply()
	}
	defer rows.Close()

	for rows.Next() {
		var m TableKeysDef
		if err := rows.Scan(&m.Name, &m.Type, &m.Desc); err != nil {
			response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while scanning rows the table '%s' metadata", table), nil).WithErrSck(err)
			d.dispatch_event(EventTableKeysIndexes, EventLevelError, response.Reply())
			return keys, response.Reply()
		}
		keys = append(keys, m)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving rows the table '%s' metadata", table), nil).WithErrSck(err)
		d.dispatch_event(EventTableKeysIndexes, EventLevelError, response.Reply())
		return keys, response.Reply()
	}

	if len(keys) == 0 {
		response := wrapify.WrapNotFound(fmt.Sprintf("Table '%s' not found", table), keys).BindCause()
		d.dispatch_event(EventTableKeysIndexes, EventLevelError, response.Reply())
		return keys, response.Reply()
	}

	d.dispatch_event(EventTableKeysIndexes, EventLevelSuccess, response.Reply())
	return keys, wrapify.WrapOk(fmt.Sprintf("Retrieved table '%s' keys and indexes metadata successfully", table), keys).WithTotal(len(keys)).Reply()
}

// ColsSpec retrieves metadata for all columns of the specified table from the PostgreSQL database.
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
func (d *Datasource) ColsSpec(table string) (cols []ColsSpec, response wrapify.R) {
	if !d.IsConnected() {
		return cols, d.State()
	}

	if isEmpty(table) {
		response := wrapify.WrapBadRequest("Table name is required", cols).BindCause()
		d.dispatch_event(EventTableColsSpec, EventLevelError, response.Reply())
		return cols, response.Reply()
	}

	query := `
		SELECT
			column_name,
			data_type,
			character_maximum_length
		FROM
			information_schema.columns
		WHERE
			table_name = $1;
	`

	// Start inspection
	done := d.inspectQuery("ColsSpec", query, table)
	rows, err := d.Conn().Query(query, table)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the columns metadata by table '%s'", table), nil).WithErrSck(err)
		d.dispatch_event(EventTableColsSpec, EventLevelError, response.Reply())
		return cols, response.Reply()
	}
	defer rows.Close()

	for rows.Next() {
		var m ColsSpec
		if err := rows.Scan(&m.Column, &m.Type, &m.MaxLength); err != nil {
			response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while scanning the columns metadata by table '%s' ", table), nil).WithErrSck(err)
			d.dispatch_event(EventTableColsSpec, EventLevelError, response.Reply())
			return cols, response.Reply()
		}
		cols = append(cols, m)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving rows and mapping the columns' metadata for the table '%s'", table), nil).WithErrSck(err)
		d.dispatch_event(EventTableColsSpec, EventLevelError, response.Reply())
		return cols, response.Reply()
	}

	if len(cols) == 0 {
		response := wrapify.WrapNotFound(fmt.Sprintf("Table '%s' not found", table), cols).BindCause()
		d.dispatch_event(EventTableColsSpec, EventLevelError, response.Reply())
		return cols, response.Reply()
	}

	d.dispatch_event(EventTableColsSpec, EventLevelSuccess, response.Reply())
	return cols, wrapify.WrapOk(fmt.Sprintf("Retrieved columns metadata by table '%s' successfully", table), cols).WithTotal(len(cols)).Reply()
}

// TablesByCols searches for tables that contain ALL specified columns.
//
// This function queries the information_schema.columns view to find tables that contain
// every column in the provided list. Only tables containing ALL specified columns will
// be returned.
//
// Parameters:
//   - columns: A slice of column names to search for.  All columns must exist in a table
//     for that table to be included in the results.
//
// Returns:
//   - A wrapify.R instance that encapsulates either a slice of TableWithColumns containing
//     all tables with all specified columns, or an error message, along with additional metadata.
func (d *Datasource) TablesByCols(columns []string) (stats []TableColsSpec, response wrapify.R) {
	if !d.IsConnected() {
		return stats, d.State()
	}
	if len(columns) == 0 {
		response := wrapify.WrapBadRequest("No columns provided for search", nil).BindCause()
		d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	// Build query to find tables containing ALL specified columns
	query := `
		SELECT 
			table_schema,
			table_name,
			array_agg(column_name ORDER BY column_name) AS matched_columns
		FROM information_schema.columns
		WHERE column_name = ANY($1)
		  AND table_schema NOT IN ('pg_catalog', 'information_schema')
		GROUP BY table_schema, table_name
		HAVING COUNT(DISTINCT column_name) = $2
		ORDER BY table_schema, table_name;
	`

	// Start inspection
	done := d.inspectQuery("TablesByCols", query, pq.Array(columns), len(columns))
	rows, err := d.Conn().Query(query, pq.Array(columns), len(columns))
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with columns %v", columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}
	defer rows.Close()

	for rows.Next() {
		var r TableColsSpec
		var matchedCols pq.StringArray
		if err := rows.Scan(&r.SchemaName, &r.TableName, &matchedCols); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v", columns),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
			return stats, response.Reply()
		}
		r.MatchedColumns = []string(matchedCols)
		r.TotalColumns = len(columns)
		r.MatchedCount = len(r.MatchedColumns)
		stats = append(stats, r)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating results for columns %v", columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	if len(stats) == 0 {
		response := wrapify.WrapNotFound(
			fmt.Sprintf("No tables found containing all specified columns '%v'", strings.Join(columns, ", ")),
			stats,
		).BindCause()
		d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	d.dispatch_event(EventTableSearchByCols, EventLevelSuccess, response.Reply())
	return stats, wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) containing all %d specified column(s)", len(stats), len(columns)),
		stats,
	).WithTotal(len(stats)).Reply()
}

// TablesByAnyCols searches for tables that contain AT LEAST ONE of the specified columns.
//
// This function queries the information_schema.columns view to find tables that contain
// any of the columns in the provided list. Tables containing at least one matching column
// will be returned, along with information about which columns matched.
//
// Parameters:
//   - columns: A slice of column names to search for. Tables containing any of these
//     columns will be included in the results.
//
// Returns:
//   - A wrapify. R instance that encapsulates either a slice of TableWithColumns containing
//     all tables with at least one specified column, or an error message.
func (d *Datasource) TablesByAnyCols(columns []string) (stats []TableColsSpec, response wrapify.R) {
	if !d.IsConnected() {
		return stats, d.State()
	}
	if len(columns) == 0 {
		response := wrapify.WrapBadRequest("No columns provided for search", nil).BindCause()
		d.dispatch_event(EventTableSearchByAnyCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	query := `
		SELECT 
			table_schema,
			table_name,
			array_agg(column_name ORDER BY column_name) AS matched_columns
		FROM information_schema. columns
		WHERE column_name = ANY($1)
		  AND table_schema NOT IN ('pg_catalog', 'information_schema')
		GROUP BY table_schema, table_name
		ORDER BY table_schema, table_name;
	`

	// Start inspection
	done := d.inspectQuery("TablesByColsIn", query, pq.Array(columns))
	rows, err := d.Conn().Query(query, pq.Array(columns))
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with any columns %v", columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableSearchByAnyCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}
	defer rows.Close()

	for rows.Next() {
		var r TableColsSpec
		var matchedCols pq.StringArray
		if err := rows.Scan(&r.SchemaName, &r.TableName, &matchedCols); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v", columns),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTableSearchByAnyCols, EventLevelError, response.Reply())
			return stats, response.Reply()
		}
		r.MatchedColumns = []string(matchedCols)
		r.TotalColumns = len(columns)
		r.MatchedCount = len(r.MatchedColumns)
		stats = append(stats, r)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating results for columns %v", columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableSearchByAnyCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	if len(stats) == 0 {
		response := wrapify.WrapNotFound(
			fmt.Sprintf("No tables found containing any of the specified columns '%v'", strings.Join(columns, ", ")),
			stats,
		).BindCause()
		d.dispatch_event(EventTableSearchByAnyCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	d.dispatch_event(EventTableSearchByAnyCols, EventLevelSuccess, response.Reply())
	return stats, wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) containing at least one of %d specified column(s)", len(stats), len(columns)),
		stats,
	).WithTotal(len(stats)).Reply()
}

// TablesByColsIn searches for tables containing ALL specified columns within a specific schema.
//
// Parameters:
//   - schema:  The name of the schema to search within.
//   - columns: A slice of column names to search for.
//
// Returns:
//   - A wrapify. R instance that encapsulates either a slice of TableWithColumns or an error message.
func (d *Datasource) TablesByColsIn(schema string, columns []string) (stats []TableColsSpec, response wrapify.R) {
	if !d.IsConnected() {
		return stats, d.State()
	}
	if len(columns) == 0 {
		response := wrapify.WrapBadRequest("No columns provided for search", nil).BindCause()
		d.dispatch_event(EventTablesByColsIn, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	query := `
		SELECT 
			table_schema,
			table_name,
			array_agg(column_name ORDER BY column_name) AS matched_columns
		FROM information_schema. columns
		WHERE table_schema = $1
		  AND column_name = ANY($2)
		GROUP BY table_schema, table_name
		HAVING COUNT(DISTINCT column_name) = $3
		ORDER BY table_name;
	`

	// Start inspection
	done := d.inspectQuery("TablesByColsIn", query, schema, pq.Array(columns), len(columns))
	rows, err := d.Conn().Query(query, schema, pq.Array(columns), len(columns))
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with columns %v in schema '%s'", columns, schema),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTablesByColsIn, EventLevelError, response.Reply())
		return stats, response.Reply()
	}
	defer rows.Close()

	for rows.Next() {
		var r TableColsSpec
		var matchedCols pq.StringArray
		if err := rows.Scan(&r.SchemaName, &r.TableName, &matchedCols); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v in schema '%s'", columns, schema),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTablesByColsIn, EventLevelError, response.Reply())
			return stats, response.Reply()
		}
		r.MatchedColumns = []string(matchedCols)
		r.TotalColumns = len(columns)
		r.MatchedCount = len(r.MatchedColumns)
		stats = append(stats, r)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating results for columns %v in schema '%s'", columns, schema),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTablesByColsIn, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	if len(stats) == 0 {
		response := wrapify.WrapNotFound(
			fmt.Sprintf("No tables found in schema '%s' containing all specified columns '%v'", schema, strings.Join(columns, ", ")),
			stats,
		).BindCause()
		d.dispatch_event(EventTablesByColsIn, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	d.dispatch_event(EventTablesByColsIn, EventLevelSuccess, response.Reply())
	return stats, wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) in schema '%s' containing all %d specified column(s)", len(stats), schema, len(columns)),
		stats,
	).WithTotal(len(stats)).Reply()
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
func (d *Datasource) TablesByColsPlus(columns []string) (stats []TableColsSpecMeta, response wrapify.R) {
	if !d.IsConnected() {
		return stats, d.State()
	}
	if len(columns) == 0 {
		response := wrapify.WrapBadRequest("No columns provided for search", nil).BindCause()
		d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	// First, get all tables that have at least one of the columns
	query := `
		SELECT 
			table_schema,
			table_name,
			column_name,
			data_type,
			is_nullable
		FROM information_schema. columns
		WHERE column_name = ANY($1)
		  AND table_schema NOT IN ('pg_catalog', 'information_schema')
		ORDER BY table_schema, table_name, column_name;
	`

	// Start inspection
	done := d.inspectQuery("TablesByColsPlus", query, pq.Array(columns))
	rows, err := d.Conn().Query(query, pq.Array(columns))
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with columns %v", columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}
	defer rows.Close()

	// Group results by table
	tableMap := make(map[string]*TableColsSpecMeta)
	for rows.Next() {
		var col ColsDef
		if err := rows.Scan(&col.SchemaName, &col.TableName, &col.ColumnName, &col.DataType, &col.IsNullable); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v", columns),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
			return stats, response.Reply()
		}

		key := col.SchemaName + "." + col.TableName
		if tableMap[key] == nil {
			tableMap[key] = &TableColsSpecMeta{
				TableName:      col.TableName,
				SchemaName:     col.SchemaName,
				MatchedColumns: []ColsDef{},
				TotalRequested: len(columns),
			}
		}
		tableMap[key].MatchedColumns = append(tableMap[key].MatchedColumns, col)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating results for columns %v", columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableSearchByCols, EventLevelError, response.Reply())
		return stats, response.Reply()
	}

	// Build result with missing columns info
	for _, detail := range tableMap {
		matchedSet := make(map[string]bool)
		for _, col := range detail.MatchedColumns {
			matchedSet[col.ColumnName] = true
		}

		var missing []string
		for _, col := range columns {
			if !matchedSet[col] {
				missing = append(missing, col)
			}
		}

		detail.MissingColumns = missing
		detail.MatchedCount = len(detail.MatchedColumns)
		detail.IsFullMatch = len(missing) == 0
		stats = append(stats, *detail)
	}

	// Sort results: full matches first, then by match count descending
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].IsFullMatch != stats[j].IsFullMatch {
			return stats[i].IsFullMatch
		}
		if stats[i].MatchedCount != stats[j].MatchedCount {
			return stats[i].MatchedCount > stats[j].MatchedCount
		}
		return stats[i].SchemaName+"."+stats[i].TableName < stats[j].SchemaName+"."+stats[j].TableName
	})

	fullMatchCount := 0
	for _, r := range stats {
		if r.IsFullMatch {
			fullMatchCount++
		}
	}

	d.dispatch_event(EventTableSearchByCols, EventLevelSuccess, response.Reply())
	return stats, wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) with matches (%d full match(es)) for %d column(s)", len(stats), fullMatchCount, len(columns)),
		stats,
	).WithTotal(len(stats)).Reply()
}

// TablePrivs retrieves the privileges granted on specified tables for specified privilege types.
//
// This function queries the information_schema.role_table_grants to find all privilege grants
// matching the given tables and privilege types.  It returns detailed privilege information
// along with statistics about which tables have and don't have the requested privileges.
//
// Parameters:
//   - tables:     A slice of table names to check privileges for.
//   - privileges: A slice of privilege types to check (e.g., "SELECT", "INSERT", "UPDATE", "DELETE").
//
// Returns:
//   - A TablePrivilegesSpecMeta containing the list of privileges and statistics.
//   - A wrapify. R instance that encapsulates either the result or an error message.
func (d *Datasource) TablePrivs(tables []string, privileges []string) (privs_spec TablePrivsSpecMeta, response wrapify.R) {
	if !d.IsConnected() {
		return privs_spec, d.State()
	}

	if len(tables) == 0 {
		response := wrapify.WrapBadRequest("No tables provided for privilege check", nil).BindCause()
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}

	if len(privileges) == 0 {
		response := wrapify.WrapBadRequest("No privilege types provided for check", nil).BindCause()
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}

	// Normalize privilege types to uppercase
	normalizedPrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		normalizedPrivileges[i] = strings.ToUpper(strings.TrimSpace(p))
	}

	query := `
		SELECT grantee, privilege_type, table_name
		FROM information_schema.role_table_grants
		WHERE table_name = ANY($1)
		  AND privilege_type = ANY($2)
		ORDER BY table_name, privilege_type, grantee;
	`

	// Start inspection
	done := d.inspectQuery("TablePrivs", query, pq.Array(tables), pq.Array(normalizedPrivileges))
	rows, err := d.Conn().Query(query, pq.Array(tables), pq.Array(normalizedPrivileges))
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while retrieving privileges for tables %v", tables),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}
	defer rows.Close()

	// Track which tables have privileges
	tablesWithPrivileges := make(map[string]bool)

	for rows.Next() {
		var priv PrivsDef
		if err := rows.Scan(&priv.Grantee, &priv.PrivilegeType, &priv.TableName); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning privilege results for tables %v", tables),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
			return privs_spec, response.Reply()
		}
		privs_spec.Privileges = append(privs_spec.Privileges, priv)
		tablesWithPrivileges[priv.TableName] = true
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating privilege results for tables %v", tables),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}

	// Build statistics
	privs_spec.Stats.TotalRequested = len(tables)

	for _, table := range tables {
		if tablesWithPrivileges[table] {
			privs_spec.Stats.TablesWithPrivileges = append(privs_spec.Stats.TablesWithPrivileges, table)
		} else {
			privs_spec.Stats.TablesWithoutPrivilege = append(privs_spec.Stats.TablesWithoutPrivilege, table)
		}
	}

	// Sort the lists for consistent output
	sort.Strings(privs_spec.Stats.TablesWithPrivileges)
	sort.Strings(privs_spec.Stats.TablesWithoutPrivilege)

	privs_spec.Stats.TotalWithPrivilege = len(privs_spec.Stats.TablesWithPrivileges)
	privs_spec.Stats.TotalWithoutPrivilege = len(privs_spec.Stats.TablesWithoutPrivilege)

	d.dispatch_event(EventTablePrivileges, EventLevelSuccess, response.Reply())
	return privs_spec, wrapify.WrapOk(
		fmt.Sprintf("Retrieved privileges for %d table(s): %d with privileges, %d without privileges",
			len(tables), privs_spec.Stats.TotalWithPrivilege, privs_spec.Stats.TotalWithoutPrivilege),
		privs_spec,
	).WithTotal(len(privs_spec.Privileges)).Reply()
}

// TableAllPrivs retrieves all standard privileges for the specified tables.
//
// This function is a convenience wrapper around TablePrivs that requests all common
// privilege types: SELECT, INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, and TRIGGER.
//
// Parameters:
//   - tables: A slice of table names to check privileges for.
//
// Returns:
//   - A TablePrivsSpecMeta containing the list of privileges and statistics.
//   - A wrapify. R instance that encapsulates either the result or an error message.
func (d *Datasource) TableAllPrivs(tables ...string) (privs_spec TablePrivsSpecMeta, response wrapify.R) {
	privileges := []string{"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"}
	return d.TablePrivs(tables, privileges)
}

// TablePrivsByUser retrieves privileges for specific tables filtered by a specific grantee (user/role).
//
// This function is similar to TablePrivs but adds an additional filter for a specific user or role.
//
// Parameters:
//   - tables:     A slice of table names to check privileges for.
//   - privileges: A slice of privilege types to check (e.g., "SELECT", "INSERT", "UPDATE", "DELETE").
//   - grantee:    The name of the user or role to filter privileges by.
//
// Returns:
//   - A TablePrivsSpecMeta containing the list of privileges and statistics.
//   - A wrapify. R instance that encapsulates either the result or an error message.
func (d *Datasource) TablePrivsByUser(tables []string, privileges []string, grantee string) (privs_spec TablePrivsSpecMeta, response wrapify.R) {
	if !d.IsConnected() {
		return privs_spec, d.State()
	}

	if len(tables) == 0 {
		response := wrapify.WrapBadRequest("No tables provided for privilege check", nil).BindCause()
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}

	if len(privileges) == 0 {
		response := wrapify.WrapBadRequest("No privilege types provided for check", nil).BindCause()
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}

	if isEmpty(grantee) {
		response := wrapify.WrapBadRequest("Grantee (user/role) name is required", nil).BindCause()
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}

	// Normalize privilege types to uppercase
	normalizedPrivileges := make([]string, len(privileges))
	for i, p := range privileges {
		normalizedPrivileges[i] = strings.ToUpper(strings.TrimSpace(p))
	}

	query := `
		SELECT grantee, privilege_type, table_name
		FROM information_schema.role_table_grants
		WHERE table_name = ANY($1)
		  AND privilege_type = ANY($2)
		  AND grantee = $3
		ORDER BY table_name, privilege_type, grantee;
	`

	// Start inspection
	done := d.inspectQuery("TablePrivsByUser", query, pq.Array(tables), pq.Array(normalizedPrivileges), grantee)
	rows, err := d.Conn().Query(query, pq.Array(tables), pq.Array(normalizedPrivileges), grantee)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while retrieving privileges for tables %v and grantee '%s'", tables, grantee),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}
	defer rows.Close()

	// Track which tables have privileges
	tablesWithPrivs := make(map[string]bool)

	for rows.Next() {
		var priv PrivsDef
		if err := rows.Scan(&priv.Grantee, &priv.PrivilegeType, &priv.TableName); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning privilege results for tables %v", tables),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
			return privs_spec, response.Reply()
		}
		privs_spec.Privileges = append(privs_spec.Privileges, priv)
		tablesWithPrivs[priv.TableName] = true
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating privilege results for tables %v", tables),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTablePrivileges, EventLevelError, response.Reply())
		return privs_spec, response.Reply()
	}

	// Build statistics
	privs_spec.Stats.TotalRequested = len(tables)

	for _, table := range tables {
		if tablesWithPrivs[table] {
			privs_spec.Stats.TablesWithPrivileges = append(privs_spec.Stats.TablesWithPrivileges, table)
		} else {
			privs_spec.Stats.TablesWithoutPrivilege = append(privs_spec.Stats.TablesWithoutPrivilege, table)
		}
	}

	// Sort the lists for consistent output
	sort.Strings(privs_spec.Stats.TablesWithPrivileges)
	sort.Strings(privs_spec.Stats.TablesWithoutPrivilege)

	privs_spec.Stats.TotalWithPrivilege = len(privs_spec.Stats.TablesWithPrivileges)
	privs_spec.Stats.TotalWithoutPrivilege = len(privs_spec.Stats.TablesWithoutPrivilege)

	d.dispatch_event(EventTablePrivileges, EventLevelSuccess, response.Reply())
	return privs_spec, wrapify.WrapOk(
		fmt.Sprintf("Retrieved privileges for grantee '%s' on %d table(s): %d with privileges, %d without privileges",
			grantee, len(tables), privs_spec.Stats.TotalWithPrivilege, privs_spec.Stats.TotalWithoutPrivilege),
		privs_spec,
	).WithTotal(len(privs_spec.Privileges)).Reply()
}

// ColsExists checks the existence of specified columns across specified tables.
//
// This function performs a cross-check between a list of tables and a list of columns,
// determining whether each column exists in each table.  It returns detailed results
// for each table-column combination along with statistics about existing and missing columns.
//
// The function queries the information_schema.columns to verify column existence
// in the 'public' schema.
//
// Parameters:
//   - tables:  A slice of table names to check.
//   - columns: A slice of column names to check for existence in each table.
//
// Returns:
//   - A ColExistsSpecMeta containing all check results and statistics.
//   - A wrapify.R instance that encapsulates either the result or an error message.
func (d *Datasource) ColsExists(tables []string, columns []string) (ces ColExistsSpecMeta, response wrapify.R) {
	if !d.IsConnected() {
		return ces, d.State()
	}

	if len(tables) == 0 {
		response := wrapify.WrapBadRequest("No tables provided for column existence check", nil).BindCause()
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}

	if len(columns) == 0 {
		response := wrapify.WrapBadRequest("No columns provided for existence check", nil).BindCause()
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}

	query := `
		WITH tables_to_check AS (
			SELECT unnest($1::text[]) as table_name
		),
		columns_to_check AS (
			SELECT unnest($2::text[]) as column_name
		)
		SELECT 
			t. table_name,
			col.column_name,
			CASE 
				WHEN ic.column_name IS NOT NULL THEN 'exists'
				ELSE 'does not exist'
			END as status
		FROM tables_to_check t
		CROSS JOIN columns_to_check col
		LEFT JOIN information_schema.columns ic 
			ON ic.table_name = t.table_name 
			AND ic.column_name = col.column_name
			AND ic.table_schema = 'public'
		ORDER BY t. table_name, col.column_name;
	`

	// Start inspection
	done := d.inspectQuery("ColsExists", query, pq.Array(tables), pq.Array(columns))
	rows, err := d.Conn().Query(query, pq.Array(tables), pq.Array(columns))
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while checking column existence for tables %v and columns %v", tables, columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}
	defer rows.Close()

	for rows.Next() {
		var r ColExistsDef
		if err := rows.Scan(&r.TableName, &r.ColumnName, &r.Status); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning column existence results for tables %v", tables),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
			return ces, response.Reply()
		}

		r.Exists = r.Status == "exists"
		ces.Cols = append(ces.Cols, r)

		// Categorize into existing and missing
		if r.Exists {
			ces.Stats.ExistingCols = append(ces.Stats.ExistingCols, r)
		} else {
			ces.Stats.MissingCols = append(ces.Stats.MissingCols, r)
		}
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating column existence results for tables %v", tables),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}

	// Build statistics
	ces.Stats.TotalChecked = len(ces.Cols)
	ces.Stats.TotalExisting = len(ces.Stats.ExistingCols)
	ces.Stats.TotalMissing = len(ces.Stats.MissingCols)

	d.dispatch_event(EventTableColsExists, EventLevelSuccess, response.Reply())
	return ces, wrapify.WrapOk(
		fmt.Sprintf("Checked %d table-column combination(s): %d existing, %d missing",
			ces.Stats.TotalChecked, ces.Stats.TotalExisting, ces.Stats.TotalMissing),
		ces,
	).WithTotal(ces.Stats.TotalChecked).Reply()
}

// ColsExistsIn checks the existence of specified columns across specified tables within a specific schema.
//
// This function is similar to ColsExists but allows specifying a custom schema instead of defaulting to 'public'.
//
// Parameters:
//   - schema:  The schema name to check columns in.
//   - tables:  A slice of table names to check.
//   - columns: A slice of column names to check for existence in each table.
//
// Returns:
//   - A ColExistsSpecMeta containing all check results and statistics.
//   - A wrapify.R instance that encapsulates either the result or an error message.
func (d *Datasource) ColsExistsIn(schema string, tables []string, columns []string) (ces ColExistsSpecMeta, response wrapify.R) {
	if !d.IsConnected() {
		return ces, d.State()
	}

	if isEmpty(schema) {
		response := wrapify.WrapBadRequest("Schema name is required", nil).BindCause()
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}

	if len(tables) == 0 {
		response := wrapify.WrapBadRequest("No tables provided for column existence check", nil).BindCause()
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}

	if len(columns) == 0 {
		response := wrapify.WrapBadRequest("No columns provided for existence check", nil).BindCause()
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}

	query := `
		WITH tables_to_check AS (
			SELECT unnest($1::text[]) as table_name
		),
		columns_to_check AS (
			SELECT unnest($2::text[]) as column_name
		)
		SELECT 
			t. table_name,
			col.column_name,
			CASE 
				WHEN ic.column_name IS NOT NULL THEN 'exists'
				ELSE 'does not exist'
			END as status
		FROM tables_to_check t
		CROSS JOIN columns_to_check col
		LEFT JOIN information_schema.columns ic 
			ON ic.table_name = t.table_name 
			AND ic.column_name = col.column_name
			AND ic.table_schema = $3
		ORDER BY t.table_name, col.column_name;
	`

	// Start inspection
	done := d.inspectQuery("ColsExistsIn", query, pq.Array(tables), pq.Array(columns), schema)
	rows, err := d.Conn().Query(query, pq.Array(tables), pq.Array(columns), schema)
	// End inspection
	done()

	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while checking column existence in schema '%s' for tables %v and columns %v", schema, tables, columns),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}
	defer rows.Close()

	for rows.Next() {
		var r ColExistsDef
		if err := rows.Scan(&r.TableName, &r.ColumnName, &r.Status); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning column existence results in schema '%s' for tables %v", schema, tables),
				nil,
			).WithErrSck(err)
			d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
			return ces, response.Reply()
		}

		r.Exists = r.Status == "exists"
		ces.Cols = append(ces.Cols, r)

		// Categorize into existing and missing
		if r.Exists {
			ces.Stats.ExistingCols = append(ces.Stats.ExistingCols, r)
		} else {
			ces.Stats.MissingCols = append(ces.Stats.MissingCols, r)
		}
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating column existence results in schema '%s' for tables %v", schema, tables),
			nil,
		).WithErrSck(err)
		d.dispatch_event(EventTableColsExists, EventLevelError, response.Reply())
		return ces, response.Reply()
	}

	// Build statistics
	ces.Stats.TotalChecked = len(ces.Cols)
	ces.Stats.TotalExisting = len(ces.Stats.ExistingCols)
	ces.Stats.TotalMissing = len(ces.Stats.MissingCols)

	d.dispatch_event(EventTableColsExists, EventLevelSuccess, response.Reply())
	return ces, wrapify.WrapOk(
		fmt.Sprintf("Checked %d table-column combination(s) in schema '%s': %d existing, %d missing",
			ces.Stats.TotalChecked, schema, ces.Stats.TotalExisting, ces.Stats.TotalMissing),
		ces,
	).WithTotal(ces.Stats.TotalChecked).Reply()
}
