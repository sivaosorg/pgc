package pgc

import (
	"fmt"
	"sort"

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
func (d *Datasource) Tables() wrapify.R {
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
func (d *Datasource) Functions() wrapify.R {
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
func (d *Datasource) Procedures() wrapify.R {
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
func (d *Datasource) FuncSpec(function string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var fsm []FuncSpecMeta
	err := d.Conn().Select(&fsm, `
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
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the function '%s' metadata", function), fsm).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved function '%s' metadata successfully", function), fsm).WithTotal(len(fsm)).Reply()
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
func (d *Datasource) FuncDef(function string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var def string
	err := d.Conn().QueryRow("SELECT pg_get_functiondef($1::regproc)", function).Scan(&def)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the function '%s' metadata", function), def).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved function '%s' metadata successfully", function), def).WithTotal(1).Reply()
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
func (d *Datasource) ProcDef(procedure string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	var def string
	err := d.Conn().QueryRow("SELECT pg_get_functiondef($1::regproc)", procedure).Scan(&def)
	if err != nil {
		response := wrapify.WrapInternalServerError(fmt.Sprintf("An error occurred while retrieving the procedure '%s' metadata", procedure), def).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	return wrapify.WrapOk(fmt.Sprintf("Retrieved procedure '%s' metadata successfully", procedure), def).WithTotal(1).Reply()
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
func (d *Datasource) TableKeys(table string) wrapify.R {
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
	var results []TableKeysMeta
	for rows.Next() {
		var m TableKeysMeta
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
func (d *Datasource) ColsSpec(table string) wrapify.R {
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
	var results []ColsSpecMeta
	for rows.Next() {
		var m ColsSpecMeta
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

// FindTablesWithColumns searches for tables that contain ALL specified columns.
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
func (d *Datasource) FindTablesWithColumns(columns []string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	if len(columns) == 0 {
		return wrapify.WrapBadRequest("No columns provided for search", nil).Reply()
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

	rows, err := d.Conn().Query(query, pq.Array(columns), len(columns))
	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with columns %v", columns),
			nil,
		).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	defer rows.Close()

	var results []TableWithColumns
	for rows.Next() {
		var r TableWithColumns
		var matchedCols pq.StringArray
		if err := rows.Scan(&r.SchemaName, &r.TableName, &matchedCols); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v", columns),
				nil,
			).WithErrSck(err)
			d.notify(response.Reply())
			return response.Reply()
		}
		r.MatchedColumns = []string(matchedCols)
		r.TotalColumns = len(columns)
		r.MatchedCount = len(r.MatchedColumns)
		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating results for columns %v", columns),
			nil,
		).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}

	return wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) containing all %d specified column(s)", len(results), len(columns)),
		results,
	).WithTotal(len(results)).Reply()
}

// FindTablesWithAnyColumns searches for tables that contain AT LEAST ONE of the specified columns.
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
func (d *Datasource) FindTablesWithAnyColumns(columns []string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	if len(columns) == 0 {
		return wrapify.WrapBadRequest("No columns provided for search", nil).Reply()
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

	rows, err := d.Conn().Query(query, pq.Array(columns))
	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with any columns %v", columns),
			nil,
		).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	defer rows.Close()

	var results []TableWithColumns
	for rows.Next() {
		var r TableWithColumns
		var matchedCols pq.StringArray
		if err := rows.Scan(&r.SchemaName, &r.TableName, &matchedCols); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v", columns),
				nil,
			).WithErrSck(err)
			d.notify(response.Reply())
			return response.Reply()
		}
		r.MatchedColumns = []string(matchedCols)
		r.TotalColumns = len(columns)
		r.MatchedCount = len(r.MatchedColumns)
		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating results for columns %v", columns),
			nil,
		).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}

	return wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) containing at least one of %d specified column(s)", len(results), len(columns)),
		results,
	).WithTotal(len(results)).Reply()
}

// FindTablesWithColumnsInSchema searches for tables containing ALL specified columns within a specific schema.
//
// Parameters:
//   - schema:  The name of the schema to search within.
//   - columns: A slice of column names to search for.
//
// Returns:
//   - A wrapify. R instance that encapsulates either a slice of TableWithColumns or an error message.
func (d *Datasource) FindTablesWithColumnsInSchema(schema string, columns []string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	if len(columns) == 0 {
		return wrapify.WrapBadRequest("No columns provided for search", nil).Reply()
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

	rows, err := d.Conn().Query(query, schema, pq.Array(columns), len(columns))
	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with columns %v in schema '%s'", columns, schema),
			nil,
		).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	defer rows.Close()

	var results []TableWithColumns
	for rows.Next() {
		var r TableWithColumns
		var matchedCols pq.StringArray
		if err := rows.Scan(&r.SchemaName, &r.TableName, &matchedCols); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v in schema '%s'", columns, schema),
				nil,
			).WithErrSck(err)
			d.notify(response.Reply())
			return response.Reply()
		}
		r.MatchedColumns = []string(matchedCols)
		r.TotalColumns = len(columns)
		r.MatchedCount = len(r.MatchedColumns)
		results = append(results, r)
	}

	if err := rows.Err(); err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while iterating results for columns %v in schema '%s'", columns, schema),
			nil,
		).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}

	return wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) in schema '%s' containing all %d specified column(s)", len(results), schema, len(columns)),
		results,
	).WithTotal(len(results)).Reply()
}

// FindTablesWithColumnsDetailed searches for tables and returns detailed information about column matches.
//
// This function provides comprehensive information including which columns were found,
// which were missing, and detailed metadata for each matched column.
//
// Parameters:
//   - columns: A slice of column names to search for.
//
// Returns:
//   - A wrapify. R instance containing detailed matching information.
func (d *Datasource) FindTablesWithColumnsDetailed(columns []string) wrapify.R {
	if !d.IsConnected() {
		return d.Wrap()
	}
	if len(columns) == 0 {
		return wrapify.WrapBadRequest("No columns provided for search", nil).Reply()
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

	rows, err := d.Conn().Query(query, pq.Array(columns))
	if err != nil {
		response := wrapify.WrapInternalServerError(
			fmt.Sprintf("An error occurred while searching for tables with columns %v", columns),
			nil,
		).WithErrSck(err)
		d.notify(response.Reply())
		return response.Reply()
	}
	defer rows.Close()

	// Group results by table
	tableMap := make(map[string]*TableColumnsDetail)
	for rows.Next() {
		var col ColumnExistsResult
		if err := rows.Scan(&col.SchemaName, &col.TableName, &col.ColumnName, &col.DataType, &col.IsNullable); err != nil {
			response := wrapify.WrapInternalServerError(
				fmt.Sprintf("An error occurred while scanning results for columns %v", columns),
				nil,
			).WithErrSck(err)
			d.notify(response.Reply())
			return response.Reply()
		}

		key := col.SchemaName + "." + col.TableName
		if tableMap[key] == nil {
			tableMap[key] = &TableColumnsDetail{
				TableName:      col.TableName,
				SchemaName:     col.SchemaName,
				MatchedColumns: []ColumnExistsResult{},
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
		d.notify(response.Reply())
		return response.Reply()
	}

	// Build result with missing columns info
	var results []TableColumnsDetail
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
		results = append(results, *detail)
	}

	// Sort results: full matches first, then by match count descending
	sort.Slice(results, func(i, j int) bool {
		if results[i].IsFullMatch != results[j].IsFullMatch {
			return results[i].IsFullMatch
		}
		if results[i].MatchedCount != results[j].MatchedCount {
			return results[i].MatchedCount > results[j].MatchedCount
		}
		return results[i].SchemaName+"."+results[i].TableName < results[j].SchemaName+"."+results[j].TableName
	})

	fullMatchCount := 0
	for _, r := range results {
		if r.IsFullMatch {
			fullMatchCount++
		}
	}

	return wrapify.WrapOk(
		fmt.Sprintf("Found %d table(s) with matches (%d full match(es)) for %d column(s)", len(results), fullMatchCount, len(columns)),
		results,
	).WithTotal(len(results)).Reply()
}
