package pgc

import (
	"fmt"

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
