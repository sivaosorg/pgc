package pgc

import (
	"fmt"

	"github.com/sivaosorg/wrapify"

	"github.com/lib/pq"
)

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
