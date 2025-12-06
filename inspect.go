package pgc

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Inspect implements the QueryInspector interface for QueryInspectorFunc.
// It allows a function to be used as a QueryInspector by delegating the inspection
// call to the underlying function.
//
// Parameters:
//   - query: The QueryInspect struct containing query details to be inspected.
//
// Example usage:
//
//	inspector := QueryInspectorFunc(func(q QueryInspect) {
//	    log.Printf("Query executed: %s", q.Completed())
//	})
//	inspector.Inspect(queryInspect)
func (f QueryInspectorFunc) Inspect(query QueryInspect) {
	f(query)
}

// interpolateQuery replaces PostgreSQL placeholders ($1, $2, etc.) with actual values
// to produce a complete SQL query string for debugging/logging purposes.
//
// The function processes placeholders in reverse order (from highest to lowest index)
// to prevent issues where $1 might incorrectly match part of $10, $11, etc.
//
// Parameters:
//   - query: The SQL query string containing PostgreSQL-style placeholders.
//   - args: The slice of argument values to substitute into the placeholders.
//
// Returns:
//   - A formatted SQL query string with all placeholders replaced by their values.
//
// Note: This is for display purposes only and should NOT be used for actual query execution
// as it does not properly escape values and could be vulnerable to SQL injection.
//
// Example:
//
//	query := "SELECT * FROM users WHERE id = $1 AND status = $2"
//	args := []any{123, "active"}
//	result := interpolateQuery(query, args)
//	// Result: "SELECT * FROM users WHERE id = 123 AND status = 'active'"
func interpolateQuery(query string, args []any) string {
	if len(args) == 0 {
		return cleanupQuery(query)
	}

	result := query
	for i := len(args) - 1; i >= 0; i-- {
		placeholder := fmt.Sprintf("$%d", i+1)
		value := formatArgValue(args[i])
		result = strings.ReplaceAll(result, placeholder, value)
	}

	return cleanupQuery(result)
}

// formatArgValue formats a single argument value for inclusion in the interpolated query string.
// It handles various data types including strings, numbers, booleans, time.Time, and arrays.
//
// The function provides comprehensive type handling for:
//   - nil values (returns "NULL")
//   - pq.Array wrapper types (StringArray, Int64Array, Float64Array, BoolArray)
//   - pq.GenericArray (from pq.Array() function)
//   - Primitive types (string, int, float, bool)
//   - time.Time values (formatted using defaultTimeFormat)
//   - Slice types ([]string, []int, []int64, []float64, []bool, []interface{})
//   - Generic slices via reflection
//
// Parameters:
//   - arg: The argument value to format. Can be any type.
//
// Returns:
//   - A string representation of the argument suitable for SQL query display.
//
// Example:
//
//	formatArgValue("hello")     // Returns: "'hello'"
//	formatArgValue(42)          // Returns: "42"
//	formatArgValue(true)        // Returns: "TRUE"
//	formatArgValue(nil)         // Returns: "NULL"
//	formatArgValue([]int{1,2})  // Returns: "ARRAY[1, 2]"
func formatArgValue(arg any) string {
	if arg == nil {
		return "NULL"
	}

	// Handle pq.Array wrapper types first
	switch v := arg.(type) {
	case pq.StringArray:
		return formatStringArray([]string(v))
	case *pq.StringArray:
		if v != nil {
			return formatStringArray([]string(*v))
		}
		return "NULL"
	case pq.Int64Array:
		return formatInt64Array([]int64(v))
	case *pq.Int64Array:
		if v != nil {
			return formatInt64Array([]int64(*v))
		}
		return "NULL"
	case pq.Float64Array:
		return formatFloat64Array([]float64(v))
	case *pq.Float64Array:
		if v != nil {
			return formatFloat64Array([]float64(*v))
		}
		return "NULL"
	case pq.BoolArray:
		return formatBoolArray([]bool(v))
	case *pq.BoolArray:
		if v != nil {
			return formatBoolArray([]bool(*v))
		}
		return "NULL"
	}

	// Handle pq.GenericArray (result of pq.Array())
	if ga, ok := arg.(pq.GenericArray); ok {
		return formatGenericArray(ga.A)
	}

	// Check if it's a pointer to pq.GenericArray
	rv := reflect.ValueOf(arg)
	if rv.Kind() == reflect.Ptr && !rv.IsNil() {
		if ga, ok := rv.Elem().Interface().(pq.GenericArray); ok {
			return formatGenericArray(ga.A)
		}
	}

	// Handle basic types
	switch v := arg.(type) {
	case string:
		return formatString(v)
	case []string:
		return formatStringArray(v)
	case []byte:
		return formatString(string(v))
	case int:
		return fmt.Sprintf("%d", v)
	case int8:
		return fmt.Sprintf("%d", v)
	case int16:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint8:
		return fmt.Sprintf("%d", v)
	case uint16:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%v", v)
	case float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format(defaultTimeFormat))
	case []int:
		return formatIntArray(v)
	case []int64:
		return formatInt64Array(v)
	case []float64:
		return formatFloat64Array(v)
	case []bool:
		return formatBoolArray(v)
	case []interface{}:
		return formatInterfaceArray(v)
	default:
		// Use reflection to handle other slice types
		rv := reflect.ValueOf(arg)
		if rv.Kind() == reflect.Slice {
			return formatReflectSlice(rv)
		}
		// Default: treat as string
		return formatString(fmt.Sprintf("%v", v))
	}
}

// formatString formats a string value with proper SQL escaping for single quotes.
// It escapes single quotes by doubling them and wraps the result in single quotes.
//
// Parameters:
//   - s: The string value to format.
//
// Returns:
//   - A properly escaped and quoted string for SQL queries.
//
// Example:
//
//	formatString("hello")       // Returns: "'hello'"
//	formatString("it's fine")   // Returns: "'it''s fine'"
//	formatString("")            // Returns: "''"
func formatString(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}

// formatStringArray formats a string slice as a PostgreSQL ARRAY literal.
// Each element is properly escaped and quoted.  Empty arrays are cast to text[].
//
// Parameters:
//   - arrays: The string slice to format.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation.
//
// Example:
//
//	formatStringArray([]string{"a", "b"})     // Returns: "ARRAY['a', 'b']"
//	formatStringArray([]string{"it's"})       // Returns: "ARRAY['it''s']"
//	formatStringArray([]string{})             // Returns: "ARRAY[]::text[]"
func formatStringArray(arrays []string) string {
	if len(arrays) == 0 {
		return "ARRAY[]::text[]"
	}
	quoted := make([]string, len(arrays))
	for i, s := range arrays {
		escaped := strings.ReplaceAll(s, "'", "''")
		quoted[i] = fmt.Sprintf("'%s'", escaped)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(quoted, ", "))
}

// formatIntArray formats an int slice as a PostgreSQL ARRAY literal.
// Empty arrays are cast to integer[] to maintain type information.
//
// Parameters:
//   - arrays: The int slice to format.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation.
//
// Example:
//
//	formatIntArray([]int{1, 2, 3})    // Returns: "ARRAY[1, 2, 3]"
//	formatIntArray([]int{})           // Returns: "ARRAY[]::integer[]"
func formatIntArray(arrays []int) string {
	if len(arrays) == 0 {
		return "ARRAY[]::integer[]"
	}
	parts := make([]string, len(arrays))
	for i, v := range arrays {
		parts[i] = fmt.Sprintf("%d", v)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(parts, ", "))
}

// formatInt64Array formats an int64 slice as a PostgreSQL ARRAY literal.
// Empty arrays are cast to bigint[] to maintain type information.
//
// Parameters:
//   - arrays: The int64 slice to format.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation.
//
// Example:
//
//	formatInt64Array([]int64{100, 200})   // Returns: "ARRAY[100, 200]"
//	formatInt64Array([]int64{})           // Returns: "ARRAY[]::bigint[]"
func formatInt64Array(arrays []int64) string {
	if len(arrays) == 0 {
		return "ARRAY[]::bigint[]"
	}
	parts := make([]string, len(arrays))
	for i, v := range arrays {
		parts[i] = fmt.Sprintf("%d", v)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(parts, ", "))
}

// formatFloat64Array formats a float64 slice as a PostgreSQL ARRAY literal.
// Empty arrays are cast to double precision[] to maintain type information.
//
// Parameters:
//   - arrays: The float64 slice to format.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation.
//
// Example:
//
//	formatFloat64Array([]float64{1.5, 2.7})   // Returns: "ARRAY[1.5, 2.7]"
//	formatFloat64Array([]float64{})           // Returns: "ARRAY[]::double precision[]"
func formatFloat64Array(arrays []float64) string {
	if len(arrays) == 0 {
		return "ARRAY[]::double precision[]"
	}
	parts := make([]string, len(arrays))
	for i, v := range arrays {
		parts[i] = fmt.Sprintf("%v", v)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(parts, ", "))
}

// formatBoolArray formats a bool slice as a PostgreSQL ARRAY literal.
// Boolean values are represented as TRUE or FALSE (uppercase).
// Empty arrays are cast to boolean[] to maintain type information.
//
// Parameters:
//   - arrays: The bool slice to format.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation.
//
// Example:
//
//	formatBoolArray([]bool{true, false})   // Returns: "ARRAY[TRUE, FALSE]"
//	formatBoolArray([]bool{})              // Returns: "ARRAY[]::boolean[]"
func formatBoolArray(arrays []bool) string {
	if len(arrays) == 0 {
		return "ARRAY[]::boolean[]"
	}
	parts := make([]string, len(arrays))
	for i, v := range arrays {
		if v {
			parts[i] = "TRUE"
		} else {
			parts[i] = "FALSE"
		}
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(parts, ", "))
}

// formatInterfaceArray formats a slice of interface{} (any) values as a PostgreSQL ARRAY literal.
// Each element is recursively formatted using formatArgValue to handle mixed types.
// Empty arrays are cast to text[] as a default type.
//
// Parameters:
//   - arrays: The interface{} slice to format.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation.
//
// Example:
//
//	formatInterfaceArray([]any{"a", 1, true})   // Returns: "ARRAY['a', 1, TRUE]"
//	formatInterfaceArray([]any{})               // Returns: "ARRAY[]::text[]"
func formatInterfaceArray(arrays []any) string {
	if len(arrays) == 0 {
		return "ARRAY[]::text[]"
	}
	parts := make([]string, len(arrays))
	for i, v := range arrays {
		parts[i] = formatArgValue(v)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(parts, ", "))
}

// formatGenericArray formats a generic array (typically from pq.Array()) as a PostgreSQL ARRAY literal.
// It handles nil values, pointer dereferencing, and delegates to formatReflectSlice for actual formatting.
//
// Parameters:
//   - arrays: The generic array value to format.  Can be any slice type or pointer to slice.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation, or "NULL" for nil values.
//
// Example:
//
//	formatGenericArray([]string{"a", "b"})   // Returns: "ARRAY['a', 'b']"
//	formatGenericArray(nil)                  // Returns: "NULL"
func formatGenericArray(arrays any) string {
	if arrays == nil {
		return "NULL"
	}

	rv := reflect.ValueOf(arrays)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return "NULL"
		}
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Slice {
		return formatString(fmt.Sprintf("%v", arrays))
	}

	return formatReflectSlice(rv)
}

// formatReflectSlice formats a slice using reflection as a PostgreSQL ARRAY literal.
// This function is used when the slice type is not known at compile time.
// Each element is recursively formatted using formatArgValue.
//
// Parameters:
//   - rv: A reflect.Value representing a slice.
//
// Returns:
//   - A PostgreSQL ARRAY literal string representation.
//
// Example:
//
//	rv := reflect.ValueOf([]int{1, 2, 3})
//	formatReflectSlice(rv)   // Returns: "ARRAY[1, 2, 3]"
func formatReflectSlice(rv reflect.Value) string {
	if rv.Len() == 0 {
		return "ARRAY[]::text[]"
	}

	parts := make([]string, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		elem := rv.Index(i).Interface()
		parts[i] = formatArgValue(elem)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(parts, ", "))
}

// cleanupQuery removes extra whitespace and formats the query for better readability.
// It collapses multiple consecutive whitespace characters (spaces, tabs, newlines) into
// a single space and trims leading/trailing whitespace.
//
// Parameters:
//   - query: The SQL query string to clean up.
//
// Returns:
//   - A cleaned query string with normalized whitespace.
//
// Example:
//
//	cleanupQuery("SELECT *   FROM\n  users")   // Returns: "SELECT * FROM users"
//	cleanupQuery("  SELECT * FROM users  ")    // Returns: "SELECT * FROM users"
func cleanupQuery(query string) string {
	re := regexp.MustCompile(`\s+`)
	query = re.ReplaceAllString(query, " ")
	query = strings.TrimSpace(query)
	return query
}

// newQueryInspect creates a new QueryInspect instance with the provided query details.
// It initializes the struct with a cleaned query, the completed (interpolated) query,
// and sets the execution timestamp to the current time.
//
// Parameters:
//   - funcName: The name of the function that executed the query (for tracing/debugging).
//   - query: The original SQL query string with placeholders.
//   - args: The slice of argument values used in the query.
//
// Returns:
//   - A QueryInspect struct populated with query metadata.
//
// Example:
//
//	inspect := newQueryInspect("GetUserByID", "SELECT * FROM users WHERE id = $1", []any{123})
//	// inspect.Query: "SELECT * FROM users WHERE id = $1"
//	// inspect. Completed: "SELECT * FROM users WHERE id = 123"
//	// inspect.FuncName: "GetUserByID"
func newQueryInspect(funcName, query string, args []any) QueryInspect {
	return QueryInspect{
		query:      cleanupQuery(query),
		args:       args,
		completed:  interpolateQuery(query, args),
		executedAt: time.Now(),
		funcName:   funcName,
	}
}

// newQueryInspectWithDuration creates a new QueryInspect instance with the provided query details
// and execution duration.  This is useful for performance monitoring and query profiling.
//
// Parameters:
//   - funcName: The name of the function that executed the query (for tracing/debugging).
//   - query: The original SQL query string with placeholders.
//   - args: The slice of argument values used in the query.
//   - duration: The time. Duration representing how long the query took to execute.
//
// Returns:
//   - A QueryInspect struct populated with query metadata including execution duration.
//
// Example:
//
//	start := time.Now()
//	// ... execute query ...
//	duration := time.Since(start)
//	inspect := newQueryInspectWithDuration("GetUserByID", "SELECT * FROM users WHERE id = $1", []any{123}, duration)
//	// inspect.Duration contains the execution time
func newQueryInspectWithDuration(funcName, query string, args []any, duration time.Duration) QueryInspect {
	q := newQueryInspect(funcName, query, args)
	q.duration = duration
	return q
}
