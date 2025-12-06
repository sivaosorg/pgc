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
// Note: This is for display purposes only and should NOT be used for actual query execution
// as it does not properly escape values and could be vulnerable to SQL injection.
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
// It returns the formatted string representation of the argument.
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

// formatString formats a string value with proper escaping.
func formatString(s string) string {
	escaped := strings.ReplaceAll(s, "'", "''")
	return fmt.Sprintf("'%s'", escaped)
}

// formatStringArray formats a string slice as PostgreSQL ARRAY literal.
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

// formatIntArray formats an int slice as PostgreSQL ARRAY literal.
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

// formatInt64Array formats an int64 slice as PostgreSQL ARRAY literal.
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

// formatFloat64Array formats a float64 slice as PostgreSQL ARRAY literal.
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

// formatBoolArray formats a bool slice as PostgreSQL ARRAY literal.
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

// formatInterfaceArray formats an interface{} slice as PostgreSQL ARRAY literal.
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

// formatGenericArray formats a generic array (from pq.Array) as PostgreSQL ARRAY literal.
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

// formatReflectSlice formats a slice using reflection as PostgreSQL ARRAY literal.
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
func cleanupQuery(query string) string {
	re := regexp.MustCompile(`\s+`)
	query = re.ReplaceAllString(query, " ")
	query = strings.TrimSpace(query)
	return query
}

// newQueryInspect creates a new QueryInspect instance.
func newQueryInspect(funcName, query string, args []any) QueryInspect {
	return QueryInspect{
		Query:      cleanupQuery(query),
		Args:       args,
		Completed:  interpolateQuery(query, args),
		ExecutedAt: time.Now(),
		FuncName:   funcName,
	}
}

// newQueryInspectWithDuration creates a new QueryInspect instance with duration.
func newQueryInspectWithDuration(funcName, query string, args []any, duration time.Duration) QueryInspect {
	q := newQueryInspect(funcName, query, args)
	q.Duration = duration
	return q
}
