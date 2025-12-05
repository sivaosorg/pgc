package pgc

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// Inspect implements the QueryInspector interface for QueryInspectorFunc.
func (f QueryInspectorFunc) Inspect(q QueryInspect) {
	f(q)
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
		result = strings.Replace(result, placeholder, value, -1)
	}

	return cleanupQuery(result)
}

// formatArgValue formats an argument value for SQL display.
func formatArgValue(arg any) string {
	if arg == nil {
		return "NULL"
	}

	switch v := arg.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case []string:
		quoted := make([]string, len(v))
		for i, s := range v {
			quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
		}
		return fmt.Sprintf("ARRAY[%s]", strings.Join(quoted, ", "))
	case []byte:
		return fmt.Sprintf("'%s'", string(v))
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format(defaultTimeFormat))
	default:
		// Handle pq.Array and other types
		str := fmt.Sprintf("%v", v)
		// Check if it's an array type from pq
		if strings.HasPrefix(str, "{") && strings.HasSuffix(str, "}") {
			inner := str[1 : len(str)-1]
			if inner == "" {
				return "ARRAY[]::text[]"
			}
			parts := strings.Split(inner, ",")
			quoted := make([]string, len(parts))
			for i, p := range parts {
				quoted[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(strings.TrimSpace(p), "'", "''"))
			}
			return fmt.Sprintf("ARRAY[%s]", strings.Join(quoted, ", "))
		}
		return fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''"))
	}
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
		Query:      query,
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
