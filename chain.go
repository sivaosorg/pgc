package pgc

import (
	"time"

	"github.com/sivaosorg/loggy"
	"github.com/sivaosorg/wrapify"
)

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Default Chain Callbacks
// Provides pre-configured callback implementations for connection lifecycle
// observability and query inspection telemetry.
//_______________________________________________________________________

// DefaultReconnectChain returns a pre-configured callback function for comprehensive
// connection lifecycle observability.  This callback implements structured diagnostic
// telemetry during reconnection attempts, facilitating operational monitoring,
// distributed tracing correlation, and root cause analysis in production environments.
//
// The callback emits structured log entries adhering to observability best practices:
//   - Success events: Include trace identifiers, connection state, latency metrics,
//     and pool configuration for performance profiling and capacity planning.
//   - Error events: Include error classification, root cause analysis context,
//     diagnostic payload, and actionable recovery hints for incident response.
//
// Usage:
//
//	datasource.OnReconnect(DefaultReconnectChain())
//
// Log Output Format (Success):
//
//	[pgc.reconnect] request_id=<uuid> | state=connected | status=OK | latency=<duration> |
//	pool_config={max_idle: N, max_open: N, max_lifetime: <duration>} | msg=<message>
//
// Log Output Format (Error):
//
//	[pgc.reconnect] state=disconnected | error_class=<type> | root_cause=<error> |
//	diagnostic_context=<map> | recovery_hint=<actionable_guidance>
func DefaultReconnectChain() func(response wrapify.R, chain *Datasource) {
	return func(response wrapify.R, chain *Datasource) {
		if response.IsSuccess() {
			loggy.Infof("[pgc.reconnect] request_id=%s | state=connected | status=%s | latency=%v | pool_config={max_idle: %d, max_open: %d, max_lifetime: %v} | msg=%s",
				response.Meta().RequestID(),
				response.Reply().StatusText(),
				chain.State().OnKeyDebugging("ping_executed_in"),
				chain.conf.MaxIdleConn(),
				chain.conf.MaxOpenConn(),
				chain.conf.ConnMaxLifetime(),
				response.Message())
		}
		if response.IsError() {
			loggy.Errorf("[pgc.reconnect] request_id=%s | state=disconnected | status=%s | latency=%v | error_class=%T | root_cause=%v | diagnostic_context=%v | recovery_hint=%s",
				response.Meta().RequestID(),
				response.Reply().StatusText(),
				chain.State().OnKeyDebugging("reconnect_executed_in"),
				response.Cause(),
				response.Error(),
				response.Debugging(),
				"verify network connectivity, authentication credentials, and postgresql server availability")
		}
	}
}

// DefaultInspectorChain returns a pre-configured callback function for SQL query
// inspection and execution telemetry. This callback implements structured logging
// for all executed queries, enabling query performance profiling, slow query detection,
// and comprehensive audit trails for debugging and compliance requirements.
//
// The callback emits structured log entries containing:
//   - Function context: The originating function name for call stack traceability
//   - Execution duration: Precise timing metrics for performance analysis and SLO monitoring
//   - Query details: The fully interpolated SQL statement for debugging and audit purposes
//
// Usage:
//
//	datasource.OnInspector(DefaultInspectorChain())
//
// Log Output Format:
//
//	[pgc.sql.inspector] func=<function_name> | duration=<execution_time> | query=<interpolated_sql>
//
// Security Consideration:
//
//	The interpolated query may contain sensitive parameter values.  Ensure log
//	destinations are appropriately secured and consider implementing parameter
//	masking for PII/sensitive data in production environments.
func DefaultInspectorChain() func(ins QueryInspect) {
	return func(ins QueryInspect) {
		loggy.Infof("[pgc.sql.inspector] func=%s | duration=%v | query=%s",
			ins.FuncName(),
			ins.Duration(),
			ins.Completed())
	}
}

// DefaultInspectorChainWithThreshold returns a query inspector callback that logs
// all queries and emits warnings for queries exceeding the specified duration threshold.
// This enables proactive slow query detection and performance regression monitoring.
//
// Parameters:
//   - threshold: The duration threshold above which queries are flagged as slow.
//     Queries exceeding this threshold will be logged at WARN level.
//
// Usage:
//
//	// Flag queries taking longer than 100ms as slow
//	datasource.OnInspector(DefaultInspectorChainWithThreshold(100 * time.Millisecond))
//
// Log Output Format (Normal):
//
//	[pgc.sql.inspector] func=<function_name> | duration=<execution_time> | query=<interpolated_sql>
//
// Log Output Format (Slow Query):
//
//	[pgc.sql.inspector.slow] func=<function_name> | duration=<execution_time> |
//	threshold=<configured_threshold> | query=<interpolated_sql> |
//	recommendation=consider_query_optimization_or_indexing
func DefaultInspectorChainWithThreshold(threshold time.Duration) func(ins QueryInspect) {
	return func(ins QueryInspect) {
		loggy.Infof("[pgc.sql.inspector] func=%s | duration=%v | query=%s",
			ins.FuncName(),
			ins.Duration(),
			ins.Completed())

		if ins.Duration() > threshold {
			loggy.Warnf("[pgc.sql.inspector.slow] func=%s | duration=%v | threshold=%v | query=%s | recommendation=%s",
				ins.FuncName(),
				ins.Duration(),
				threshold,
				ins.Completed(),
				"consider_query_optimization_or_indexing")
		}
	}
}

// DefaultInspectorCallbackVerbose returns a verbose query inspector callback that logs
// comprehensive query execution details including raw query template, interpolated query,
// argument count, and execution metadata. This is intended for development, debugging,
// and detailed audit logging scenarios.
//
// Usage:
//
//	datasource.OnInspector(DefaultInspectorCallbackVerbose())
//
// Log Output Format:
//
//	[pgc.sql.inspector] func=<function_name> | duration=<execution_time> |
//	executed_at=<timestamp> | arg_count=<N> | query_template=<raw_sql> | query_interpolated=<interpolated_sql>
func DefaultInspectorCallbackVerbose() func(ins QueryInspect) {
	return func(ins QueryInspect) {
		loggy.Infof("[pgc.sql.inspector] func=%s | duration=%v | executed_at=%s | arg_count=%d | query_template=%s | query_interpolated=%s",
			ins.FuncName(),
			ins.Duration(),
			ins.ExecutedAt().Format(defaultTimeFormat),
			len(ins.Args()),
			ins.Query(),
			ins.Completed())
	}
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Event Chains
//_______________________________________________________________________

// DefaultEventCallbackChain returns a pre-configured callback function for comprehensive
// datasource event observability. This callback implements structured logging for
// all significant datasource events including transactions, connection lifecycle,
// table operations, function/procedure invocations, and query inspections.
//
// The callback correlates event keys with severity levels to emit appropriately
// leveled log entries, enabling fine-grained filtering in log aggregation systems
// and alerting pipelines.
//
// Usage:
//
//	datasource.OnEvent(DefaultEventCallbackChain())
//
// Log Output Format:
//
//	[pgc.event] event=<event_key> | request_id=<uuid> |
//	status=<status_text> | message=<message>
//
// Supported Event Categories:
//   - Transaction events: begin, commit, rollback, savepoint
//   - Connection events: open, close, retry, ping
//   - Table events: listing, definition, keys/indexes, privileges
//   - Function/Procedure events: listing, metadata, definition
//   - Query events: inspection
func DefaultEventCallbackChain() func(event EventKey, level EventLevel, response wrapify.R) {
	return func(event EventKey, level EventLevel, response wrapify.R) {
		switch level {
		case EventLevelDebug:
			loggy.Debugf("[pgc.event] event=%s | request_id=%s | status=%s | message=%s",
				event, response.Meta().RequestID(), response.StatusText(), response.Message())
		case EventLevelInfo:
			loggy.Infof("[pgc.event] event=%s | request_id=%s | status=%s | message=%s",
				event, response.Meta().RequestID(), response.StatusText(), response.Message())
		case EventLevelWarn:
			loggy.Warnf("[pgc.event] event=%s | request_id=%s | status=%s | message=%s",
				event, response.Meta().RequestID(), response.StatusText(), response.Message())
		case EventLevelError:
			loggy.Errorf("[pgc.event] event=%s | request_id=%s | status=%s | message=%s",
				event, response.Meta().RequestID(), response.StatusText(), response.Message())
		default:
			loggy.Infof("[pgc.event] event=%s | request_id=%s | status=%s | message=%s",
				event, response.Meta().RequestID(), response.StatusText(), response.Message())
		}
	}
}
