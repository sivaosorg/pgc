# EventBus Examples

This document provides comprehensive examples of using the EventBus system in pgc.

## Overview

The EventBus system provides a flexible, decoupled way to handle datasource events using a publish-subscribe pattern. It supports:

- **Topic-based filtering**: Subscribe to specific event types using topics like "query", "transaction", "connection"
- **Wildcard matching**: Use wildcard topics like "query.*" to match all query-related events
- **Async/Sync delivery**: Choose between asynchronous and synchronous event delivery
- **Custom filtering**: Apply custom filters to events before delivery
- **Thread-safe operations**: All operations are thread-safe and can be used concurrently
- **Graceful shutdown**: Properly shutdown the EventBus to ensure all events are processed

## Basic Usage

### 1. Create and Configure EventBus

```go
package main

import (
    "fmt"
    "github.com/sivaosorg/pgc"
)

func main() {
    // Create EventBus with default configuration (4 workers, buffer size 100)
    bus := pgc.NewEventBus()
    defer bus.Shutdown() // Always shutdown gracefully

    // Or create with custom configuration
    bus := pgc.NewEventBusWithConfig(pgc.EventBusConfig{
        WorkerCount: 8,    // 8 worker goroutines
        BufferSize:  200,  // Buffer up to 200 events
    })
}
```

### 2. Connect EventBus to Datasource

```go
// Create datasource
conf := pgc.NewSettings()
conf.SetEnable(true).
    SetHost("localhost").
    SetPort(5432).
    SetUser("postgres").
    SetPassword("password").
    SetDatabase("mydb").
    SetSslMode("disable")

client := pgc.NewClient(*conf)

// Attach EventBus to datasource
client.SetEventBus(bus)

// Now all datasource events will be published to the EventBus
```

## Example 1: Metrics Collector

Collect metrics on database operations:

```go
package main

import (
    "fmt"
    "sync"
    "time"
    
    "github.com/sivaosorg/pgc"
)

// MetricsCollector collects and reports database metrics
type MetricsCollector struct {
    mu              sync.Mutex
    queryCount      int
    txCount         int
    errorCount      int
    totalQueryTime  time.Duration
}

func NewMetricsCollector() *MetricsCollector {
    return &MetricsCollector{}
}

func (mc *MetricsCollector) HandleEvent(event pgc.Event) {
    mc.mu.Lock()
    defer mc.mu.Unlock()

    switch event.Level() {
    case pgc.EventLevelError:
        mc.errorCount++
    case pgc.EventLevelSuccess:
        // Track successful operations
        if event.Key() == pgc.EventQueryInspect {
            mc.queryCount++
        }
        if event.Key() == pgc.EventTxCommit {
            mc.txCount++
        }
    }
}

func (mc *MetricsCollector) Report() {
    mc.mu.Lock()
    defer mc.mu.Unlock()
    
    fmt.Printf("=== Database Metrics ===\n")
    fmt.Printf("Queries: %d\n", mc.queryCount)
    fmt.Printf("Transactions: %d\n", mc.txCount)
    fmt.Printf("Errors: %d\n", mc.errorCount)
    fmt.Printf("=======================\n")
}

func main() {
    // Setup
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    collector := NewMetricsCollector()
    
    // Subscribe to all events
    bus.Subscribe(pgc.TopicAll, collector.HandleEvent)
    
    // Setup datasource
    conf := pgc.NewSettings()
    conf.SetEnable(true).SetHost("localhost").SetPort(5432)
    client := pgc.NewClient(*conf)
    client.SetEventBus(bus)
    
    // Perform database operations...
    // (queries, transactions, etc.)
    
    // Report metrics
    time.Sleep(1 * time.Second) // Wait for async events
    collector.Report()
}
```

## Example 2: Audit Logger

Log all database operations for compliance:

```go
package main

import (
    "fmt"
    "log"
    "os"
    
    "github.com/sivaosorg/pgc"
)

type AuditLogger struct {
    logger *log.Logger
}

func NewAuditLogger() *AuditLogger {
    return &AuditLogger{
        logger: log.New(os.Stdout, "[AUDIT] ", log.LstdFlags|log.Lmicroseconds),
    }
}

func (al *AuditLogger) LogEvent(event pgc.Event) {
    // Format: timestamp | topic | key | level | message
    msg := fmt.Sprintf("%s | %s | %s | %s",
        event.Topic(),
        event.Key(),
        event.Level(),
        event.Response().Message(),
    )
    
    // Add metadata if present
    if query, ok := event.GetMetadata("query"); ok {
        msg += fmt.Sprintf(" | query=%v", query)
    }
    
    // Log with appropriate level
    switch event.Level() {
    case pgc.EventLevelError:
        al.logger.Printf("ERROR: %s | error=%v", msg, event.Response().Cause())
    case pgc.EventLevelWarn:
        al.logger.Printf("WARN: %s", msg)
    default:
        al.logger.Printf("INFO: %s", msg)
    }
}

func main() {
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    auditor := NewAuditLogger()
    
    // Subscribe to all topics for complete audit trail
    bus.SubscribeAsync(pgc.TopicAll, auditor.LogEvent)
    
    // Setup and use datasource...
}
```

## Example 3: Slow Query Detector

Detect and alert on slow queries:

```go
package main

import (
    "fmt"
    "time"
    
    "github.com/sivaosorg/pgc"
)

type SlowQueryDetector struct {
    threshold time.Duration
    alertFunc func(query string, duration time.Duration)
}

func NewSlowQueryDetector(threshold time.Duration, alertFunc func(string, time.Duration)) *SlowQueryDetector {
    return &SlowQueryDetector{
        threshold: threshold,
        alertFunc: alertFunc,
    }
}

func (sqd *SlowQueryDetector) CheckQuery(event pgc.Event) {
    // Only check query events
    if event.Key() != pgc.EventQueryInspect {
        return
    }
    
    // Check if we have duration metadata
    duration, ok := event.GetMetadata("duration")
    if !ok {
        return
    }
    
    dur, ok := duration.(time.Duration)
    if !ok {
        return
    }
    
    // Alert if above threshold
    if dur > sqd.threshold {
        query, _ := event.GetMetadata("query")
        sqd.alertFunc(fmt.Sprintf("%v", query), dur)
    }
}

func main() {
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    // Alert on queries taking more than 100ms
    detector := NewSlowQueryDetector(100*time.Millisecond, func(query string, dur time.Duration) {
        fmt.Printf("⚠️  SLOW QUERY DETECTED: %s (took %v)\n", query, dur)
    })
    
    // Subscribe only to query events with async delivery
    bus.SubscribeAsync(pgc.TopicQuery, detector.CheckQuery)
    
    // Setup and use datasource...
}
```

## Example 4: Custom Event Filtering

Subscribe with custom filters:

```go
package main

import (
    "fmt"
    "github.com/sivaosorg/pgc"
)

func main() {
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    // Filter 1: Only error events
    errorFilter := func(event pgc.Event) bool {
        return event.Level() == pgc.EventLevelError
    }
    
    bus.SubscribeWithOptions(pgc.TopicAll, func(event pgc.Event) {
        fmt.Printf("ERROR EVENT: %s - %s\n", event.Key(), event.Response().Message())
    }, pgc.SubscribeOptions{
        Async:  true,
        Filter: errorFilter,
    })
    
    // Filter 2: Only transaction events with specific datasource
    txFilter := func(event pgc.Event) bool {
        ds := event.Datasource()
        // Add your custom logic here
        return ds != nil && ds.IsConnected()
    }
    
    bus.SubscribeWithOptions(pgc.TopicTransaction, func(event pgc.Event) {
        fmt.Printf("TX EVENT: %s\n", event.Key())
    }, pgc.SubscribeOptions{
        Async:  false,
        Filter: txFilter,
    })
}
```

## Example 5: Multiple Subscribers

Different components can subscribe independently:

```go
package main

import (
    "fmt"
    "github.com/sivaosorg/pgc"
)

type Logger struct{}
func (l *Logger) Log(event pgc.Event) {
    fmt.Printf("[LOG] %s\n", event.Key())
}

type Monitor struct{}
func (m *Monitor) Track(event pgc.Event) {
    fmt.Printf("[MONITOR] %s\n", event.Key())
}

type Alerter struct{}
func (a *Alerter) Alert(event pgc.Event) {
    if event.Level() == pgc.EventLevelError {
        fmt.Printf("[ALERT] Error detected: %s\n", event.Response().Message())
    }
}

func main() {
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    logger := &Logger{}
    monitor := &Monitor{}
    alerter := &Alerter{}
    
    // Each component subscribes independently
    bus.SubscribeAsync(pgc.TopicAll, logger.Log)
    bus.SubscribeAsync(pgc.TopicQuery, monitor.Track)
    bus.SubscribeAsync(pgc.TopicError, alerter.Alert)
    
    fmt.Printf("Total subscriptions: %d\n", bus.SubscriptionCount())
}
```

## Example 6: Unsubscribing

Manage subscriptions dynamically:

```go
package main

import (
    "fmt"
    "github.com/sivaosorg/pgc"
)

func main() {
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    // Subscribe and get subscription ID
    subID := bus.Subscribe(pgc.TopicQuery, func(event pgc.Event) {
        fmt.Printf("Query event: %s\n", event.Key())
    })
    
    fmt.Printf("Subscription ID: %s\n", subID)
    
    // Later, unsubscribe by ID
    if bus.Unsubscribe(subID) {
        fmt.Println("Successfully unsubscribed")
    }
    
    // Or unsubscribe all subscribers for a topic
    count := bus.UnsubscribeByTopic(pgc.TopicTransaction)
    fmt.Printf("Removed %d subscriptions\n", count)
}
```

## Example 7: Building Custom Events

Use EventBuilder for creating events manually:

```go
package main

import (
    "fmt"
    "github.com/sivaosorg/pgc"
    "github.com/sivaosorg/wrapify"
)

func main() {
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    // Subscribe to custom events
    bus.Subscribe(pgc.TopicQuery, func(event pgc.Event) {
        fmt.Printf("Custom event received: %s\n", event.Key())
        if val, ok := event.GetMetadata("custom_field"); ok {
            fmt.Printf("Custom field: %v\n", val)
        }
    })
    
    // Build and publish a custom event
    event := pgc.NewEventBuilder().
        WithTopic(pgc.TopicQuery).
        WithKey(pgc.EventQueryInspect).
        WithLevel(pgc.EventLevelInfo).
        WithResponse(wrapify.WrapOk("Custom event", nil).Reply()).
        WithMetadata("custom_field", "custom_value").
        WithMetadata("query_type", "SELECT").
        Build()
    
    // Publish synchronously
    bus.PublishSync(event)
    
    // Or publish asynchronously
    bus.Publish(event)
}
```

## Example 8: Connection Health Monitoring

Monitor connection health events:

```go
package main

import (
    "fmt"
    "time"
    
    "github.com/sivaosorg/pgc"
)

type HealthMonitor struct {
    lastHealthCheck time.Time
    isHealthy       bool
}

func NewHealthMonitor() *HealthMonitor {
    return &HealthMonitor{
        isHealthy: true,
    }
}

func (hm *HealthMonitor) OnConnectionEvent(event pgc.Event) {
    hm.lastHealthCheck = event.Timestamp()
    
    switch event.Key() {
    case pgc.EventConnOpen:
        hm.isHealthy = true
        fmt.Printf("✅ Connection established at %s\n", event.Timestamp().Format(time.RFC3339))
        
    case pgc.EventConnClose:
        hm.isHealthy = false
        fmt.Printf("❌ Connection lost at %s\n", event.Timestamp().Format(time.RFC3339))
        
    case pgc.EventConnRetry:
        fmt.Printf("🔄 Connection retry attempt at %s\n", event.Timestamp().Format(time.RFC3339))
        
    case pgc.EventConnPing:
        if event.Response().IsSuccess() {
            hm.isHealthy = true
            fmt.Printf("💚 Health check passed at %s\n", event.Timestamp().Format(time.RFC3339))
        } else {
            hm.isHealthy = false
            fmt.Printf("💔 Health check failed at %s: %v\n", 
                event.Timestamp().Format(time.RFC3339),
                event.Response().Cause())
        }
    }
}

func (hm *HealthMonitor) GetStatus() string {
    if hm.isHealthy {
        return fmt.Sprintf("Healthy (last check: %s)", hm.lastHealthCheck.Format(time.RFC3339))
    }
    return fmt.Sprintf("Unhealthy (last check: %s)", hm.lastHealthCheck.Format(time.RFC3339))
}

func main() {
    bus := pgc.NewEventBus()
    defer bus.Shutdown()
    
    monitor := NewHealthMonitor()
    
    // Subscribe to connection events with wildcard
    bus.SubscribeAsync(pgc.TopicConnection, monitor.OnConnectionEvent)
    
    // Setup datasource with keepalive
    conf := pgc.NewSettings()
    conf.SetEnable(true).
        SetHost("localhost").
        SetPort(5432).
        SetKeepalive(true).
        SetPingInterval(5 * time.Second)
    
    client := pgc.NewClient(*conf)
    client.SetEventBus(bus)
    
    // Periodically report status
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    
    for range ticker.C {
        fmt.Printf("Status: %s\n", monitor.GetStatus())
    }
}
```

## Best Practices

### 1. Always Shutdown Gracefully

```go
bus := pgc.NewEventBus()
defer bus.Shutdown() // Ensures all events are processed
```

### 2. Use Async for Heavy Operations

```go
// Heavy operations should use async delivery
bus.SubscribeAsync(pgc.TopicAll, func(event pgc.Event) {
    // Heavy processing, logging to external service, etc.
})
```

### 3. Use Filters for Efficiency

```go
// Filter at subscription time instead of in handler
filter := func(event pgc.Event) bool {
    return event.Level() == pgc.EventLevelError
}

bus.SubscribeWithOptions(pgc.TopicAll, errorHandler, pgc.SubscribeOptions{
    Filter: filter,
})
```

### 4. Use Specific Topics

```go
// Prefer specific topics over wildcard
bus.Subscribe(pgc.TopicQuery, queryHandler)        // Good
bus.Subscribe(pgc.TopicAll, queryHandler)          // Less efficient
```

### 5. Handle Errors in Subscribers

```go
bus.Subscribe(pgc.TopicAll, func(event pgc.Event) {
    defer func() {
        if r := recover(); r != nil {
            log.Printf("Panic in event handler: %v", r)
        }
    }()
    
    // Your handler code
})
```

## Performance Considerations

1. **Worker Count**: Adjust based on workload
   - More workers = better throughput for async events
   - But more overhead for context switching

2. **Buffer Size**: Adjust based on event rate
   - Larger buffer = less blocking on Publish
   - But more memory usage

3. **Sync vs Async**: Choose based on requirements
   - Sync: Immediate processing, blocks publisher
   - Async: Non-blocking, eventual delivery

4. **Filtering**: Apply filters at subscription time
   - More efficient than filtering in handler
   - Reduces unnecessary function calls

## Thread Safety

All EventBus operations are thread-safe and can be called concurrently from multiple goroutines:

```go
// Safe to call from multiple goroutines
go bus.Publish(event1)
go bus.Publish(event2)
go bus.Subscribe(topic, handler)
go bus.Unsubscribe(id)
```

## Backward Compatibility

The EventBus is opt-in and fully backward compatible:

```go
// Without EventBus - works as before
client := pgc.NewClient(*conf)
client.OnEvent(func(key pgc.EventKey, level pgc.EventLevel, response wrapify.R) {
    // Legacy callback
})

// With EventBus - both work together
client.SetEventBus(bus)
// Now both legacy callback AND EventBus receive events
```
