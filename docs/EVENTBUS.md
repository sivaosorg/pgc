# EventBus System

## Overview

The EventBus system provides a flexible, decoupled publish-subscribe mechanism for handling datasource events in the pgc library. It enables users and library developers to listen and react to database events, metrics, custom logging, auditing, notifications, and more.

## Architecture

### Core Components

1. **Event**: Represents a datasource event with metadata and context
   - Internal fields with public getters for encapsulation
   - Includes topic, key, level, response, timestamp, metadata, and datasource reference

2. **EventBus**: Central pub-sub coordinator
   - Thread-safe subscription management
   - Topic-based routing with wildcard support
   - Configurable worker pool for async delivery
   - Graceful shutdown mechanism

3. **EventBuilder**: Fluent interface for creating events
   - Method chaining for easy event construction
   - Type-safe event creation

4. **Subscription**: Represents a subscriber with options
   - Unique ID for management
   - Optional filter function
   - Async/sync delivery mode

## Features

### Topic-Based Filtering

Events are categorized by topics using a hierarchical naming scheme:

- **Exact Match**: `TopicQuery` matches only query events
- **Wildcard**: `TopicAll` (or `*`) matches all events
- **Prefix Wildcard**: `"query.*"` matches `query.select`, `query.insert`, etc.

### Async/Sync Delivery

- **Synchronous**: Blocks until all subscribers process the event
- **Asynchronous**: Non-blocking, delivered by worker pool

### Custom Filtering

Apply filter functions at subscription time for fine-grained control:

```go
filter := func(event pgc.Event) bool {
    return event.Level() == pgc.EventLevelError
}

bus.SubscribeWithOptions(topic, handler, pgc.SubscribeOptions{
    Filter: filter,
})
```

### Worker Pool

Configurable number of workers for async event processing:

```go
bus := pgc.NewEventBusWithConfig(pgc.EventBusConfig{
    WorkerCount: 8,    // 8 worker goroutines
    BufferSize:  200,  // Buffer up to 200 events
})
```

### Panic Recovery

Async subscribers have built-in panic recovery to prevent goroutine crashes:

```go
// If a subscriber panics, the EventBus continues processing
bus.SubscribeAsync(topic, func(event pgc.Event) {
    // This panic won't crash the application
    panic("subscriber error")
})
```

### Dropped Event Detection

The `Publish` method returns a boolean indicating whether the event was queued:

```go
if !bus.Publish(event) {
    // Event was dropped (buffer full or shutdown)
    log.Warn("Event dropped")
}
```

## Integration with Datasource

The EventBus integrates seamlessly with the Datasource:

```go
// Create EventBus
bus := pgc.NewEventBus()
defer bus.Shutdown()

// Create Datasource
client := pgc.NewClient(*conf)

// Attach EventBus
client.SetEventBus(bus)

// Now all datasource operations publish events
```

### Event Topics

Datasource events are automatically mapped to topics:

| EventKey | EventTopic |
|----------|------------|
| EventTxBegin | TopicTransactionBegin |
| EventTxCommit | TopicTransactionCommit |
| EventConnOpen | TopicConnectionOpen |
| EventConnClose | TopicConnectionClose |
| EventConnRetry | TopicConnectionRetry |
| EventConnPing | TopicConnectionHealth |
| EventQueryInspect | TopicQuery |
| event_tx_* | TopicTransaction |
| event_conn_* | TopicConnection |
| event_query* | TopicQuery |

### Backward Compatibility

The EventBus is opt-in and fully backward compatible:

- Legacy `OnEvent()` callback still works
- Both EventBus and callback can coexist
- No EventBus = no events published (zero overhead)

## Usage Patterns

### 1. Metrics Collection

```go
type MetricsCollector struct {
    queryCount int
    txCount    int
    errorCount int
}

func (mc *MetricsCollector) HandleEvent(event pgc.Event) {
    switch event.Level() {
    case pgc.EventLevelError:
        mc.errorCount++
    case pgc.EventLevelSuccess:
        if event.Key() == pgc.EventQueryInspect {
            mc.queryCount++
        }
    }
}

bus.Subscribe(pgc.TopicAll, collector.HandleEvent)
```

### 2. Audit Logging

```go
bus.SubscribeAsync(pgc.TopicAll, func(event pgc.Event) {
    log.Printf("[AUDIT] %s | %s | %s",
        event.Topic(),
        event.Key(),
        event.Response().Message())
})
```

### 3. Slow Query Detection

```go
bus.SubscribeAsync(pgc.TopicQuery, func(event pgc.Event) {
    if duration, ok := event.GetMetadata("duration"); ok {
        if dur := duration.(time.Duration); dur > threshold {
            alert("Slow query detected", dur)
        }
    }
})
```

### 4. Connection Health Monitoring

```go
bus.SubscribeAsync(pgc.TopicConnection, func(event pgc.Event) {
    switch event.Key() {
    case pgc.EventConnOpen:
        log.Info("Connection established")
    case pgc.EventConnClose:
        log.Error("Connection lost")
    case pgc.EventConnRetry:
        log.Warn("Connection retry attempt")
    }
})
```

## Performance Considerations

### Worker Pool Sizing

- **More workers**: Better throughput for async events
- **Fewer workers**: Less overhead, suitable for low event rates

Default: 4 workers

### Buffer Size

- **Larger buffer**: Handles burst traffic, less event dropping
- **Smaller buffer**: Less memory usage, faster shutdown

Default: 100 events

### Sync vs Async

- **Sync**: Immediate processing, blocks publisher
- **Async**: Non-blocking, eventual delivery

Choose based on your use case:
- Critical operations: Sync
- Logging, metrics: Async

### Event Dropping

When the event buffer is full:
- Events are dropped silently
- `Publish` returns `false`
- Consider increasing buffer size or adding more workers

## Thread Safety

All EventBus operations are thread-safe:

- `Subscribe/Unsubscribe`: Safe to call concurrently
- `Publish/PublishSync`: Safe to call from multiple goroutines
- `Shutdown`: Safe to call once

## Best Practices

1. **Always Shutdown Gracefully**
   ```go
   bus := pgc.NewEventBus()
   defer bus.Shutdown()
   ```

2. **Use Specific Topics**
   ```go
   // Good
   bus.Subscribe(pgc.TopicQuery, handler)
   
   // Less efficient
   bus.Subscribe(pgc.TopicAll, handler)
   ```

3. **Apply Filters at Subscription Time**
   ```go
   // Good
   filter := func(e pgc.Event) bool { return e.Level() == pgc.EventLevelError }
   bus.SubscribeWithOptions(topic, handler, pgc.SubscribeOptions{Filter: filter})
   
   // Less efficient
   bus.Subscribe(topic, func(e pgc.Event) {
       if e.Level() == pgc.EventLevelError {
           // handle
       }
   })
   ```

4. **Use Async for Heavy Operations**
   ```go
   // Good for external logging, API calls, etc.
   bus.SubscribeAsync(topic, heavyHandler)
   
   // Bad (blocks event processing)
   bus.Subscribe(topic, heavyHandler)
   ```

5. **Handle Panics in Sync Subscribers**
   ```go
   bus.Subscribe(topic, func(event pgc.Event) {
       defer func() {
           if r := recover(); r != nil {
               log.Error("Subscriber panic:", r)
           }
       }()
       // handler code
   })
   ```

6. **Monitor Dropped Events**
   ```go
   if !bus.Publish(event) {
       metrics.IncrementDroppedEvents()
   }
   ```

## Testing

The EventBus includes comprehensive tests:

- Subscription management
- Topic matching (exact, wildcard, prefix)
- Async/sync delivery
- Event filtering
- Multiple subscribers
- Graceful shutdown
- Dropped event detection
- Datasource integration

Run tests:
```bash
go test -v -cover ./...
```

## Examples

See comprehensive examples in:
- `docs/EVENTBUS_EXAMPLES.md` - Detailed usage patterns
- `examples/eventbus_basic.go` - Working code example

## API Reference

### EventBus Methods

```go
// Creation
func NewEventBus() *EventBus
func NewEventBusWithConfig(config EventBusConfig) *EventBus

// Subscription
func (eb *EventBus) Subscribe(topic EventTopic, subscriber EventSubscriber) string
func (eb *EventBus) SubscribeAsync(topic EventTopic, subscriber EventSubscriber) string
func (eb *EventBus) SubscribeWithOptions(topic EventTopic, subscriber EventSubscriber, opts SubscribeOptions) string
func (eb *EventBus) Unsubscribe(subscriptionID string) bool
func (eb *EventBus) UnsubscribeByTopic(topic EventTopic) int

// Publishing
func (eb *EventBus) Publish(event Event) bool
func (eb *EventBus) PublishSync(event Event)

// Management
func (eb *EventBus) Shutdown()
func (eb *EventBus) SubscriptionCount() int
```

### Event Methods (Getters)

```go
func (e Event) Topic() EventTopic
func (e Event) Key() EventKey
func (e Event) Level() EventLevel
func (e Event) Response() wrapify.R
func (e Event) Timestamp() time.Time
func (e Event) Metadata() map[string]interface{}
func (e Event) Datasource() *Datasource
func (e Event) GetMetadata(key string) (interface{}, bool)
```

### EventBuilder Methods

```go
func NewEventBuilder() *EventBuilder
func (eb *EventBuilder) WithTopic(topic EventTopic) *EventBuilder
func (eb *EventBuilder) WithKey(key EventKey) *EventBuilder
func (eb *EventBuilder) WithLevel(level EventLevel) *EventBuilder
func (eb *EventBuilder) WithResponse(response wrapify.R) *EventBuilder
func (eb *EventBuilder) WithDatasource(ds *Datasource) *EventBuilder
func (eb *EventBuilder) WithMetadata(key string, value interface{}) *EventBuilder
func (eb *EventBuilder) WithMetadataMap(metadata map[string]interface{}) *EventBuilder
func (eb *EventBuilder) Build() Event
```

### Datasource Methods

```go
func (d *Datasource) SetEventBus(eventBus *EventBus) *Datasource
func (d *Datasource) EventBus() *EventBus
func (d *Datasource) HasEventBus() bool
```

## Security

The EventBus has been scanned with CodeQL and no security vulnerabilities were found. The implementation includes:

- Panic recovery for async subscribers
- Thread-safe operations
- Graceful shutdown to prevent resource leaks
- No external dependencies beyond the standard library and existing pgc dependencies

## Future Enhancements

Potential future improvements:

1. **Event Replay**: Store events for replay/debugging
2. **Priority Queue**: Prioritize critical events
3. **Batch Processing**: Process events in batches
4. **Metrics Export**: Built-in metrics for event processing
5. **Event Transformers**: Transform events before delivery
6. **Dead Letter Queue**: Handle permanently failed events
7. **Event Persistence**: Optional event storage

## License

Same as the pgc library.
