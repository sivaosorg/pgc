# Event and Inspector System Independence

## Overview

As of this version, the event system and inspector system in `pgc` are fully independent. Previously, `OnEvent()` with `EventQueryInspect` only worked when an inspector was registered, creating an unnecessary coupling. This has been resolved.

## Key Changes

### 1. Independent Event System

The event system now operates independently from the inspector system through a new `eventEnabled` flag.

```go
type Datasource struct {
    // ... existing fields ...
    
    // eventEnabled indicates whether event dispatching is enabled.
    // When true, events will be dispatched to the on_event callback.
    eventEnabled bool
}
```

### 2. New Methods

Three new methods have been added for event system control:

- **`EnableEvent()`** - Explicitly enables event dispatching
- **`DisableEvent()`** - Explicitly disables event dispatching  
- **`IsEventEnabled()`** - Checks if event dispatching is enabled

### 3. Automatic Event Enablement

When you set an event callback using `OnEvent()`, the event system is automatically enabled:

```go
// Automatically enables events
client.OnEvent(pgc.DefaultEventCallback())
```

Setting a `nil` callback automatically disables events:

```go
// Automatically disables events
client.OnEvent(nil)
```

## Usage Scenarios

### Scenario 1: Event System Only

You can now use events without needing to set up an inspector:

```go
client := pgc.NewClient(config)

// Set event callback - inspector NOT required
client.OnEvent(func(event pgc.EventKey, level pgc.EventLevel, response wrapify.R) {
    if event == pgc.EventQueryInspect {
        log.Printf("Query executed: %s", response.Message())
    }
})

// Query events will now be dispatched
client.ExecContext(ctx, "INSERT INTO users (name) VALUES ($1)", "John")
```

### Scenario 2: Inspector System Only

You can use the inspector without events:

```go
client := pgc.NewClient(config)

// Set inspector - events NOT dispatched
client.OnInspector(func(ins pgc.QueryInspect) {
    log.Printf("Query: %s | Duration: %v", ins.Completed(), ins.Duration())
})

// Only inspector callback is invoked
client.QueryContext(ctx, "SELECT * FROM users")
```

### Scenario 3: Both Systems Together

Both systems work harmoniously:

```go
client := pgc.NewClient(config)

// Use both systems
client.OnEvent(pgc.DefaultEventCallback())
client.OnInspector(pgc.DefaultInspectorChain())

// Both event and inspector callbacks are invoked
client.ExecContext(ctx, "UPDATE users SET name = $1 WHERE id = $2", "Jane", 1)
```

### Scenario 4: Independent Control

You can enable/disable each system independently:

```go
client := pgc.NewClient(config)

// Setup both
client.OnEvent(pgc.DefaultEventCallback())
client.OnInspector(pgc.DefaultInspectorChain())

// Disable only events, keep inspector active
client.DisableEvent()

// Disable only inspector, keep events active
client.DisableInspect()

// Re-enable as needed
client.EnableEvent()
client.EnableInspect()
```

## Default Behavior

By default, when using `NewClient()`:

1. **Event system** is automatically enabled because `DefaultEventCallback()` is registered
2. **Inspector system** is automatically enabled because `DefaultInspectorChain()` is registered

If you want to disable either system:

```go
client := pgc.NewClient(config)

// Disable events but keep inspector
client.DisableEvent()

// Disable inspector but keep events  
client.DisableInspect()

// Or set nil callbacks
client.OnEvent(nil)        // Disables events
client.OnInspector(nil)    // Disables inspector (via SetInspector)
```

## Performance Considerations

### Query Timing

Query timing is tracked when **either** the inspector **or** event system is enabled:

```go
func (d *Datasource) inspectQuery(funcName, query string, args ...any) func() {
    // Timing tracked if EITHER inspect OR event is enabled
    if !d.IsInspectEnabled() && !d.IsEventEnabled() {
        return func() {} // No-op if both disabled
    }
    
    start := time.Now()
    return func() {
        d.inspect(funcName, query, args, time.Since(start))
    }
}
```

This means:
- If both are disabled → No timing overhead
- If either is enabled → Timing is tracked
- If both are enabled → Timing is tracked once and shared

### Event Dispatching

Events are only dispatched when the event system is enabled:

```go
func (d *Datasource) dispatch_event(event EventKey, level EventLevel, response wrapify.R) {
    d.mu.RLock()
    enabled := d.eventEnabled
    callback := d.on_event
    d.mu.RUnlock()
    
    if !enabled || callback == nil {
        return  // Fast return if disabled
    }
    go callback(event, level, response)
}
```

## Backward Compatibility

All existing functionality is preserved:

- **`EnableInspect()`** - Still works as before
- **`DisableInspect()`** - Still works as before
- **`IsInspectEnabled()`** - Still works as before
- **`OnInspector()`** - Still works as before
- **`SetInspector()`** - Still works as before
- **`OnReconnect()`** - Still works as before
- **`OnReconnectChain()`** - Still works as before

No breaking changes have been introduced.

## Migration Guide

If you were previously working around the coupling by unnecessarily setting up an inspector just to receive events, you can now simplify your code:

### Before (Workaround)

```go
// Had to set dummy inspector just to get events
client.OnInspector(func(ins pgc.QueryInspect) {
    // Empty - we don't actually need inspector
})

client.OnEvent(pgc.DefaultEventCallback())
```

### After (Clean)

```go
// Just use events - no inspector needed
client.OnEvent(pgc.DefaultEventCallback())
```

## Testing

Comprehensive tests have been added to verify:

1. Event system works independently from inspector
2. Inspector system works independently from events
3. Both systems work together correctly
4. Enable/Disable methods work as expected
5. Timing tracking behaves correctly
6. Nil callbacks properly disable systems

Run tests with:

```bash
go test -v ./...
```

## Summary

The decoupling of the event and inspector systems provides:

- ✅ **Flexibility**: Use either system independently or together
- ✅ **Simplicity**: No unnecessary setup required
- ✅ **Performance**: Avoid overhead when systems are disabled
- ✅ **Backward Compatibility**: All existing code continues to work
- ✅ **Clear API**: Explicit methods for controlling each system
