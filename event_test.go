package pgc

import (
	"sync"
	"testing"
	"time"

	"github.com/sivaosorg/wrapify"
)

// TestEventEnabledIndependently tests that events work independently from inspector
func TestEventEnabledIndependently(t *testing.T) {
	// Create a datasource without any inspector
	ds := &Datasource{}

	// Test 1: OnEvent should automatically enable events
	eventReceived := false
	var mu sync.Mutex

	ds.OnEvent(func(event EventKey, level EventLevel, response wrapify.R) {
		mu.Lock()
		eventReceived = true
		mu.Unlock()
	})

	if !ds.IsEventEnabled() {
		t.Error("Expected event to be enabled after OnEvent(), but it was not")
	}

	// Test dispatch_event should work
	ds.dispatch_event(EventQueryInspect, EventLevelDebug, wrapify.New().Reply())
	
	// Give goroutine time to execute
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !eventReceived {
		t.Error("Expected event to be dispatched, but it was not received")
	}
	mu.Unlock()
}

// TestInspectorIndependently tests that inspector works independently from events
func TestInspectorIndependently(t *testing.T) {
	// Create a datasource with inspector but without event
	ds := &Datasource{}

	inspectReceived := false
	var mu sync.Mutex

	ds.OnInspector(func(ins QueryInspect) {
		mu.Lock()
		inspectReceived = true
		mu.Unlock()
	})

	if !ds.IsInspectEnabled() {
		t.Error("Expected inspect to be enabled after OnInspector(), but it was not")
	}

	// Test inspect should work
	ds.inspect("TestFunc", "SELECT * FROM test", nil, 100*time.Millisecond)
	
	// Give goroutine time to execute
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !inspectReceived {
		t.Error("Expected inspector to be called, but it was not")
	}
	mu.Unlock()
}

// TestEventAndInspectorTogether tests that both systems work together
func TestEventAndInspectorTogether(t *testing.T) {
	ds := &Datasource{}

	eventReceived := false
	inspectReceived := false
	var mu sync.Mutex

	// Set both event and inspector
	ds.OnEvent(func(event EventKey, level EventLevel, response wrapify.R) {
		mu.Lock()
		eventReceived = true
		mu.Unlock()
	})

	ds.OnInspector(func(ins QueryInspect) {
		mu.Lock()
		inspectReceived = true
		mu.Unlock()
	})

	if !ds.IsEventEnabled() {
		t.Error("Expected event to be enabled")
	}

	if !ds.IsInspectEnabled() {
		t.Error("Expected inspect to be enabled")
	}

	// Test both should work
	ds.inspect("TestFunc", "SELECT * FROM test", nil, 100*time.Millisecond)
	
	// Give goroutines time to execute
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !eventReceived {
		t.Error("Expected event to be received")
	}
	if !inspectReceived {
		t.Error("Expected inspector to be called")
	}
	mu.Unlock()
}

// TestEnableDisableEvent tests Enable/Disable methods
func TestEnableDisableEvent(t *testing.T) {
	ds := &Datasource{}

	// Initially disabled
	if ds.IsEventEnabled() {
		t.Error("Expected event to be disabled initially")
	}

	// Enable
	ds.EnableEvent()
	if !ds.IsEventEnabled() {
		t.Error("Expected event to be enabled after EnableEvent()")
	}

	// Disable
	ds.DisableEvent()
	if ds.IsEventEnabled() {
		t.Error("Expected event to be disabled after DisableEvent()")
	}
}

// TestInspectQueryWithEventOnly tests that inspectQuery works when only event is enabled
func TestInspectQueryWithEventOnly(t *testing.T) {
	ds := &Datasource{}

	eventReceived := false
	var mu sync.Mutex

	ds.OnEvent(func(event EventKey, level EventLevel, response wrapify.R) {
		if event == EventQueryInspect {
			mu.Lock()
			eventReceived = true
			mu.Unlock()
		}
	})

	// inspectQuery should return a function that tracks timing
	done := ds.inspectQuery("TestFunc", "SELECT 1")
	if done == nil {
		t.Error("Expected inspectQuery to return a non-nil function")
	}

	// Simulate query execution
	time.Sleep(10 * time.Millisecond)
	done() // This should trigger inspect() which dispatches event

	// Give goroutine time to execute
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if !eventReceived {
		t.Error("Expected EventQueryInspect to be dispatched")
	}
	mu.Unlock()
}

// TestInspectQueryWithNeitherEnabled tests that inspectQuery returns noop when neither is enabled
func TestInspectQueryWithNeitherEnabled(t *testing.T) {
	ds := &Datasource{}

	// Neither inspect nor event is enabled
	done := ds.inspectQuery("TestFunc", "SELECT 1")
	if done == nil {
		t.Error("Expected inspectQuery to return a non-nil function")
	}

	// The returned function should be a noop
	// We can't directly test if it's noop, but we can verify no panic occurs
	done()
}

// TestOnEventWithNilCallback tests that setting nil callback disables events
func TestOnEventWithNilCallback(t *testing.T) {
	ds := &Datasource{}

	// Enable first
	ds.OnEvent(func(event EventKey, level EventLevel, response wrapify.R) {})
	if !ds.IsEventEnabled() {
		t.Error("Expected event to be enabled")
	}

	// Set nil callback
	ds.OnEvent(nil)
	if ds.IsEventEnabled() {
		t.Error("Expected event to be disabled after setting nil callback")
	}
}
