package pgc

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sivaosorg/wrapify"
)

func TestNewEventBus(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	if bus == nil {
		t.Fatal("NewEventBus returned nil")
	}

	if bus.workerCount != 4 {
		t.Errorf("Expected 4 workers, got %d", bus.workerCount)
	}

	if bus.bufferSize != 100 {
		t.Errorf("Expected buffer size 100, got %d", bus.bufferSize)
	}
}

func TestNewEventBusWithConfig(t *testing.T) {
	config := EventBusConfig{
		WorkerCount: 8,
		BufferSize:  200,
	}

	bus := NewEventBusWithConfig(config)
	defer bus.Shutdown()

	if bus.workerCount != 8 {
		t.Errorf("Expected 8 workers, got %d", bus.workerCount)
	}

	if bus.bufferSize != 200 {
		t.Errorf("Expected buffer size 200, got %d", bus.bufferSize)
	}
}

func TestEventBusSubscribeUnsubscribe(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	subscriber := func(event Event) {
		// Subscriber implementation
	}

	// Subscribe
	id := bus.Subscribe(TopicQuery, subscriber)
	if id == "" {
		t.Fatal("Subscribe returned empty ID")
	}

	if bus.SubscriptionCount() != 1 {
		t.Errorf("Expected 1 subscription, got %d", bus.SubscriptionCount())
	}

	// Unsubscribe
	if !bus.Unsubscribe(id) {
		t.Error("Unsubscribe failed")
	}

	if bus.SubscriptionCount() != 0 {
		t.Errorf("Expected 0 subscriptions, got %d", bus.SubscriptionCount())
	}
}

func TestEventBusPublishSync(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	var receivedCount int32
	subscriber := func(event Event) {
		atomic.AddInt32(&receivedCount, 1)
	}

	bus.Subscribe(TopicQuery, subscriber)

	event := NewEventBuilder().
		WithTopic(TopicQuery).
		WithKey(EventQueryInspect).
		WithLevel(EventLevelInfo).
		WithResponse(wrapify.WrapOk("test", nil).Reply()).
		Build()

	bus.PublishSync(event)

	if atomic.LoadInt32(&receivedCount) != 1 {
		t.Errorf("Expected 1 event received, got %d", receivedCount)
	}
}

func TestEventBusPublishAsync(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	var receivedCount int32
	var wg sync.WaitGroup
	wg.Add(1)

	subscriber := func(event Event) {
		atomic.AddInt32(&receivedCount, 1)
		wg.Done()
	}

	bus.SubscribeAsync(TopicQuery, subscriber)

	event := NewEventBuilder().
		WithTopic(TopicQuery).
		WithKey(EventQueryInspect).
		WithLevel(EventLevelInfo).
		WithResponse(wrapify.WrapOk("test", nil).Reply()).
		Build()

	if !bus.Publish(event) {
		t.Error("Failed to publish event")
	}

	// Wait for async delivery
	wg.Wait()

	if atomic.LoadInt32(&receivedCount) != 1 {
		t.Errorf("Expected 1 event received, got %d", receivedCount)
	}
}

func TestEventBusTopicMatching(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	tests := []struct {
		name       string
		eventTopic EventTopic
		subTopic   EventTopic
		matches    bool
	}{
		{"exact match", TopicQuery, TopicQuery, true},
		{"wildcard all", TopicQuery, TopicAll, true},
		{"prefix wildcard match", TopicQuerySelect, "query.*", true},
		{"no match", TopicQuery, TopicTransaction, false},
		{"prefix no match", TopicTransaction, "query.*", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := bus.topicMatches(tt.eventTopic, tt.subTopic)
			if result != tt.matches {
				t.Errorf("topicMatches(%s, %s) = %v, want %v",
					tt.eventTopic, tt.subTopic, result, tt.matches)
			}
		})
	}
}

func TestEventBusFilter(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	var receivedCount int32
	subscriber := func(event Event) {
		atomic.AddInt32(&receivedCount, 1)
	}

	// Subscribe with filter for errors only
	filter := func(event Event) bool {
		return event.Level() == EventLevelError
	}

	bus.SubscribeWithOptions(TopicAll, subscriber, SubscribeOptions{
		Async:  false,
		Filter: filter,
	})

	// Publish success event (should be filtered out)
	successEvent := NewEventBuilder().
		WithTopic(TopicQuery).
		WithLevel(EventLevelSuccess).
		WithResponse(wrapify.WrapOk("success", nil).Reply()).
		Build()

	bus.PublishSync(successEvent)

	if atomic.LoadInt32(&receivedCount) != 0 {
		t.Errorf("Expected 0 events (filtered), got %d", receivedCount)
	}

	// Publish error event (should pass filter)
	errorEvent := NewEventBuilder().
		WithTopic(TopicQuery).
		WithLevel(EventLevelError).
		WithResponse(wrapify.WrapInternalServerError("error", nil).WithHeader(wrapify.InternalServerError).Reply()).
		Build()

	bus.PublishSync(errorEvent)

	if atomic.LoadInt32(&receivedCount) != 1 {
		t.Errorf("Expected 1 event (passed filter), got %d", receivedCount)
	}
}

func TestEventBusMultipleSubscribers(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	var count1, count2 int32

	sub1 := func(event Event) {
		atomic.AddInt32(&count1, 1)
	}

	sub2 := func(event Event) {
		atomic.AddInt32(&count2, 1)
	}

	bus.Subscribe(TopicQuery, sub1)
	bus.Subscribe(TopicQuery, sub2)

	event := NewEventBuilder().
		WithTopic(TopicQuery).
		WithLevel(EventLevelInfo).
		WithResponse(wrapify.WrapOk("test", nil).Reply()).
		Build()

	bus.PublishSync(event)

	if atomic.LoadInt32(&count1) != 1 {
		t.Errorf("Subscriber 1: expected 1 event, got %d", count1)
	}

	if atomic.LoadInt32(&count2) != 1 {
		t.Errorf("Subscriber 2: expected 1 event, got %d", count2)
	}
}

func TestEventBusUnsubscribeByTopic(t *testing.T) {
	bus := NewEventBus()
	defer bus.Shutdown()

	sub1 := func(event Event) {}
	sub2 := func(event Event) {}

	bus.Subscribe(TopicQuery, sub1)
	bus.Subscribe(TopicQuery, sub2)
	bus.Subscribe(TopicTransaction, sub1)

	if bus.SubscriptionCount() != 3 {
		t.Errorf("Expected 3 subscriptions, got %d", bus.SubscriptionCount())
	}

	count := bus.UnsubscribeByTopic(TopicQuery)
	if count != 2 {
		t.Errorf("Expected to remove 2 subscriptions, removed %d", count)
	}

	if bus.SubscriptionCount() != 1 {
		t.Errorf("Expected 1 subscription remaining, got %d", bus.SubscriptionCount())
	}
}

func TestEventBuilder(t *testing.T) {
	response := wrapify.WrapOk("test message", nil).Reply()
	metadata := map[string]interface{}{
		"key1": "value1",
		"key2": 123,
	}

	event := NewEventBuilder().
		WithTopic(TopicQuery).
		WithKey(EventQueryInspect).
		WithLevel(EventLevelInfo).
		WithResponse(response).
		WithMetadataMap(metadata).
		WithMetadata("key3", true).
		Build()

	if event.Topic() != TopicQuery {
		t.Errorf("Expected topic %s, got %s", TopicQuery, event.Topic())
	}

	if event.Key() != EventQueryInspect {
		t.Errorf("Expected key %s, got %s", EventQueryInspect, event.Key())
	}

	if event.Level() != EventLevelInfo {
		t.Errorf("Expected level %s, got %s", EventLevelInfo, event.Level())
	}

	if val, ok := event.GetMetadata("key1"); !ok || val != "value1" {
		t.Errorf("Expected metadata key1=value1, got %v", val)
	}

	if val, ok := event.GetMetadata("key2"); !ok || val != 123 {
		t.Errorf("Expected metadata key2=123, got %v", val)
	}

	if val, ok := event.GetMetadata("key3"); !ok || val != true {
		t.Errorf("Expected metadata key3=true, got %v", val)
	}
}

func TestEventBusShutdown(t *testing.T) {
	bus := NewEventBus()

	// Add some subscriptions
	bus.Subscribe(TopicAll, func(event Event) {})

	// Publish some events
	for i := 0; i < 10; i++ {
		event := NewEventBuilder().
			WithTopic(TopicQuery).
			WithLevel(EventLevelInfo).
			WithResponse(wrapify.WrapOk("test", nil).Reply()).
			Build()
		if !bus.Publish(event) {
			t.Errorf("Failed to publish event %d", i)
		}
	}

	// Shutdown should wait for all events to be processed
	bus.Shutdown()

	// After shutdown, context should be cancelled
	select {
	case <-bus.ctx.Done():
		// Expected
	default:
		t.Error("Context not cancelled after shutdown")
	}
}

func TestDatasourceEventBusIntegration(t *testing.T) {
	// Create EventBus
	bus := NewEventBus()
	defer bus.Shutdown()

	var receivedEvents []Event
	var mu sync.Mutex

	subscriber := func(event Event) {
		mu.Lock()
		receivedEvents = append(receivedEvents, event)
		mu.Unlock()
	}

	bus.Subscribe(TopicAll, subscriber)

	// Create datasource (without actual DB connection for test)
	conf := NewSettings()
	conf.SetEnable(false) // Disabled to avoid actual connection

	ds := NewClient(*conf)
	ds.SetEventBus(bus)

	// Check that EventBus is set
	if !ds.HasEventBus() {
		t.Error("EventBus not set on datasource")
	}

	if ds.EventBus() != bus {
		t.Error("EventBus reference mismatch")
	}

	// Manually dispatch an event to test integration
	response := wrapify.WrapOk("test event", nil).Reply()
	ds.dispatch_event(EventConnClose, EventLevelError, response)

	// Give time for async delivery
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(receivedEvents) == 0 {
		t.Error("No events received through EventBus")
	}
}

func TestEventKeyToTopic(t *testing.T) {
	ds := &Datasource{}

	tests := []struct {
		key      EventKey
		expected EventTopic
	}{
		{EventTxBegin, TopicTransactionBegin},
		{EventTxCommit, TopicTransactionCommit},
		{EventConnOpen, TopicConnectionOpen},
		{EventConnClose, TopicConnectionClose},
		{EventConnRetry, TopicConnectionRetry},
		{EventConnPing, TopicConnectionHealth},
		{EventQueryInspect, TopicQuery},
	}

	for _, tt := range tests {
		t.Run(string(tt.key), func(t *testing.T) {
			topic := ds.eventKeyToTopic(tt.key)
			if topic != tt.expected {
				t.Errorf("eventKeyToTopic(%s) = %s, want %s", tt.key, topic, tt.expected)
			}
		})
	}
}

func TestEventBusDroppedEvents(t *testing.T) {
	// Create EventBus with small buffer
	bus := NewEventBusWithConfig(EventBusConfig{
		WorkerCount: 1,
		BufferSize:  2,
	})
	defer bus.Shutdown()

	// Add blocking subscriber to fill the buffer
	var wg sync.WaitGroup
	wg.Add(1)
	
	bus.Subscribe(TopicAll, func(event Event) {
		wg.Wait() // Block until we signal
	})

	event := NewEventBuilder().
		WithTopic(TopicQuery).
		WithLevel(EventLevelInfo).
		WithResponse(wrapify.WrapOk("test", nil).Reply()).
		Build()

	// Fill the buffer
	if !bus.Publish(event) {
		t.Error("First publish should succeed")
	}
	if !bus.Publish(event) {
		t.Error("Second publish should succeed")
	}

	// This should be dropped (buffer full)
	if bus.Publish(event) {
		t.Error("Third publish should fail (buffer full)")
	}

	// Unblock subscriber
	wg.Done()
}

func BenchmarkEventBusPublish(b *testing.B) {
	bus := NewEventBus()
	defer bus.Shutdown()

	subscriber := func(event Event) {}
	bus.SubscribeAsync(TopicAll, subscriber)

	event := NewEventBuilder().
		WithTopic(TopicQuery).
		WithLevel(EventLevelInfo).
		WithResponse(wrapify.WrapOk("test", nil).Reply()).
		Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(event)
	}
}

func BenchmarkEventBusPublishSync(b *testing.B) {
	bus := NewEventBus()
	defer bus.Shutdown()

	subscriber := func(event Event) {}
	bus.Subscribe(TopicAll, subscriber)

	event := NewEventBuilder().
		WithTopic(TopicQuery).
		WithLevel(EventLevelInfo).
		WithResponse(wrapify.WrapOk("test", nil).Reply()).
		Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.PublishSync(event)
	}
}
