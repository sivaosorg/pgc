package pgc

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sivaosorg/wrapify"
)

// EventTopic represents a topic/channel for event categorization.
// It supports wildcard matching (e.g., "query.*" matches "query.select", "query.insert").
type EventTopic string

// Event represents a datasource event with metadata and context.
// All fields are internal with public getter methods.
type Event struct {
	// Internal fields
	topic      EventTopic
	key        EventKey
	level      EventLevel
	response   wrapify.R
	timestamp  time.Time
	metadata   map[string]interface{}
	datasource *Datasource
}

// EventSubscriber represents a subscriber function that receives events.
type EventSubscriber func(event Event)

// EventFilter is a function that determines if an event should be delivered to a subscriber.
type EventFilter func(event Event) bool

// subscription represents an internal subscription with its filter and options.
type subscription struct {
	id         string
	subscriber EventSubscriber
	filter     EventFilter
	async      bool
}

// EventBus provides a thread-safe publish-subscribe system for datasource events.
// It supports topic-based filtering, multiple subscribers, async/sync delivery,
// and graceful shutdown.
type EventBus struct {
	mu            sync.RWMutex
	subscriptions map[EventTopic]map[string]*subscription
	workerCount   int
	eventChan     chan Event
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	bufferSize    int
}

// EventBusConfig holds configuration for the EventBus.
type EventBusConfig struct {
	WorkerCount int // Number of worker goroutines for async event delivery
	BufferSize  int // Size of the event channel buffer
}

// EventBuilder provides a fluent interface for building events.
type EventBuilder struct {
	event Event
}

// SubscribeOptions provides options for subscription.
type SubscribeOptions struct {
	Async  bool        // If true, events are delivered asynchronously
	Filter EventFilter // Optional filter function
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Event Topic Constants
//_______________________________________________________________________

const (
	// Wildcard topic that matches all events
	TopicAll EventTopic = "*"

	// Query-related topics
	TopicQuery       EventTopic = "query"
	TopicQuerySelect EventTopic = "query.select"
	TopicQueryInsert EventTopic = "query.insert"
	TopicQueryUpdate EventTopic = "query.update"
	TopicQueryDelete EventTopic = "query.delete"

	// Transaction topics
	TopicTransaction       EventTopic = "transaction"
	TopicTransactionBegin  EventTopic = "transaction.begin"
	TopicTransactionCommit EventTopic = "transaction.commit"

	// Connection topics
	TopicConnection       EventTopic = "connection"
	TopicConnectionOpen   EventTopic = "connection.open"
	TopicConnectionClose  EventTopic = "connection.close"
	TopicConnectionRetry  EventTopic = "connection.retry"
	TopicConnectionHealth EventTopic = "connection.health"

	// Error topic
	TopicError EventTopic = "error"

	// Health topic
	TopicHealth EventTopic = "health"
)

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// EventBus Constructors
//_______________________________________________________________________

// NewEventBus creates a new EventBus with default configuration.
// Default: 4 workers, buffer size 100.
func NewEventBus() *EventBus {
	return NewEventBusWithConfig(EventBusConfig{
		WorkerCount: 4,
		BufferSize:  100,
	})
}

// NewEventBusWithConfig creates a new EventBus with custom configuration.
func NewEventBusWithConfig(config EventBusConfig) *EventBus {
	if config.WorkerCount <= 0 {
		config.WorkerCount = 4
	}
	if config.BufferSize <= 0 {
		config.BufferSize = 100
	}

	ctx, cancel := context.WithCancel(context.Background())
	eb := &EventBus{
		subscriptions: make(map[EventTopic]map[string]*subscription),
		workerCount:   config.WorkerCount,
		eventChan:     make(chan Event, config.BufferSize),
		ctx:           ctx,
		cancel:        cancel,
		bufferSize:    config.BufferSize,
	}

	// Start worker pool
	for i := 0; i < config.WorkerCount; i++ {
		eb.wg.Add(1)
		go eb.worker()
	}

	return eb
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// EventBus Methods
//_______________________________________________________________________

// Subscribe subscribes to a topic with default options (synchronous delivery).
// Returns a subscription ID that can be used to unsubscribe.
func (eb *EventBus) Subscribe(topic EventTopic, subscriber EventSubscriber) string {
	return eb.SubscribeWithOptions(topic, subscriber, SubscribeOptions{Async: false})
}

// SubscribeAsync subscribes to a topic with async delivery.
// Returns a subscription ID that can be used to unsubscribe.
func (eb *EventBus) SubscribeAsync(topic EventTopic, subscriber EventSubscriber) string {
	return eb.SubscribeWithOptions(topic, subscriber, SubscribeOptions{Async: true})
}

// SubscribeWithOptions subscribes to a topic with custom options.
// Returns a subscription ID that can be used to unsubscribe.
func (eb *EventBus) SubscribeWithOptions(topic EventTopic, subscriber EventSubscriber, opts SubscribeOptions) string {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	if eb.subscriptions[topic] == nil {
		eb.subscriptions[topic] = make(map[string]*subscription)
	}

	// Generate unique subscription ID
	id := fmt.Sprintf("%s-%d-%d", topic, time.Now().UnixNano(), len(eb.subscriptions[topic]))

	sub := &subscription{
		id:         id,
		subscriber: subscriber,
		filter:     opts.Filter,
		async:      opts.Async,
	}

	eb.subscriptions[topic][id] = sub
	return id
}

// Unsubscribe removes a subscription by ID.
// Returns true if the subscription was found and removed.
func (eb *EventBus) Unsubscribe(subscriptionID string) bool {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	for topic, subs := range eb.subscriptions {
		if _, exists := subs[subscriptionID]; exists {
			delete(subs, subscriptionID)
			// Clean up empty topic maps
			if len(subs) == 0 {
				delete(eb.subscriptions, topic)
			}
			return true
		}
	}
	return false
}

// UnsubscribeByTopic removes all subscriptions for a topic.
// Returns the number of subscriptions removed.
func (eb *EventBus) UnsubscribeByTopic(topic EventTopic) int {
	eb.mu.Lock()
	defer eb.mu.Unlock()

	count := len(eb.subscriptions[topic])
	delete(eb.subscriptions, topic)
	return count
}

// Publish publishes an event asynchronously to all matching subscribers.
// The event is queued in the event channel and delivered by workers.
func (eb *EventBus) Publish(event Event) {
	select {
	case eb.eventChan <- event:
		// Event queued successfully
	case <-eb.ctx.Done():
		// EventBus is shutting down
	default:
		// Channel is full, drop event (or could block/log)
		// For production, consider logging this
	}
}

// PublishSync publishes an event synchronously to all matching subscribers.
// This blocks until all synchronous subscribers have processed the event.
func (eb *EventBus) PublishSync(event Event) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	// Collect matching subscriptions
	var matchingSubs []*subscription
	for topic, subs := range eb.subscriptions {
		if eb.topicMatches(event.topic, topic) {
			for _, sub := range subs {
				if sub.filter == nil || sub.filter(event) {
					matchingSubs = append(matchingSubs, sub)
				}
			}
		}
	}

	// Deliver to subscribers
	for _, sub := range matchingSubs {
		if sub.async {
			// Queue async subscribers
			go sub.subscriber(event)
		} else {
			// Call sync subscribers directly
			sub.subscriber(event)
		}
	}
}

// Shutdown gracefully shuts down the EventBus, waiting for all workers to finish.
func (eb *EventBus) Shutdown() {
	eb.cancel()
	close(eb.eventChan)
	eb.wg.Wait()
}

// worker processes events from the event channel.
func (eb *EventBus) worker() {
	defer eb.wg.Done()

	for {
		select {
		case event, ok := <-eb.eventChan:
			if !ok {
				return // Channel closed, shutdown
			}
			eb.deliverEvent(event)
		case <-eb.ctx.Done():
			return // Context cancelled, shutdown
		}
	}
}

// deliverEvent delivers an event to all matching subscribers.
func (eb *EventBus) deliverEvent(event Event) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	for topic, subs := range eb.subscriptions {
		if eb.topicMatches(event.topic, topic) {
			for _, sub := range subs {
				// Apply filter if present
				if sub.filter != nil && !sub.filter(event) {
					continue
				}

				// Deliver to subscriber
				if sub.async {
					go sub.subscriber(event)
				} else {
					sub.subscriber(event)
				}
			}
		}
	}
}

// topicMatches checks if an event topic matches a subscription topic.
// Supports wildcard matching (e.g., "query.*" matches "query.select").
func (eb *EventBus) topicMatches(eventTopic, subTopic EventTopic) bool {
	// Wildcard matches all
	if subTopic == TopicAll {
		return true
	}

	// Exact match
	if eventTopic == subTopic {
		return true
	}

	// Prefix wildcard match (e.g., "query.*" matches "query.select")
	subStr := string(subTopic)
	if strings.HasSuffix(subStr, ".*") {
		prefix := strings.TrimSuffix(subStr, ".*")
		return strings.HasPrefix(string(eventTopic), prefix+".")
	}

	return false
}

// SubscriptionCount returns the total number of active subscriptions.
func (eb *EventBus) SubscriptionCount() int {
	eb.mu.RLock()
	defer eb.mu.RUnlock()

	count := 0
	for _, subs := range eb.subscriptions {
		count += len(subs)
	}
	return count
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// Event Methods (Getters)
//_______________________________________________________________________

// Topic returns the topic of the event.
func (e Event) Topic() EventTopic {
	return e.topic
}

// Key returns the event key.
func (e Event) Key() EventKey {
	return e.key
}

// Level returns the event level.
func (e Event) Level() EventLevel {
	return e.level
}

// Response returns the wrapify response.
func (e Event) Response() wrapify.R {
	return e.response
}

// Timestamp returns when the event was created.
func (e Event) Timestamp() time.Time {
	return e.timestamp
}

// Metadata returns the event metadata map.
func (e Event) Metadata() map[string]interface{} {
	return e.metadata
}

// Datasource returns the datasource that generated the event (may be nil).
func (e Event) Datasource() *Datasource {
	return e.datasource
}

// GetMetadata retrieves a metadata value by key.
func (e Event) GetMetadata(key string) (interface{}, bool) {
	if e.metadata == nil {
		return nil, false
	}
	val, ok := e.metadata[key]
	return val, ok
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// EventBuilder Methods
//_______________________________________________________________________

// NewEventBuilder creates a new EventBuilder.
func NewEventBuilder() *EventBuilder {
	return &EventBuilder{
		event: Event{
			timestamp: time.Now(),
			metadata:  make(map[string]interface{}),
		},
	}
}

// WithTopic sets the event topic.
func (eb *EventBuilder) WithTopic(topic EventTopic) *EventBuilder {
	eb.event.topic = topic
	return eb
}

// WithKey sets the event key.
func (eb *EventBuilder) WithKey(key EventKey) *EventBuilder {
	eb.event.key = key
	return eb
}

// WithLevel sets the event level.
func (eb *EventBuilder) WithLevel(level EventLevel) *EventBuilder {
	eb.event.level = level
	return eb
}

// WithResponse sets the wrapify response.
func (eb *EventBuilder) WithResponse(response wrapify.R) *EventBuilder {
	eb.event.response = response
	return eb
}

// WithDatasource sets the datasource reference.
func (eb *EventBuilder) WithDatasource(ds *Datasource) *EventBuilder {
	eb.event.datasource = ds
	return eb
}

// WithMetadata adds a metadata key-value pair.
func (eb *EventBuilder) WithMetadata(key string, value interface{}) *EventBuilder {
	if eb.event.metadata == nil {
		eb.event.metadata = make(map[string]interface{})
	}
	eb.event.metadata[key] = value
	return eb
}

// WithMetadataMap sets multiple metadata entries at once.
func (eb *EventBuilder) WithMetadataMap(metadata map[string]interface{}) *EventBuilder {
	if eb.event.metadata == nil {
		eb.event.metadata = make(map[string]interface{})
	}
	for k, v := range metadata {
		eb.event.metadata[k] = v
	}
	return eb
}

// Build returns the constructed Event.
func (eb *EventBuilder) Build() Event {
	return eb.event
}

//‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
// EventTopic Methods
//_______________________________________________________________________

// String returns the string representation of the EventTopic.
func (t EventTopic) String() string {
	return string(t)
}

// IsValid checks if the EventTopic is not empty.
func (t EventTopic) IsValid() bool {
	return len(t) > 0
}
