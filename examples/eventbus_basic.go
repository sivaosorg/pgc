package main

import (
	"context"
	"fmt"
	"time"

	"github.com/sivaosorg/pgc"
)

// This example demonstrates basic EventBus usage with the pgc library.
// It shows how to:
// 1. Create and configure an EventBus
// 2. Attach it to a datasource
// 3. Subscribe to events with different delivery modes
// 4. Handle events from datasource operations

func main() {
	fmt.Println("=== pgc EventBus Basic Example ===")
	fmt.Println()

	// Create EventBus with default configuration (4 workers, buffer size 100)
	bus := pgc.NewEventBus()
	defer bus.Shutdown() // Always shutdown gracefully to process all events

	// Subscribe to all events (synchronous)
	bus.Subscribe(pgc.TopicAll, func(event pgc.Event) {
		fmt.Printf("[ALL] %s | %s | %s\n",
			event.Timestamp().Format("15:04:05.000"),
			event.Key(),
			event.Response().Message())
	})

	// Subscribe to connection events only (asynchronous)
	bus.SubscribeAsync(pgc.TopicConnection, func(event pgc.Event) {
		fmt.Printf("[CONNECTION] Event: %s, Level: %s\n",
			event.Key(),
			event.Level())
	})

	// Subscribe to errors only with custom filter
	errorFilter := func(event pgc.Event) bool {
		return event.Level() == pgc.EventLevelError
	}

	bus.SubscribeWithOptions(pgc.TopicAll, func(event pgc.Event) {
		fmt.Printf("[ERROR] %s: %s\n",
			event.Key(),
			event.Response().Message())
		if event.Response().Cause() != nil {
			fmt.Printf("        Root cause: %v\n", event.Response().Cause())
		}
	}, pgc.SubscribeOptions{
		Async:  true,
		Filter: errorFilter,
	})

	// Create datasource configuration
	conf := pgc.NewSettings()
	conf.SetEnable(true).
		SetHost("localhost").
		SetPort(5432).
		SetUser("postgres").
		SetPassword("password").
		SetDatabase("testdb").
		SetSslMode("disable").
		SetConnTimeout(5 * time.Second).
		SetMaxOpenConn(10).
		SetMaxIdleConn(5).
		SetConnMaxLifetime(1 * time.Hour)

	// Create datasource client
	client := pgc.NewClient(*conf)

	// Attach EventBus to datasource
	// All datasource events will now be published to the EventBus
	client.SetEventBus(bus)

	// Check connection status
	if client.IsConnected() {
		fmt.Printf("\n✓ Connected to PostgreSQL: %s\n", client.State().Message())
	} else {
		fmt.Printf("\n✗ Failed to connect: %s\n", client.State().Message())
		if client.State().Cause() != nil {
			fmt.Printf("  Root cause: %v\n", client.State().Cause())
		}
	}

	// If connected, perform some operations that generate events
	if client.IsConnected() {
		fmt.Println("\n--- Performing Database Operations ---")

		// List tables (generates query inspection events)
		tables, resp := client.Tables()
		if resp.IsSuccess() {
			fmt.Printf("\nFound %d tables: %v\n", len(tables), tables)
		} else if resp.IsError() {
			fmt.Printf("\nError listing tables: %s\n", resp.Message())
		}

		// Begin a transaction (generates transaction events)
		tx := client.BeginTx(context.Background())
		if tx.IsActivated() {
			fmt.Println("\nTransaction started successfully")
		}
	}

	// Wait a moment for async events to be processed
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("\n--- EventBus Statistics ---")
	fmt.Printf("\nTotal subscriptions: %d\n", bus.SubscriptionCount())

	fmt.Println("\n=== Example Complete ===")
}
