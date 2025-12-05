# pgc

A Golang Postgresql compass library designed to simplify interactions with PostgreSQL databases. It provides a robust and configurable client for managing database connections, executing queries, and retrieving metadata about tables, functions, procedures, and more. The library also includes features like connection pooling, automatic reconnection, and health monitoring.

## Features

- **Connection Management**: Easily connect to PostgreSQL databases with configurable connection parameters.
- **Connection Pooling**: Configure maximum open and idle connections, connection lifetime, and more.
- **Automatic Reconnection**: Keep-alive mechanism to monitor connection health and automatically reconnect if needed.
- **Metadata Retrieval**: Retrieve metadata about tables, columns, functions, procedures, and more.
- **DDL Generation**: Generate Data Definition Language (DDL) scripts for tables, including relationships, constraints, and indexes.
- **Thread-Safe Operations**: Safe concurrent access to database connections and configurations.
- **Event Callbacks**: Register callbacks for connection status changes, replica events, and notifications.

## Requirements

Go version `1.23` or higher

## Installation

To start using `pgc`, run `go get`:

- For a specific version:

```bash
go get github.com/sivaosorg/pgc@v0.0.1
```

- For the latest version:

```bash
go get -u github.com/sivaosorg/pgc@latest
```

## Getting started

### Getting pgc

With [Go's module support](https://go.dev/wiki/Modules#how-to-use-modules), `go [build|run|test]` automatically fetches the necessary dependencies when you add the import in your code:

```go
import "github.com/sivaosorg/pgc"
```

### Usage

#### Configure the Client

You can configure the PostgreSQL client using the `Settings` struct. Here's an example configuration:

```go
conf := pgc.NewSettings()
conf.SetEnable(true). // Enables or disables the Postgres connection.
    SetDebug(true). // Turns on/off debugging mode for more verbose logging.
    SetHost("localhost"). // The hostname or IP address of the PostgreSQL server.
    SetPort(5432). // The port number on which the PostgreSQL server listens.
    SetUser("postgres"). // The username for authenticating with the database.
    SetPassword("password"). // The password for the given user.
    SetDatabase("mydb"). // The name of the database to connect to.
    SetSslMode("disable"). // SetSslModeVarious(pgc.SslmodeDisable).; The SSL mode for the connection (e.g., "disable", "require", "verify-ca", "verify-full").
    SetConnTimeout(30 * time.Second). // The maximum duration to wait when establishing a connection.
    SetMaxOpenConn(10). // The maximum number of open connections allowed in the connection pool.
    SetMaxIdleConn(5). // The maximum number of idle connections maintained in the pool.
    SetConnMaxLifetime(1 * time.Hour). // The maximum lifetime of a connection before it is recycled.
    SetPingInterval(30 * time.Second). // The interval between health-check pings to the database.
    SetKeepalive(true) // Enables TCP keep-alive to maintain persistent connections.
```

All in one by connection strings:

```go
// connectionStrings holds the generated connection string used to establish a connection
// to the PostgreSQL database. This string typically combines all the configuration parameters
// (such as host, port, user, password, database, SSL settings, etc.) into a formatted string
// that is recognized by the PostgreSQL driver.
conf.SetConnectionStrings("host=localhost port=5432 user=postgres password=your_password dbname=your_database sslmode=disable")
```

#### Create a New Client

Use the `NewClient` function to create a new PostgreSQL client:

```go
client := pgc.NewClient(*conf)
```

#### Check Connection Status

You can check if the client is connected to the database:

```go
if client.IsConnected() {
    fmt.Println("Connected to PostgreSQL!")
} else {
    fmt.Println("Failed to connect to PostgreSQL.")
    fmt.Println(client.State().Cause().Error()) // root cause error
}
```

#### Retrieve Metadata

You can retrieve metadata about tables, functions, procedures, and more:

```go
	// Get all tables in the database
	tables, response := client.Tables()
	if response.IsError() {
		loggy.Errorf("error: %v", response.Cause().Error())
	} else {
		loggy.Infof("tables: %v", unify4g.JsonN(tables))
	}

	// Get metadata for a specific table
	table_keys, response := client.TableKeys("my_table")
	if response.IsError() {
		loggy.Errorf("error: %v", response.Cause().Error())
	} else {
		loggy.Infof("table keys: %v", unify4g.JsonN(table_keys))
	}

	// Get DDL for a specific table
	ddl, response := client.TableDefPlus("my_table")
	if response.IsError() {
		loggy.Errorf("error: %v", response.Cause().Error())
	} else {
		loggy.Infof("table definition: %v", unify4g.JsonN(ddl))
	}
```

#### Execute Custom Queries

You can execute custom SQL queries using the underlying `sqlx.DB` connection:

```go
rows, err := client.Conn().Queryx("SELECT * FROM my_table")
if err != nil {
    log.Fatal(err)
}
defer rows.Close()

for rows.Next() {
    var result map[string]interface{}
    err := rows.MapScan(result)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(result)
}
```

#### Handle Connection Events

You can register callbacks to handle connection events, such as reconnection attempts or keepalive updates:

```go
client.OnReconnect(func(response wrapify.R) {
    if response.IsSuccess() {
        fmt.Println("Connection status updated:", response.Message())
    } else {
        fmt.Println("Connection error:", response.Error())
    }
})
```

### API Reference

```go
NewClient(conf pgc.Settings) *pgc.Datasource // Creates and returns a fully configured Datasource instance for PostgreSQL based on the provided Settings configuration.

Tables() wrapify.R // Retrieves the names of all base tables in the "public" schema of the connected PostgreSQL database.

Functions() wrapify.R // Retrieves the names of all stored functions from the "public" schema of the connected PostgreSQL database.

Procedures() wrapify.R // Retrieves the names of all stored procedures from the "public" schema of the connected PostgreSQL database.

FuncSpec(function string) wrapify.R // Retrieves detailed metadata for a specified function from the PostgreSQL database.

FuncDef(function string) wrapify.R // Retrieves the complete definition of a specified PostgreSQL function.

ProcDef(procedure string) wrapify.R // Retrieves the complete definition of a specified PostgreSQL procedure.

TableDef(table string) wrapify.R // Retrieves metadata information for the specified table from the connected PostgreSQL database.

ColsSpec(table string) wrapify.R // Retrieves metadata for all columns of the specified table from the PostgreSQL database.

TableDefPlus(table string) wrapify.R // Generates a comprehensive DDL script for the specified table, including detailed column definitions, default values, primary key markers, and sequence indicators.

keepalive() // Initiates a background goroutine that periodically pings the PostgreSQL database to monitor connection health.

ping() error // Performs a health check on the current PostgreSQL connection by issuing a PingContext request.

reconnect() error // Attempts to establish a new connection to the PostgreSQL database using the current configuration.
```

### Examples

Basic Usage

```go
package main

import (
	"fmt"
	"time"

	"github.com/sivaosorg/pgc"
)

func main() {
	conf := pgc.NewSettings()
	conf.SetEnable(true).
		SetHost("localhost").
		SetPort(5432).
		SetUser("postgres").
		SetPassword("password").
		SetDatabase("my_db").
		SetSslMode("disable").
		SetConnTimeout(30 * time.Second).
		SetMaxOpenConn(10).
		SetMaxIdleConn(5).
		SetConnMaxLifetime(1 * time.Hour).
		SetPingInterval(2 * time.Second).
		SetKeepalive(true)

	client := pgc.NewClient(*conf)
	// check connection status
	if client.IsConnected() {
		fmt.Println(client.State().Message())
	} else {
		fmt.Println(client.State().Cause().Error())
		return
	}

	callback := func(response wrapify.R, replicator *pgc.Datasource) {
		if response.IsSuccess() {
			loggy.Infof("%s Connection state: %v, message: %v", response.Meta().RequestID(), response.Reply().StatusText(), response.Message())
		}
		if response.IsError() {
			loggy.Errorf("root: %v, debug: %v, msg: %v", response.Cause().Error(), response.Debugging(), response.Message())
		}
	}
	client.OnReconnectChain(callback)

	// Get all tables in the database
	ous, response := client.Tables()
	if response.IsError() {
		loggy.Errorf("error: %v", response.Cause().Error())
	} else {
		loggy.Infof("tables: %v", unify4g.JsonN(ous))
	}

	// Get metadata for a specific table
	ous1, response := client.TableKeys("my_table")
	if response.IsError() {
		loggy.Errorf("error: %v", response.Cause().Error())
	} else {
		loggy.Infof("table keys: %v", unify4g.JsonN(ous1))
	}

	// Get DDL for a specific table
	ous2, response := client.TableDefPlus("my_table")
	if response.IsError() {
		loggy.Errorf("error: %v", response.Cause().Error())
	} else {
		loggy.Infof("table definition: %v", unify4g.JsonN(ous2))
	}

	// Since keepalive is false and no background goroutine is running,
	// start a dummy goroutine that sleeps forever to avoid a runtime deadlock.
	go func() {
		for {
			time.Sleep(1 * time.Second)
		}
	}()
	// main goroutine blocks indefinitely, but now at least one other goroutine is active
	select {}
}
```

Handling Connection Events

```go
package main

import (
	"fmt"
	"time"

	"github.com/sivaosorg/loggy"
	"github.com/sivaosorg/pgc"
	"github.com/sivaosorg/wrapify"
)

func main() {
	conf := pgc.NewSettings()
	conf.SetEnable(true).
		SetHost("localhost").
		SetPort(5432).
		SetUser("postgres").
		SetPassword("password").
		SetDatabase("my_db").
		SetSslMode("disable").
		SetConnTimeout(30 * time.Second).
		SetMaxOpenConn(10).
		SetMaxIdleConn(5).
		SetConnMaxLifetime(1 * time.Hour).
		SetPingInterval(2 * time.Second).
		SetKeepalive(true)

	client := pgc.NewClient(*conf)
	// check connection status
	if client.IsConnected() {
		fmt.Println(client.State().Message())
	} else {
		fmt.Println(client.State().Cause().Error())
		return
	}

	callback := func(response wrapify.R, replicator *pgc.Datasource) {
		if response.IsSuccess() {
			loggy.Infof("%s Connection state: %v, message: %v", response.Meta().RequestID(), response.Reply().StatusText(), response.Message())
		}
		if response.IsError() {
			loggy.Errorf("root: %v, debug: %v, msg: %v", response.Cause().Error(), response.Debugging(), response.Message())
		}
	}
	client.OnReconnectChain(callback)

	// Since keepalive is false and no background goroutine is running,
	// start a dummy goroutine that sleeps forever to avoid a runtime deadlock.
	go func() {
		for {
			time.Sleep(1 * time.Second)
		}
	}()
	// main goroutine blocks indefinitely, but now at least one other goroutine is active
	select {}
}
```

## Contributing

To contribute to project, follow these steps:

1. Clone the repository:

```bash
git clone --depth 1 https://github.com/sivaosorg/pgc.git
```

2. Navigate to the project directory:

```bash
cd pgc
```

3. Prepare the project environment:

```bash
go mod tidy
```
