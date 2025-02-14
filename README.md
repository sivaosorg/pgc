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

You can configure the PostgreSQL client using the `RConf` struct. Here's an example configuration:

```go
conf := &pgc.RConf{}
conf.SetEnable(true).
    SetDebug(true).
    SetHost("localhost").
    SetPort(5432).
    SetUser("postgres").
    SetPassword("password").
    SetDatabase("mydb").
    SetSslMode("disable"). // SetSslModeVarious(pgc.SslmodeDisable).
    SetConnTimeout(30 * time.Second).
    SetMaxOpenConn(10).
    SetMaxIdleConn(5).
    SetConnMaxLifetime(1 * time.Hour).
    SetPingInterval(30 * time.Second).
    SetKeepalive(true)
```

All in one by connection strings:

```go
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
    fmt.Println(client.Wrap().Cause().Error()) // root cause error
}
```

#### Retrieve Metadata

You can retrieve metadata about tables, functions, procedures, and more:

```go
// Get all tables in the database
tables := client.AllTables()
if tables.IsError() {
	fmt.Println(tables.Cause().Error())
} else {
	fmt.Println(tables.Body())
}

// Get metadata for a specific table
tableMetadata := client.GetTableBrief("my_table")
if tableMetadata.IsError() {
	fmt.Println(tableMetadata.Cause().Error())
} else {
	fmt.Println(tableMetadata.Body())
}

// Get DDL for a specific table
tableDDL := client.GetTableDDL("my_table")
if tableDDL.IsError() {
	fmt.Println(tableDDL.Cause().Error())
} else {
	fmt.Println(tableDDL.Body())
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
client.SetOn(func(response wrapify.R) {
    if response.IsSuccess() {
        fmt.Println("Connection status updated:", response.Message())
    } else {
        fmt.Println("Connection error:", response.Cause().Error())
    }
})
```
