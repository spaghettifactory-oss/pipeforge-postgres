# PipeForge Postgres

A PostgreSQL adapter for [PipeForge](https://github.com/spaghettifactory-oss/pipeforge) — read from and write to PostgreSQL tables in your data pipelines.

## Features

- **PostgreSQL Source** — Read data from tables with filtering and joins
- **PostgreSQL Store** — Write data with append, truncate, or upsert modes
- **Batch Insert** — Insert records in configurable batches for performance
- **Upsert Support** — INSERT ON CONFLICT DO UPDATE for idempotent writes
- **Raw SQL Queries** — Use custom SQL as data source
- **Schema Auto-detect** — Automatically detect table schema from PostgreSQL
- **GORM Integration** — Built on GORM for reliable database operations
- **Type Conversion** — Automatic mapping between Go and PostgreSQL types
- **Functional Options** — Clean, composable API for query configuration

## Installation

```bash
go get github.com/spaghettifactory-oss/pipeforge-postgres
```

## Quick Start

```go
package main

import (
    "github.com/spaghettifactory-oss/pipeforge-postgres/source"
    "github.com/spaghettifactory-oss/pipeforge-postgres/store"
    "github.com/spaghettifactory-oss/pipeforge/domain"
    "github.com/spaghettifactory-oss/pipeforge/pipeline"
    "gorm.io/driver/postgres"
    "gorm.io/gorm"
)

func main() {
    dsn := "host=localhost user=app password=secret dbname=mydb port=5432 sslmode=disable"
    db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{})

    schema := &domain.DataSchema{
        ID: "Product",
        Columns: []domain.SchemaColumn{
            domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
            domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
        },
    }

    p := pipeline.DataPipeline{
        Source: source.NewPostgresSource(db, "products", schema),
        Store:  store.NewPostgresStore(db, "products_backup"),
    }

    p.Run()
}
```

## Source Options

### Filtering with WHERE

```go
src := source.NewPostgresSource(db, "products", schema,
    source.WithWhere("price > ?", 100),
    source.WithWhere("active = ?", true),
)
```

### Joining Tables

```go
src := source.NewPostgresSource(db, "products", schema,
    source.WithJoin("JOIN categories ON products.category_id = categories.id"),
    source.WithWhere("categories.name = ?", "electronics"),
)
```

### Select Specific Columns

```go
src := source.NewPostgresSource(db, "products", schema,
    source.WithSelect("name", "price"),
)
```

### Raw SQL Query

```go
src := source.NewPostgresRawSource(db, schema,
    "SELECT name, price FROM products WHERE category = $1 ORDER BY price DESC",
    "electronics",
)
```

### Auto-detect Schema

```go
schema, err := source.DetectSchema(db, "products")
if err != nil {
    log.Fatal(err)
}
src := source.NewPostgresSource(db, "products", schema)
```

## Store Options

### Append Mode (default)

```go
dst := store.NewPostgresStore(db, "output_table")
```

### Truncate Mode

```go
dst := store.NewPostgresStore(db, "output_table", store.WithTruncate())
```

### Batch Size

Records are inserted in batches for better performance (default: 1000).

```go
dst := store.NewPostgresStore(db, "output_table", store.WithBatchSize(500))
```

### Upsert (INSERT ON CONFLICT)

```go
dst := store.NewPostgresStore(db, "products",
    store.WithUpsert([]string{"id"}, []string{"name", "price"}),
)
```

### Combined Options

```go
dst := store.NewPostgresStore(db, "output_table",
    store.WithTruncate(),
    store.WithBatchSize(500),
)
```

## Supported Types

| PipeForge Type | PostgreSQL Type |
|----------------|-----------------|
| `StringValue`  | VARCHAR, TEXT   |
| `IntValue`     | INT, BIGINT     |
| `FloatValue`   | FLOAT, DOUBLE   |
| `BoolValue`    | BOOLEAN         |
| `DateValue`    | TIMESTAMP       |
| `NullValue`    | NULL            |

## Project Structure

```
pipeforge-postgres/
├── source/
│   ├── postgres_source.go      # PostgreSQL source implementation
│   └── postgres_source_test.go
├── store/
│   ├── postgres_store.go       # PostgreSQL store implementation
│   └── postgres_store_test.go
├── samples/
│   ├── price_increase/         # Filter and transform pipeline
│   ├── raw_query/              # Raw SQL aggregation
│   ├── schema_detect/          # Auto-detect schema
│   └── upsert/                 # Idempotent upsert
├── docker-compose.yml          # Local PostgreSQL for development
└── go.mod
```

## Examples

| Example | Description |
|---------|-------------|
| [price_increase](samples/price_increase) | Filter tech products, apply price transformation, write to new table |
| [raw_query](samples/raw_query) | Use raw SQL for aggregation queries |
| [schema_detect](samples/schema_detect) | Auto-detect schema from existing table |
| [upsert](samples/upsert) | Idempotent updates with INSERT ON CONFLICT |

## Development

### Prerequisites

- Go 1.25+
- Docker (for local PostgreSQL)

### Running Tests

```bash
# Start PostgreSQL
docker compose up -d

# Run tests with coverage
go test ./... -cover
```

## License

MIT
