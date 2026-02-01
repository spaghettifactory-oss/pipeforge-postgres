# Changelog

All notable changes to this project will be documented in this file.


## v0.1.0

### Added
- `PostgresSource` for reading data from PostgreSQL tables
  - `WithWhere(query, args...)` option for filtering rows
  - `WithJoin(query, args...)` option for joining tables
  - `WithSelect(columns...)` option for selecting specific columns
  - `NewPostgresRawSource()` for custom SQL queries with parameters
  - `DetectSchema()` for auto-detecting schema from existing tables
  - Support for String, Int, Float, Bool, and Null value types
- `PostgresStore` for writing data to PostgreSQL tables
  - `WithTruncate()` option for truncate-before-insert mode
  - `WithBatchSize(n)` option for configurable batch inserts (default: 1000)
  - `WithUpsert(conflictCols, updateCols)` for INSERT ON CONFLICT DO UPDATE
  - Support for String, Int, Float, Bool, Date, and Null value types
- Samples demonstrating:
  - `price_increase`: GORM AutoMigrate, filtering with joins, transformation
  - `raw_query`: Raw SQL aggregation queries
  - `schema_detect`: Auto-detecting schema from existing table
  - `upsert`: Idempotent updates with INSERT ON CONFLICT
