// Package source provides PostgreSQL data source implementations for pipeforge pipelines.
package source

import (
	"fmt"
	"strings"

	"github.com/spaghettifactory-oss/pipeforge/domain"
	"gorm.io/gorm"
)

// PostgresSource reads records from a PostgreSQL table.
// It implements the pipeforge Source interface.
type PostgresSource struct {
	// DB is the GORM database connection.
	DB *gorm.DB
	// TableName is the source table name.
	TableName string
	// Schema defines the expected columns and their types.
	Schema   *domain.DataSchema
	where    []WhereClause
	joins    []JoinClause
	selects  []string
	rawQuery string
	rawArgs  []any
}

// WhereClause represents a WHERE condition with its arguments.
type WhereClause struct {
	Query string
	Args  []any
}

// JoinClause represents a JOIN clause with its arguments.
type JoinClause struct {
	Query string
	Args  []any
}

// SourceOption is a functional option for configuring PostgresSource.
type SourceOption func(*PostgresSource)

// WithWhere adds a WHERE clause to filter the query results.
// Multiple WithWhere options are combined with AND.
//
// Example:
//
//	source := NewPostgresSource(db, "products", schema,
//	    WithWhere("price > ?", 100),
//	    WithWhere("category = ?", "tech"),
//	)
func WithWhere(query string, args ...any) SourceOption {
	return func(s *PostgresSource) {
		s.where = append(s.where, WhereClause{Query: query, Args: args})
	}
}

// WithJoin adds a JOIN clause to the query.
//
// Example:
//
//	source := NewPostgresSource(db, "products", schema,
//	    WithJoin("JOIN categories ON products.category_id = categories.id"),
//	    WithWhere("categories.name = ?", "tech"),
//	)
func WithJoin(query string, args ...any) SourceOption {
	return func(s *PostgresSource) {
		s.joins = append(s.joins, JoinClause{Query: query, Args: args})
	}
}

// WithSelect specifies which columns to select.
// If not specified, all columns from the schema are selected.
//
// Example:
//
//	source := NewPostgresSource(db, "products", schema,
//	    WithSelect("name", "price"),
//	)
func WithSelect(columns ...string) SourceOption {
	return func(s *PostgresSource) {
		s.selects = append(s.selects, columns...)
	}
}

// NewPostgresSource creates a new PostgresSource for reading from a PostgreSQL table.
//
// Example:
//
//	schema := &domain.DataSchema{
//	    ID: "Product",
//	    Columns: []domain.SchemaColumn{
//	        domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
//	        domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
//	    },
//	}
//	source := NewPostgresSource(db, "products", schema)
//	recordSet, err := source.Load()
func NewPostgresSource(db *gorm.DB, tableName string, schema *domain.DataSchema, opts ...SourceOption) *PostgresSource {
	s := &PostgresSource{
		DB:        db,
		TableName: tableName,
		Schema:    schema,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewPostgresRawSource creates a source from a raw SQL query.
// The query results are mapped to the provided schema.
//
// Example:
//
//	source := NewPostgresRawSource(db, schema,
//	    "SELECT name, price FROM products WHERE category = $1",
//	    "tech",
//	)
//	recordSet, err := source.Load()
func NewPostgresRawSource(db *gorm.DB, schema *domain.DataSchema, query string, args ...any) *PostgresSource {
	return &PostgresSource{
		DB:       db,
		Schema:   schema,
		rawQuery: query,
		rawArgs:  args,
	}
}

// Load reads all matching records from the PostgreSQL table and returns them as a RecordSet.
// Joins and WHERE clauses are applied if configured.
// Returns an error if the query fails.
func (s *PostgresSource) Load() (*domain.RecordSet, error) {
	var results []map[string]any
	var err error

	if s.rawQuery != "" {
		// Raw query mode
		err = s.DB.Raw(s.rawQuery, s.rawArgs...).Scan(&results).Error
		if err != nil {
			return nil, fmt.Errorf("failed to execute raw query: %w", err)
		}
	} else {
		// Table query mode
		query := s.DB.Table(s.TableName)

		// Apply SELECT if specified
		if len(s.selects) > 0 {
			query = query.Select(s.selects)
		}

		for _, join := range s.joins {
			query = query.Joins(join.Query, join.Args...)
		}

		for _, where := range s.where {
			query = query.Where(where.Query, where.Args...)
		}

		if err = query.Find(&results).Error; err != nil {
			return nil, fmt.Errorf("failed to load from table %s: %w", s.TableName, err)
		}
	}

	records := make([]*domain.Record, 0, len(results))
	for _, row := range results {
		record := &domain.Record{
			Schema: s.Schema,
			Values: make(map[string]domain.Value),
		}

		for _, col := range s.Schema.Columns {
			colID := col.GetID()
			val, ok := row[colID]
			if !ok || val == nil {
				record.Values[colID] = domain.NullValue{}
				continue
			}
			record.Values[colID] = convertToValue(val, col.GetType())
		}
		records = append(records, record)
	}

	return &domain.RecordSet{
		Schema:  s.Schema,
		Records: records,
	}, nil
}

// DetectSchema reads the table structure and returns a DataSchema.
// This auto-detects column names and types from PostgreSQL metadata.
//
// Example:
//
//	schema, err := DetectSchema(db, "products")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	source := NewPostgresSource(db, "products", schema)
func DetectSchema(db *gorm.DB, tableName string) (*domain.DataSchema, error) {
	type columnInfo struct {
		ColumnName string `gorm:"column:column_name"`
		DataType   string `gorm:"column:data_type"`
	}

	var columns []columnInfo
	err := db.Raw(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = ?
		ORDER BY ordinal_position
	`, tableName).Scan(&columns).Error

	if err != nil {
		return nil, fmt.Errorf("failed to detect schema for table %s: %w", tableName, err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", tableName)
	}

	schemaColumns := make([]domain.SchemaColumn, 0, len(columns))
	for _, col := range columns {
		schemaType := pgTypeToDomainType(col.DataType)
		schemaColumns = append(schemaColumns, domain.SchemaColumnSingle{
			ID:         col.ColumnName,
			SchemaType: schemaType,
		})
	}

	return &domain.DataSchema{
		ID:      tableName,
		Columns: schemaColumns,
	}, nil
}

// pgTypeToDomainType converts PostgreSQL data types to pipeforge domain types.
func pgTypeToDomainType(pgType string) domain.SchemaType {
	pgType = strings.ToLower(pgType)
	switch {
	case strings.Contains(pgType, "int"):
		return domain.NativeTypeInt
	case strings.Contains(pgType, "float") || strings.Contains(pgType, "double") ||
		strings.Contains(pgType, "numeric") || strings.Contains(pgType, "decimal") ||
		strings.Contains(pgType, "real"):
		return domain.NativeTypeFloat
	case strings.Contains(pgType, "bool"):
		return domain.NativeTypeBool
	case strings.Contains(pgType, "timestamp") || strings.Contains(pgType, "date"):
		return domain.NativeTypeDate
	default:
		return domain.NativeTypeString
	}
}

// convertToValue converts a native Go value to a domain.Value based on the schema type.
func convertToValue(val any, schemaType domain.SchemaType) domain.Value {
	switch schemaType {
	case domain.NativeTypeString:
		if v, ok := val.(string); ok {
			return domain.StringValue(v)
		}
		return domain.StringValue(fmt.Sprintf("%v", val))
	case domain.NativeTypeInt:
		switch v := val.(type) {
		case int:
			return domain.IntValue(v)
		case int32:
			return domain.IntValue(int(v))
		case int64:
			return domain.IntValue(int(v))
		case float64:
			return domain.IntValue(int(v))
		}
		return domain.IntValue(0)
	case domain.NativeTypeFloat:
		switch v := val.(type) {
		case float64:
			return domain.FloatValue(v)
		case float32:
			return domain.FloatValue(float64(v))
		case int:
			return domain.FloatValue(float64(v))
		case int32:
			return domain.FloatValue(float64(v))
		case int64:
			return domain.FloatValue(float64(v))
		}
		return domain.FloatValue(0)
	case domain.NativeTypeBool:
		if v, ok := val.(bool); ok {
			return domain.BoolValue(v)
		}
		return domain.BoolValue(false)
	default:
		return domain.NullValue{}
	}
}
