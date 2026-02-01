// Package store provides PostgreSQL storage implementations for pipeforge pipelines.
package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/spaghettifactory-oss/pipeforge/domain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const defaultBatchSize = 1000

// PostgresStore writes records to a PostgreSQL table.
// It implements the pipeforge Store interface.
type PostgresStore struct {
	// DB is the GORM database connection.
	DB *gorm.DB
	// TableName is the target table name.
	TableName string
	// Truncate indicates whether to truncate the table before inserting.
	Truncate bool
	// BatchSize is the number of records to insert per batch (default: 1000).
	BatchSize int
	// Upsert configuration
	upsertConflict []string
	upsertUpdate   []string
}

// StoreOption is a functional option for configuring PostgresStore.
type StoreOption func(*PostgresStore)

// WithTruncate enables truncating the table before inserting.
//
// Example:
//
//	store := NewPostgresStore(db, "products", WithTruncate())
func WithTruncate() StoreOption {
	return func(s *PostgresStore) {
		s.Truncate = true
	}
}

// WithBatchSize sets the batch size for bulk inserts.
// Default is 1000 records per batch.
//
// Example:
//
//	store := NewPostgresStore(db, "products", WithBatchSize(500))
func WithBatchSize(size int) StoreOption {
	return func(s *PostgresStore) {
		s.BatchSize = size
	}
}

// WithUpsert enables INSERT ... ON CONFLICT DO UPDATE behavior.
// conflictColumns are the columns used to detect conflicts (usually primary key or unique constraint).
// updateColumns are the columns to update when a conflict occurs.
// If updateColumns is empty, all non-conflict columns are updated.
//
// Example:
//
//	// Upsert on id, update name and price on conflict
//	store := NewPostgresStore(db, "products",
//	    WithUpsert([]string{"id"}, []string{"name", "price"}),
//	)
//
//	// Upsert on composite key
//	store := NewPostgresStore(db, "product_prices",
//	    WithUpsert([]string{"product_id", "region"}, []string{"price", "updated_at"}),
//	)
func WithUpsert(conflictColumns []string, updateColumns []string) StoreOption {
	return func(s *PostgresStore) {
		s.upsertConflict = conflictColumns
		s.upsertUpdate = updateColumns
	}
}

// NewPostgresStore creates a new PostgresStore with optional configuration.
//
// Example:
//
//	// Simple usage
//	store := NewPostgresStore(db, "products")
//
//	// With options
//	store := NewPostgresStore(db, "products",
//	    WithTruncate(),
//	    WithBatchSize(500),
//	)
func NewPostgresStore(db *gorm.DB, tableName string, opts ...StoreOption) *PostgresStore {
	s := &PostgresStore{
		DB:        db,
		TableName: tableName,
		Truncate:  false,
		BatchSize: defaultBatchSize,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// NewPostgresStoreWithTruncate creates a new PostgresStore that truncates
// the table before inserting new records.
//
// Deprecated: Use NewPostgresStore with WithTruncate() option instead.
//
// Example:
//
//	store := NewPostgresStoreWithTruncate(db, "products")
//	err := store.Store(recordSet) // Table is cleared first
func NewPostgresStoreWithTruncate(db *gorm.DB, tableName string) *PostgresStore {
	return NewPostgresStore(db, tableName, WithTruncate())
}

// Store writes all records from the RecordSet to the PostgreSQL table.
// Records are inserted in batches for better performance.
// If Truncate is true, the table is truncated before inserting.
// If Upsert is configured, conflicts are handled with UPDATE.
// Returns an error if truncation or any insert fails.
func (s *PostgresStore) Store(recordSet *domain.RecordSet) error {
	if s.Truncate {
		if err := s.DB.Exec(fmt.Sprintf("TRUNCATE TABLE %s", s.TableName)).Error; err != nil {
			return fmt.Errorf("failed to truncate table %s: %w", s.TableName, err)
		}
	}

	if len(recordSet.Records) == 0 {
		return nil
	}

	// Convert all records to maps
	rows := make([]map[string]any, 0, len(recordSet.Records))
	for _, record := range recordSet.Records {
		rows = append(rows, convertRecordToMap(record))
	}

	// Insert in batches
	for i := 0; i < len(rows); i += s.BatchSize {
		end := i + s.BatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]

		if err := s.insertBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

// insertBatch inserts a batch of rows, handling upsert if configured.
func (s *PostgresStore) insertBatch(batch []map[string]any) error {
	if len(s.upsertConflict) > 0 {
		return s.upsertBatch(batch)
	}

	if err := s.DB.Table(s.TableName).Create(batch).Error; err != nil {
		return fmt.Errorf("failed to insert batch into table %s: %w", s.TableName, err)
	}
	return nil
}

// upsertBatch performs INSERT ... ON CONFLICT DO UPDATE for a batch.
func (s *PostgresStore) upsertBatch(batch []map[string]any) error {
	if len(batch) == 0 {
		return nil
	}

	// Build conflict columns
	conflictCols := make([]clause.Column, len(s.upsertConflict))
	for i, col := range s.upsertConflict {
		conflictCols[i] = clause.Column{Name: col}
	}

	// Build update columns
	var doUpdates clause.Set
	if len(s.upsertUpdate) > 0 {
		// Update specific columns
		doUpdates = make(clause.Set, len(s.upsertUpdate))
		for i, col := range s.upsertUpdate {
			doUpdates[i] = clause.Assignment{
				Column: clause.Column{Name: col},
				Value:  gorm.Expr("EXCLUDED." + col),
			}
		}
	} else {
		// Update all columns except conflict columns
		if len(batch) > 0 {
			conflictSet := make(map[string]bool)
			for _, col := range s.upsertConflict {
				conflictSet[col] = true
			}

			for col := range batch[0] {
				if !conflictSet[col] {
					doUpdates = append(doUpdates, clause.Assignment{
						Column: clause.Column{Name: col},
						Value:  gorm.Expr("EXCLUDED." + col),
					})
				}
			}
		}
	}

	err := s.DB.Table(s.TableName).Clauses(clause.OnConflict{
		Columns:   conflictCols,
		DoUpdates: doUpdates,
	}).Create(batch).Error

	if err != nil {
		return fmt.Errorf("failed to upsert batch into table %s: %w", s.TableName, err)
	}
	return nil
}

// buildColumnList creates a comma-separated list of column names for SQL.
func buildColumnList(columns []string) string {
	return strings.Join(columns, ", ")
}

// convertRecordToMap converts a domain.Record to a map suitable for GORM insertion.
func convertRecordToMap(record *domain.Record) map[string]any {
	row := make(map[string]any)
	for key, val := range record.Values {
		row[key] = convertValueToNative(val)
	}
	return row
}

// convertValueToNative converts a domain.Value to its native Go type.
func convertValueToNative(val domain.Value) any {
	switch v := val.(type) {
	case domain.StringValue:
		return string(v)
	case domain.IntValue:
		return int(v)
	case domain.FloatValue:
		return float64(v)
	case domain.BoolValue:
		return bool(v)
	case domain.DateValue:
		return time.Time(v)
	case domain.NullValue:
		return nil
	default:
		return nil
	}
}
