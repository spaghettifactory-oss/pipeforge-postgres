package store_test

import (
	"testing"
	"time"

	"github.com/spaghettifactory-oss/pipeforge-postgres/store"
	"github.com/spaghettifactory-oss/pipeforge/domain"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupTestDB(t *testing.T) *gorm.DB {
	dsn := "host=localhost user=pipeforge password=pipeforge dbname=pipeforge port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Skipf("Skipping test: unable to connect to postgres: %v", err)
	}
	return db
}

func TestPostgresStore_Store(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_output")
	db.Exec(`CREATE TABLE test_output (
		name VARCHAR(255),
		price INT
	)`)

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	recordSet := &domain.RecordSet{
		Schema: schema,
		Records: []*domain.Record{
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"name":  domain.StringValue("Laptop"),
					"price": domain.IntValue(999),
				},
			},
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"name":  domain.StringValue("Phone"),
					"price": domain.IntValue(499),
				},
			},
		},
	}

	dst := store.NewPostgresStore(db, "test_output")
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var count int64
	db.Table("test_output").Count(&count)
	if count != 2 {
		t.Errorf("Expected 2 rows, got %d", count)
	}

	db.Exec("DROP TABLE test_output")
}

func TestPostgresStore_StoreWithTruncate(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_truncate")
	db.Exec(`CREATE TABLE test_truncate (name VARCHAR(255))`)
	db.Exec("INSERT INTO test_truncate (name) VALUES ('existing')")

	schema := &domain.DataSchema{
		ID: "Item",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	recordSet := &domain.RecordSet{
		Schema: schema,
		Records: []*domain.Record{
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"name": domain.StringValue("new"),
				},
			},
		},
	}

	dst := store.NewPostgresStoreWithTruncate(db, "test_truncate")
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var count int64
	db.Table("test_truncate").Count(&count)
	if count != 1 {
		t.Errorf("Expected 1 row after truncate, got %d", count)
	}

	var name string
	db.Table("test_truncate").Select("name").Scan(&name)
	if name != "new" {
		t.Errorf("Expected 'new', got '%s'", name)
	}

	db.Exec("DROP TABLE test_truncate")
}

func TestPostgresStore_StoreEmpty(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_empty_store")
	db.Exec(`CREATE TABLE test_empty_store (name VARCHAR(255))`)

	schema := &domain.DataSchema{
		ID: "Empty",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	recordSet := &domain.RecordSet{
		Schema:  schema,
		Records: []*domain.Record{},
	}

	dst := store.NewPostgresStore(db, "test_empty_store")
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var count int64
	db.Table("test_empty_store").Count(&count)
	if count != 0 {
		t.Errorf("Expected 0 rows, got %d", count)
	}

	db.Exec("DROP TABLE test_empty_store")
}

func TestPostgresStore_StoreInsertError(t *testing.T) {
	db := setupTestDB(t)

	schema := &domain.DataSchema{
		ID: "NotExist",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	recordSet := &domain.RecordSet{
		Schema: schema,
		Records: []*domain.Record{
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"name": domain.StringValue("test"),
				},
			},
		},
	}

	dst := store.NewPostgresStore(db, "table_that_does_not_exist")
	err := dst.Store(recordSet)
	if err == nil {
		t.Error("Expected error for non-existent table, got nil")
	}
}

func TestPostgresStore_StoreTruncateError(t *testing.T) {
	db := setupTestDB(t)

	schema := &domain.DataSchema{
		ID: "NotExist",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	recordSet := &domain.RecordSet{
		Schema:  schema,
		Records: []*domain.Record{},
	}

	dst := store.NewPostgresStoreWithTruncate(db, "table_that_does_not_exist")
	err := dst.Store(recordSet)
	if err == nil {
		t.Error("Expected error for truncating non-existent table, got nil")
	}
}

func TestPostgresStore_StoreAllTypes(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_store_all_types")
	db.Exec(`CREATE TABLE test_store_all_types (
		str_col VARCHAR(255),
		int_col INT,
		float_col DOUBLE PRECISION,
		bool_col BOOLEAN,
		date_col TIMESTAMP,
		null_col VARCHAR(255)
	)`)

	schema := &domain.DataSchema{
		ID: "AllTypes",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "str_col", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "int_col", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "float_col", SchemaType: domain.NativeTypeFloat},
			domain.SchemaColumnSingle{ID: "bool_col", SchemaType: domain.NativeTypeBool},
			domain.SchemaColumnSingle{ID: "date_col", SchemaType: domain.NativeTypeDate},
			domain.SchemaColumnSingle{ID: "null_col", SchemaType: domain.NativeTypeString},
		},
	}

	testDate := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	recordSet := &domain.RecordSet{
		Schema: schema,
		Records: []*domain.Record{
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"str_col":   domain.StringValue("hello"),
					"int_col":   domain.IntValue(42),
					"float_col": domain.FloatValue(3.14159),
					"bool_col":  domain.BoolValue(true),
					"date_col":  domain.DateValue(testDate),
					"null_col":  domain.NullValue{},
				},
			},
		},
	}

	dst := store.NewPostgresStore(db, "test_store_all_types")
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var count int64
	db.Table("test_store_all_types").Count(&count)
	if count != 1 {
		t.Errorf("Expected 1 row, got %d", count)
	}

	// Verify the values
	var result struct {
		StrCol   string
		IntCol   int
		FloatCol float64
		BoolCol  bool
		DateCol  time.Time
		NullCol  *string
	}
	db.Table("test_store_all_types").First(&result)

	if result.StrCol != "hello" {
		t.Errorf("Expected 'hello', got '%s'", result.StrCol)
	}
	if result.IntCol != 42 {
		t.Errorf("Expected 42, got %d", result.IntCol)
	}
	if result.FloatCol < 3.14 || result.FloatCol > 3.15 {
		t.Errorf("Expected ~3.14159, got %f", result.FloatCol)
	}
	if result.BoolCol != true {
		t.Error("Expected true")
	}
	if result.NullCol != nil {
		t.Errorf("Expected nil, got '%v'", result.NullCol)
	}

	db.Exec("DROP TABLE test_store_all_types")
}

func TestPostgresStore_StoreBatch(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_batch")
	db.Exec(`CREATE TABLE test_batch (id INT, name VARCHAR(255))`)

	schema := &domain.DataSchema{
		ID: "Item",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "id", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	// Create 250 records to test batching with batch size of 100
	records := make([]*domain.Record, 250)
	for i := 0; i < 250; i++ {
		records[i] = &domain.Record{
			Schema: schema,
			Values: map[string]domain.Value{
				"id":   domain.IntValue(i),
				"name": domain.StringValue("item"),
			},
		}
	}

	recordSet := &domain.RecordSet{
		Schema:  schema,
		Records: records,
	}

	dst := store.NewPostgresStore(db, "test_batch", store.WithBatchSize(100))
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var count int64
	db.Table("test_batch").Count(&count)
	if count != 250 {
		t.Errorf("Expected 250 rows, got %d", count)
	}

	db.Exec("DROP TABLE test_batch")
}

func TestPostgresStore_StoreWithOptions(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_options")
	db.Exec(`CREATE TABLE test_options (name VARCHAR(255))`)
	db.Exec("INSERT INTO test_options (name) VALUES ('existing')")

	schema := &domain.DataSchema{
		ID: "Item",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	recordSet := &domain.RecordSet{
		Schema: schema,
		Records: []*domain.Record{
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"name": domain.StringValue("new"),
				},
			},
		},
	}

	// Test with both options
	dst := store.NewPostgresStore(db, "test_options",
		store.WithTruncate(),
		store.WithBatchSize(500),
	)
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var count int64
	db.Table("test_options").Count(&count)
	if count != 1 {
		t.Errorf("Expected 1 row after truncate, got %d", count)
	}

	db.Exec("DROP TABLE test_options")
}

func TestPostgresStore_Upsert(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_upsert")
	db.Exec(`CREATE TABLE test_upsert (
		id INT PRIMARY KEY,
		name VARCHAR(255),
		price INT
	)`)
	// Insert initial data
	db.Exec("INSERT INTO test_upsert (id, name, price) VALUES (1, 'Laptop', 999)")
	db.Exec("INSERT INTO test_upsert (id, name, price) VALUES (2, 'Phone', 499)")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "id", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	// Upsert: update existing (id=1), insert new (id=3)
	recordSet := &domain.RecordSet{
		Schema: schema,
		Records: []*domain.Record{
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"id":    domain.IntValue(1),
					"name":  domain.StringValue("Laptop Pro"), // Updated
					"price": domain.IntValue(1299),            // Updated
				},
			},
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"id":    domain.IntValue(3),
					"name":  domain.StringValue("Tablet"),
					"price": domain.IntValue(599),
				},
			},
		},
	}

	dst := store.NewPostgresStore(db, "test_upsert",
		store.WithUpsert([]string{"id"}, []string{"name", "price"}),
	)
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	// Verify: should have 3 rows total
	var count int64
	db.Table("test_upsert").Count(&count)
	if count != 3 {
		t.Errorf("Expected 3 rows, got %d", count)
	}

	// Verify updated row
	var updated struct {
		Name  string
		Price int
	}
	db.Table("test_upsert").Where("id = ?", 1).First(&updated)
	if updated.Name != "Laptop Pro" {
		t.Errorf("Expected 'Laptop Pro', got '%s'", updated.Name)
	}
	if updated.Price != 1299 {
		t.Errorf("Expected 1299, got %d", updated.Price)
	}

	// Verify unchanged row
	db.Table("test_upsert").Where("id = ?", 2).First(&updated)
	if updated.Name != "Phone" {
		t.Errorf("Expected 'Phone', got '%s'", updated.Name)
	}

	db.Exec("DROP TABLE test_upsert")
}

func TestPostgresStore_UpsertUpdateAll(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_upsert_all")
	db.Exec(`CREATE TABLE test_upsert_all (
		id INT PRIMARY KEY,
		name VARCHAR(255),
		price INT
	)`)
	db.Exec("INSERT INTO test_upsert_all (id, name, price) VALUES (1, 'Old', 100)")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "id", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	recordSet := &domain.RecordSet{
		Schema: schema,
		Records: []*domain.Record{
			{
				Schema: schema,
				Values: map[string]domain.Value{
					"id":    domain.IntValue(1),
					"name":  domain.StringValue("New"),
					"price": domain.IntValue(200),
				},
			},
		},
	}

	// Upsert with empty updateColumns = update all non-conflict columns
	dst := store.NewPostgresStore(db, "test_upsert_all",
		store.WithUpsert([]string{"id"}, nil),
	)
	err := dst.Store(recordSet)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	var result struct {
		Name  string
		Price int
	}
	db.Table("test_upsert_all").Where("id = ?", 1).First(&result)
	if result.Name != "New" {
		t.Errorf("Expected 'New', got '%s'", result.Name)
	}
	if result.Price != 200 {
		t.Errorf("Expected 200, got %d", result.Price)
	}

	db.Exec("DROP TABLE test_upsert_all")
}
