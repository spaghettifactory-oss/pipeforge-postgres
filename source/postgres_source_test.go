package source_test

import (
	"testing"

	"github.com/spaghettifactory-oss/pipeforge-postgres/source"
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

func TestPostgresSource_Load(t *testing.T) {
	db := setupTestDB(t)

	// Setup test table
	db.Exec("DROP TABLE IF EXISTS test_products")
	db.Exec(`CREATE TABLE test_products (
		name VARCHAR(255),
		price INT
	)`)
	db.Exec("INSERT INTO test_products (name, price) VALUES ('Laptop', 999)")
	db.Exec("INSERT INTO test_products (name, price) VALUES ('Phone', 499)")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresSource(db, "test_products", schema)
	recordSet, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(recordSet.Records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(recordSet.Records))
	}

	// Cleanup
	db.Exec("DROP TABLE test_products")
}

func TestPostgresSource_LoadEmpty(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_empty")
	db.Exec("CREATE TABLE test_empty (name VARCHAR(255))")

	schema := &domain.DataSchema{
		ID: "Empty",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	src := source.NewPostgresSource(db, "test_empty", schema)
	recordSet, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(recordSet.Records) != 0 {
		t.Errorf("Expected 0 records, got %d", len(recordSet.Records))
	}

	db.Exec("DROP TABLE test_empty")
}

func TestPostgresSource_LoadError(t *testing.T) {
	db := setupTestDB(t)

	schema := &domain.DataSchema{
		ID: "NotExist",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
		},
	}

	src := source.NewPostgresSource(db, "table_that_does_not_exist", schema)
	_, err := src.Load()
	if err == nil {
		t.Error("Expected error for non-existent table, got nil")
	}
}

func TestPostgresSource_LoadWithNullValues(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_nulls")
	db.Exec(`CREATE TABLE test_nulls (
		name VARCHAR(255),
		value INT
	)`)
	db.Exec("INSERT INTO test_nulls (name, value) VALUES ('with_value', 100)")
	db.Exec("INSERT INTO test_nulls (name, value) VALUES (NULL, NULL)")

	schema := &domain.DataSchema{
		ID: "Nulls",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "value", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresSource(db, "test_nulls", schema)
	recordSet, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(recordSet.Records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(recordSet.Records))
	}

	db.Exec("DROP TABLE test_nulls")
}

func TestPostgresSource_LoadWithMissingColumn(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_missing_col")
	db.Exec(`CREATE TABLE test_missing_col (name VARCHAR(255))`)
	db.Exec("INSERT INTO test_missing_col (name) VALUES ('test')")

	schema := &domain.DataSchema{
		ID: "MissingCol",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "nonexistent", SchemaType: domain.NativeTypeString},
		},
	}

	src := source.NewPostgresSource(db, "test_missing_col", schema)
	recordSet, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(recordSet.Records) != 1 {
		t.Errorf("Expected 1 record, got %d", len(recordSet.Records))
	}

	db.Exec("DROP TABLE test_missing_col")
}

func TestPostgresSource_LoadAllTypes(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_all_types")
	db.Exec(`CREATE TABLE test_all_types (
		str_col VARCHAR(255),
		int_col INT,
		bigint_col BIGINT,
		float_col DOUBLE PRECISION,
		real_col REAL,
		bool_col BOOLEAN
	)`)
	db.Exec(`INSERT INTO test_all_types (str_col, int_col, bigint_col, float_col, real_col, bool_col)
		VALUES ('hello', 42, 9223372036854775807, 3.14159, 2.5, true)`)

	t.Cleanup(func() {
		db.Exec("DROP TABLE test_all_types")
	})

	// Test string type
	t.Run("StringType", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "StringTest",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "str_col", SchemaType: domain.NativeTypeString},
			},
		}
		src := source.NewPostgresSource(db, "test_all_types", schema)
		rs, err := src.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if rs.Records[0].GetString("str_col") != "hello" {
			t.Errorf("Expected 'hello', got '%s'", rs.Records[0].GetString("str_col"))
		}
	})

	// Test int type from int column
	t.Run("IntType", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "IntTest",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "int_col", SchemaType: domain.NativeTypeInt},
			},
		}
		src := source.NewPostgresSource(db, "test_all_types", schema)
		rs, err := src.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if rs.Records[0].GetInt("int_col") != 42 {
			t.Errorf("Expected 42, got %d", rs.Records[0].GetInt("int_col"))
		}
	})

	// Test int type from bigint (int64) column
	t.Run("Int64ToIntType", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "Int64Test",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "bigint_col", SchemaType: domain.NativeTypeInt},
			},
		}
		src := source.NewPostgresSource(db, "test_all_types", schema)
		rs, err := src.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		// Value will be truncated but should still work
		if rs.Records[0].GetInt("bigint_col") == 0 {
			t.Error("Expected non-zero value")
		}
	})

	// Test float type
	t.Run("FloatType", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "FloatTest",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "float_col", SchemaType: domain.NativeTypeFloat},
			},
		}
		src := source.NewPostgresSource(db, "test_all_types", schema)
		rs, err := src.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		val := rs.Records[0].GetFloat("float_col")
		if val < 3.14 || val > 3.15 {
			t.Errorf("Expected ~3.14159, got %f", val)
		}
	})

	// Test float from int column
	t.Run("IntToFloatType", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "IntToFloatTest",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "int_col", SchemaType: domain.NativeTypeFloat},
			},
		}
		src := source.NewPostgresSource(db, "test_all_types", schema)
		rs, err := src.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		val := rs.Records[0].GetFloat("int_col")
		if val != 42.0 {
			t.Errorf("Expected 42.0, got %f", val)
		}
	})

	// Test bool type
	t.Run("BoolType", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "BoolTest",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "bool_col", SchemaType: domain.NativeTypeBool},
			},
		}
		src := source.NewPostgresSource(db, "test_all_types", schema)
		rs, err := src.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if rs.Records[0].GetBool("bool_col") != true {
			t.Error("Expected true")
		}
	})

	// Test non-string to string conversion (fallback)
	t.Run("IntToStringFallback", func(t *testing.T) {
		schema := &domain.DataSchema{
			ID: "IntToStringTest",
			Columns: []domain.SchemaColumn{
				domain.SchemaColumnSingle{ID: "int_col", SchemaType: domain.NativeTypeString},
			},
		}
		src := source.NewPostgresSource(db, "test_all_types", schema)
		rs, err := src.Load()
		if err != nil {
			t.Fatalf("Load failed: %v", err)
		}
		if rs.Records[0].GetString("int_col") != "42" {
			t.Errorf("Expected '42', got '%s'", rs.Records[0].GetString("int_col"))
		}
	})
}

func TestPostgresSource_LoadIntDefaultZero(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_int_default")
	db.Exec(`CREATE TABLE test_int_default (str_val VARCHAR(255))`)
	db.Exec("INSERT INTO test_int_default (str_val) VALUES ('not_a_number')")

	schema := &domain.DataSchema{
		ID: "IntDefault",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "str_val", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresSource(db, "test_int_default", schema)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if rs.Records[0].GetInt("str_val") != 0 {
		t.Errorf("Expected 0 for non-numeric string, got %d", rs.Records[0].GetInt("str_val"))
	}

	db.Exec("DROP TABLE test_int_default")
}

func TestPostgresSource_LoadFloatDefaultZero(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_float_default")
	db.Exec(`CREATE TABLE test_float_default (str_val VARCHAR(255))`)
	db.Exec("INSERT INTO test_float_default (str_val) VALUES ('not_a_number')")

	schema := &domain.DataSchema{
		ID: "FloatDefault",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "str_val", SchemaType: domain.NativeTypeFloat},
		},
	}

	src := source.NewPostgresSource(db, "test_float_default", schema)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if rs.Records[0].GetFloat("str_val") != 0 {
		t.Errorf("Expected 0 for non-numeric string, got %f", rs.Records[0].GetFloat("str_val"))
	}

	db.Exec("DROP TABLE test_float_default")
}

func TestPostgresSource_LoadBoolDefaultFalse(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_bool_default")
	db.Exec(`CREATE TABLE test_bool_default (str_val VARCHAR(255))`)
	db.Exec("INSERT INTO test_bool_default (str_val) VALUES ('not_a_bool')")

	schema := &domain.DataSchema{
		ID: "BoolDefault",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "str_val", SchemaType: domain.NativeTypeBool},
		},
	}

	src := source.NewPostgresSource(db, "test_bool_default", schema)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if rs.Records[0].GetBool("str_val") != false {
		t.Error("Expected false for non-bool string")
	}

	db.Exec("DROP TABLE test_bool_default")
}

func TestPostgresSource_LoadWithWhere(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_where")
	db.Exec(`CREATE TABLE test_where (name VARCHAR(255), price INT)`)
	db.Exec("INSERT INTO test_where (name, price) VALUES ('Laptop', 999)")
	db.Exec("INSERT INTO test_where (name, price) VALUES ('Phone', 499)")
	db.Exec("INSERT INTO test_where (name, price) VALUES ('Cable', 10)")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresSource(db, "test_where", schema,
		source.WithWhere("price > ?", 50),
	)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(rs.Records) != 2 {
		t.Errorf("Expected 2 records (price > 50), got %d", len(rs.Records))
	}

	db.Exec("DROP TABLE test_where")
}

func TestPostgresSource_LoadWithMultipleWhere(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_multi_where")
	db.Exec(`CREATE TABLE test_multi_where (name VARCHAR(255), price INT, category VARCHAR(255))`)
	db.Exec("INSERT INTO test_multi_where (name, price, category) VALUES ('Laptop', 999, 'tech')")
	db.Exec("INSERT INTO test_multi_where (name, price, category) VALUES ('Phone', 499, 'tech')")
	db.Exec("INSERT INTO test_multi_where (name, price, category) VALUES ('Cable', 10, 'tech')")
	db.Exec("INSERT INTO test_multi_where (name, price, category) VALUES ('Bread', 3, 'food')")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresSource(db, "test_multi_where", schema,
		source.WithWhere("price > ?", 50),
		source.WithWhere("category = ?", "tech"),
	)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(rs.Records) != 2 {
		t.Errorf("Expected 2 records (price > 50 AND category = tech), got %d", len(rs.Records))
	}

	db.Exec("DROP TABLE test_multi_where")
}

func TestPostgresSource_LoadWithJoin(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_products_join")
	db.Exec("DROP TABLE IF EXISTS test_categories")
	db.Exec(`CREATE TABLE test_categories (id INT PRIMARY KEY, name VARCHAR(255))`)
	db.Exec(`CREATE TABLE test_products_join (name VARCHAR(255), price INT, category_id INT)`)
	db.Exec("INSERT INTO test_categories (id, name) VALUES (1, 'tech')")
	db.Exec("INSERT INTO test_categories (id, name) VALUES (2, 'food')")
	db.Exec("INSERT INTO test_products_join (name, price, category_id) VALUES ('Laptop', 999, 1)")
	db.Exec("INSERT INTO test_products_join (name, price, category_id) VALUES ('Phone', 499, 1)")
	db.Exec("INSERT INTO test_products_join (name, price, category_id) VALUES ('Bread', 3, 2)")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresSource(db, "test_products_join", schema,
		source.WithJoin("JOIN test_categories ON test_products_join.category_id = test_categories.id"),
		source.WithWhere("test_categories.name = ?", "tech"),
	)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(rs.Records) != 2 {
		t.Errorf("Expected 2 records (tech category), got %d", len(rs.Records))
	}

	db.Exec("DROP TABLE test_products_join")
	db.Exec("DROP TABLE test_categories")
}

func TestPostgresSource_LoadWithSelect(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_select")
	db.Exec(`CREATE TABLE test_select (id INT, name VARCHAR(255), price INT, description TEXT)`)
	db.Exec("INSERT INTO test_select (id, name, price, description) VALUES (1, 'Laptop', 999, 'A great laptop')")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresSource(db, "test_select", schema,
		source.WithSelect("name", "price"),
	)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(rs.Records) != 1 {
		t.Errorf("Expected 1 record, got %d", len(rs.Records))
	}

	if rs.Records[0].GetString("name") != "Laptop" {
		t.Errorf("Expected 'Laptop', got '%s'", rs.Records[0].GetString("name"))
	}

	db.Exec("DROP TABLE test_select")
}

func TestPostgresSource_RawQuery(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_raw")
	db.Exec(`CREATE TABLE test_raw (name VARCHAR(255), price INT)`)
	db.Exec("INSERT INTO test_raw (name, price) VALUES ('Laptop', 999)")
	db.Exec("INSERT INTO test_raw (name, price) VALUES ('Phone', 499)")
	db.Exec("INSERT INTO test_raw (name, price) VALUES ('Cable', 10)")

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	src := source.NewPostgresRawSource(db, schema,
		"SELECT name, price FROM test_raw WHERE price > $1 ORDER BY price DESC",
		100,
	)
	rs, err := src.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if len(rs.Records) != 2 {
		t.Errorf("Expected 2 records, got %d", len(rs.Records))
	}

	// First record should be Laptop (highest price)
	if rs.Records[0].GetString("name") != "Laptop" {
		t.Errorf("Expected 'Laptop', got '%s'", rs.Records[0].GetString("name"))
	}

	db.Exec("DROP TABLE test_raw")
}

func TestDetectSchema(t *testing.T) {
	db := setupTestDB(t)

	db.Exec("DROP TABLE IF EXISTS test_detect")
	db.Exec(`CREATE TABLE test_detect (
		id SERIAL PRIMARY KEY,
		name VARCHAR(255),
		price INTEGER,
		rating FLOAT,
		active BOOLEAN,
		created_at TIMESTAMP
	)`)

	schema, err := source.DetectSchema(db, "test_detect")
	if err != nil {
		t.Fatalf("DetectSchema failed: %v", err)
	}

	if schema.ID != "test_detect" {
		t.Errorf("Expected schema ID 'test_detect', got '%s'", schema.ID)
	}

	if len(schema.Columns) != 6 {
		t.Errorf("Expected 6 columns, got %d", len(schema.Columns))
	}

	// Verify column types
	expectedTypes := map[string]domain.SchemaType{
		"id":         domain.NativeTypeInt,
		"name":       domain.NativeTypeString,
		"price":      domain.NativeTypeInt,
		"rating":     domain.NativeTypeFloat,
		"active":     domain.NativeTypeBool,
		"created_at": domain.NativeTypeDate,
	}

	for _, col := range schema.Columns {
		expectedType, ok := expectedTypes[col.GetID()]
		if !ok {
			t.Errorf("Unexpected column: %s", col.GetID())
			continue
		}
		if col.GetType() != expectedType {
			t.Errorf("Column %s: expected type %v, got %v", col.GetID(), expectedType, col.GetType())
		}
	}

	db.Exec("DROP TABLE test_detect")
}

func TestDetectSchema_TableNotFound(t *testing.T) {
	db := setupTestDB(t)

	_, err := source.DetectSchema(db, "nonexistent_table")
	if err == nil {
		t.Error("Expected error for non-existent table, got nil")
	}
}
