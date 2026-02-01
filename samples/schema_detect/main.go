package main

import (
	"fmt"
	"log"

	"github.com/spaghettifactory-oss/pipeforge-postgres/source"
	"github.com/spaghettifactory-oss/pipeforge-postgres/store"
	"github.com/spaghettifactory-oss/pipeforge/pipeline"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "host=localhost user=pipeforge password=pipeforge dbname=pipeforge port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Setup source table (simulating an existing table)
	db.Exec("DROP TABLE IF EXISTS inventory")
	db.Exec("DROP TABLE IF EXISTS inventory_backup")
	db.Exec(`CREATE TABLE inventory (
		id SERIAL PRIMARY KEY,
		sku VARCHAR(50),
		name VARCHAR(255),
		quantity INTEGER,
		price NUMERIC(10,2),
		in_stock BOOLEAN,
		last_updated TIMESTAMP
	)`)
	db.Exec(`CREATE TABLE inventory_backup (
		id INTEGER,
		sku VARCHAR(50),
		name VARCHAR(255),
		quantity INTEGER,
		price NUMERIC(10,2),
		in_stock BOOLEAN,
		last_updated TIMESTAMP
	)`)

	// Insert sample data
	db.Exec("INSERT INTO inventory (sku, name, quantity, price, in_stock, last_updated) VALUES ('LAP001', 'Laptop Pro', 15, 1299.99, true, NOW())")
	db.Exec("INSERT INTO inventory (sku, name, quantity, price, in_stock, last_updated) VALUES ('PHO001', 'Smartphone X', 42, 899.00, true, NOW())")
	db.Exec("INSERT INTO inventory (sku, name, quantity, price, in_stock, last_updated) VALUES ('TAB001', 'Tablet Air', 0, 549.50, false, NOW())")
	db.Exec("INSERT INTO inventory (sku, name, quantity, price, in_stock, last_updated) VALUES ('ACC001', 'USB-C Cable', 200, 19.99, true, NOW())")

	// Auto-detect schema from existing table
	fmt.Println("Auto-detecting schema from 'inventory' table...")
	schema, err := source.DetectSchema(db, "inventory")
	if err != nil {
		log.Fatalf("Failed to detect schema: %v", err)
	}

	fmt.Println("\nDetected Schema:")
	fmt.Println("----------------")
	fmt.Printf("Table: %s\n", schema.ID)
	fmt.Println("Columns:")
	for _, col := range schema.Columns {
		fmt.Printf("  - %s (%s)\n", col.GetID(), col.GetType().GetTypeName())
	}

	// Use detected schema to copy data
	p := pipeline.DataPipeline{
		Source: source.NewPostgresSource(db, "inventory", schema),
		Store:  store.NewPostgresStore(db, "inventory_backup"),
	}

	result, err := p.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	fmt.Println("\nCopied inventory to backup:")
	fmt.Println("---------------------------")
	for _, record := range result.Records {
		fmt.Printf("%s - %s (qty: %d, in_stock: %v)\n",
			record.GetString("sku"),
			record.GetString("name"),
			record.GetInt("quantity"),
			record.GetBool("in_stock"))
	}

	// Verify backup
	var count int64
	db.Table("inventory_backup").Count(&count)
	fmt.Printf("\nBackup table now has %d records\n", count)
}
