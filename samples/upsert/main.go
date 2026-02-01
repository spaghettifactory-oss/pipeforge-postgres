package main

import (
	"fmt"
	"log"

	"github.com/spaghettifactory-oss/pipeforge-postgres/source"
	"github.com/spaghettifactory-oss/pipeforge-postgres/store"
	"github.com/spaghettifactory-oss/pipeforge/adapters/transform"
	"github.com/spaghettifactory-oss/pipeforge/domain"
	"github.com/spaghettifactory-oss/pipeforge/pipeline"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// PriceUpdateTransform applies a price increase percentage
type PriceUpdateTransform struct {
	Percentage int
}

func (t *PriceUpdateTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	return input.Map(func(r *domain.Record) *domain.Record {
		newValues := make(map[string]domain.Value)
		for k, v := range r.Values {
			if k == "price" {
				if intVal, ok := v.(domain.IntValue); ok {
					newPrice := int(intVal) * (100 + t.Percentage) / 100
					newValues[k] = domain.IntValue(newPrice)
					continue
				}
			}
			newValues[k] = v
		}
		return &domain.Record{Schema: r.Schema, Values: newValues}
	}), nil
}

func main() {
	dsn := "host=localhost user=pipeforge password=pipeforge dbname=pipeforge port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Setup tables
	db.Exec("DROP TABLE IF EXISTS product_prices")
	db.Exec("DROP TABLE IF EXISTS price_updates")
	db.Exec(`CREATE TABLE product_prices (
		product_id INT PRIMARY KEY,
		name VARCHAR(255),
		price INT,
		updated_count INT DEFAULT 0
	)`)
	db.Exec(`CREATE TABLE price_updates (
		product_id INT,
		name VARCHAR(255),
		price INT
	)`)

	// Insert initial prices
	db.Exec("INSERT INTO product_prices (product_id, name, price, updated_count) VALUES (1, 'Laptop', 999, 0)")
	db.Exec("INSERT INTO product_prices (product_id, name, price, updated_count) VALUES (2, 'Phone', 599, 0)")
	db.Exec("INSERT INTO product_prices (product_id, name, price, updated_count) VALUES (3, 'Tablet', 449, 0)")

	fmt.Println("Initial prices:")
	fmt.Println("---------------")
	printPrices(db)

	schema := &domain.DataSchema{
		ID: "ProductPrice",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "product_id", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	// Simulate incoming price updates (some existing, some new)
	db.Exec("INSERT INTO price_updates (product_id, name, price) VALUES (1, 'Laptop', 999)")      // Existing - will be updated
	db.Exec("INSERT INTO price_updates (product_id, name, price) VALUES (2, 'Phone', 599)")       // Existing - will be updated
	db.Exec("INSERT INTO price_updates (product_id, name, price) VALUES (4, 'Smartwatch', 299)") // New - will be inserted

	fmt.Println("\nIncoming price updates (+10% increase):")
	fmt.Println("----------------------------------------")
	var updates []map[string]any
	db.Table("price_updates").Find(&updates)
	for _, u := range updates {
		newPrice := int(u["price"].(int64)) * 110 / 100
		fmt.Printf("  %s: %v -> %d EUR\n", u["name"], u["price"], newPrice)
	}

	// Pipeline: Read updates -> Apply 10% increase -> Upsert into product_prices
	p := pipeline.DataPipeline{
		Source: source.NewPostgresSource(db, "price_updates", schema),
		Transform: transform.NewTransformBuilder().
			Add(&PriceUpdateTransform{Percentage: 10}).
			Build(),
		Store: store.NewPostgresStore(db, "product_prices",
			store.WithUpsert([]string{"product_id"}, []string{"name", "price"}),
		),
	}

	_, err = p.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	fmt.Println("\nAfter upsert (existing updated, new inserted):")
	fmt.Println("-----------------------------------------------")
	printPrices(db)

	// Run the pipeline again to demonstrate idempotent updates
	fmt.Println("\nRunning pipeline again (+10% on already updated prices):")
	fmt.Println("---------------------------------------------------------")

	_, err = p.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	printPrices(db)
}

func printPrices(db *gorm.DB) {
	var prices []map[string]any
	db.Table("product_prices").Order("product_id").Find(&prices)
	for _, p := range prices {
		fmt.Printf("  [%v] %s: %v EUR\n", p["product_id"], p["name"], p["price"])
	}
}
