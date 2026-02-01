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

func main() {
	dsn := "host=localhost user=pipeforge password=pipeforge dbname=pipeforge port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	// Setup tables
	db.Exec("DROP TABLE IF EXISTS orders")
	db.Exec("DROP TABLE IF EXISTS order_summary")
	db.Exec(`CREATE TABLE orders (
		id SERIAL PRIMARY KEY,
		customer VARCHAR(255),
		product VARCHAR(255),
		quantity INT,
		price INT,
		created_at DATE
	)`)
	db.Exec(`CREATE TABLE order_summary (
		customer VARCHAR(255),
		total_orders INT,
		total_revenue INT
	)`)

	// Insert sample orders
	db.Exec("INSERT INTO orders (customer, product, quantity, price, created_at) VALUES ('Alice', 'Laptop', 1, 999, '2024-01-15')")
	db.Exec("INSERT INTO orders (customer, product, quantity, price, created_at) VALUES ('Alice', 'Mouse', 2, 25, '2024-01-16')")
	db.Exec("INSERT INTO orders (customer, product, quantity, price, created_at) VALUES ('Bob', 'Phone', 1, 599, '2024-01-15')")
	db.Exec("INSERT INTO orders (customer, product, quantity, price, created_at) VALUES ('Bob', 'Case', 1, 29, '2024-01-17')")
	db.Exec("INSERT INTO orders (customer, product, quantity, price, created_at) VALUES ('Charlie', 'Tablet', 1, 449, '2024-01-18')")

	fmt.Println("Orders:")
	fmt.Println("--------")
	var orders []map[string]any
	db.Table("orders").Find(&orders)
	for _, o := range orders {
		fmt.Printf("  %s bought %v x %s for %v\n", o["customer"], o["quantity"], o["product"], o["price"])
	}

	// Schema for aggregated results
	schema := &domain.DataSchema{
		ID: "OrderSummary",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "customer", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "total_orders", SchemaType: domain.NativeTypeInt},
			domain.SchemaColumnSingle{ID: "total_revenue", SchemaType: domain.NativeTypeInt},
		},
	}

	// Use raw SQL to aggregate orders by customer
	p := pipeline.DataPipeline{
		Source: source.NewPostgresRawSource(db, schema, `
			SELECT
				customer,
				COUNT(*) as total_orders,
				SUM(quantity * price) as total_revenue
			FROM orders
			GROUP BY customer
			ORDER BY total_revenue DESC
		`),
		Transform: transform.NewTransformBuilder().Build(),
		Store:     store.NewPostgresStore(db, "order_summary"),
	}

	result, err := p.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	fmt.Println("\nOrder Summary (via raw SQL aggregation):")
	fmt.Println("-----------------------------------------")
	for _, record := range result.Records {
		fmt.Printf("%s: %d orders, %d EUR total\n",
			record.GetString("customer"),
			record.GetInt("total_orders"),
			record.GetInt("total_revenue"))
	}
}
