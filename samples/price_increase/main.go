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

type ProductType struct {
	ID   int    `gorm:"primaryKey;column:id"`
	Name string `gorm:"column:name;type:varchar(255)"`
}

type Product struct {
	Name   string `gorm:"column:name;type:varchar(255)"`
	Price  int    `gorm:"column:price"`
	TypeID int    `gorm:"column:type_id"`
}

type ProductUpdated struct {
	Name  string `gorm:"column:name;type:varchar(255)"`
	Price int    `gorm:"column:price"`
}

func (ProductUpdated) TableName() string {
	return "products_updated"
}

type MultiplyTransform struct {
	Column     string
	Multiplier int
}

func (t *MultiplyTransform) Transform(input *domain.RecordSet) (*domain.RecordSet, error) {
	return input.Map(func(r *domain.Record) *domain.Record {
		newValues := make(map[string]domain.Value)
		for k, v := range r.Values {
			if k == t.Column {
				if intVal, ok := v.(domain.IntValue); ok {
					newValues[k] = domain.IntValue(int(intVal) * t.Multiplier)
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
	db.Migrator().DropTable(&Product{}, &ProductUpdated{}, &ProductType{})
	db.AutoMigrate(&ProductType{}, &Product{}, &ProductUpdated{})

	// Create product types
	db.Create([]ProductType{
		{ID: 1, Name: "tech"},
		{ID: 2, Name: "food"},
	})

	// Create products with type references
	db.Create([]Product{
		{Name: "Laptop", Price: 999, TypeID: 1},   // tech
		{Name: "Phone", Price: 499, TypeID: 1},    // tech
		{Name: "Tablet", Price: 349, TypeID: 1},   // tech
		{Name: "USB Cable", Price: 10, TypeID: 1}, // tech, price <= 50
		{Name: "Bread", Price: 3, TypeID: 2},      // food
		{Name: "Cheese", Price: 8, TypeID: 2},     // food
	})

	fmt.Println("All products:")
	fmt.Println("-------------")
	var allProducts []Product
	db.Find(&allProducts)
	for _, p := range allProducts {
		typeName := "food"
		if p.TypeID == 1 {
			typeName = "tech"
		}
		fmt.Printf("  %s: %d EUR (%s)\n", p.Name, p.Price, typeName)
	}

	schema := &domain.DataSchema{
		ID: "Product",
		Columns: []domain.SchemaColumn{
			domain.SchemaColumnSingle{ID: "name", SchemaType: domain.NativeTypeString},
			domain.SchemaColumnSingle{ID: "price", SchemaType: domain.NativeTypeInt},
		},
	}

	// Pipeline: Read tech products with price > 50 -> Multiply price by 2 -> Write to products_updated
	p := pipeline.DataPipeline{
		Source: source.NewPostgresSource(db, "products", schema,
			source.WithJoin("JOIN product_types ON products.type_id = product_types.id"),
			source.WithWhere("product_types.name = ?", "tech"),
			source.WithWhere("products.price > ?", 50),
		),
		Transform: transform.NewTransformBuilder().
			Add(&MultiplyTransform{Column: "price", Multiplier: 2}).
			Build(),
		Store: store.NewPostgresStore(db, "products_updated"),
	}

	result, err := p.RunWithResult()
	if err != nil {
		log.Fatalf("Pipeline failed: %v", err)
	}

	fmt.Println("\nPrice increase applied (x2) for tech products with price > 50:")
	fmt.Println("---------------------------------------------------------------")
	for _, record := range result.Records {
		fmt.Printf("%s: %d EUR\n", record.GetString("name"), record.GetInt("price"))
	}

	// Verify in database
	fmt.Println("\nVerifying in database:")
	var results []map[string]any
	db.Table("products_updated").Find(&results)
	for _, row := range results {
		fmt.Printf("  %s: %v EUR\n", row["name"], row["price"])
	}
}
