#!/bin/bash
set -e

cd "$(dirname "$0")/../.."

echo "Starting PostgreSQL..."
docker compose up -d --wait

echo "Running price increase pipeline..."
go run ./samples/price_increase/main.go

echo ""
echo "Verifying output..."

# Query database to verify
RESULT=$(docker compose exec -T postgres psql -U pipeforge -d pipeforge -t -c "SELECT price FROM products_updated WHERE name = 'Laptop'")
PRICE=$(echo $RESULT | tr -d ' ')

if [ "$PRICE" = "1998" ]; then
    echo "PASS: Laptop price correctly doubled to 1998"
else
    echo "FAIL: Expected 1998, got $PRICE"
    docker compose down
    exit 1
fi

echo "PASS: All validations successful"

docker compose down
