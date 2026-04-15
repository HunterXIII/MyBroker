package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"task1/models"
	"task1/repo"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
)

func main() {
	godotenv.Load()

	connectString := os.Getenv("DATABASE_URL")

	db, err := sql.Open("pgx", connectString)
	if err != nil {
		log.Fatalf("Failed to open db: %s", err)
		return
	}
	defer db.Close()

	repo := repo.NewRepository(db)
	ctx := context.Background()

	if err := repo.InitSchemas(ctx); err != nil {
		log.Fatalf("Failed to initialize database schemas: %s", err)
		return
	}

	// Создание клиента
	clientID, err := repo.CreateCustomer(ctx, models.Customer{
		FirstName: "Roman",
		LastName:  "Volkov",
		Email:     "test@example.com",
	})
	if err != nil {
		log.Fatalf("Failed to create User: %s", err)
		return
	}

	// Сценарий 3
	productID, err := repo.AddProduct(ctx, models.Product{
		Name:  "Шоколадка",
		Price: 100,
	})
	if err != nil {
		log.Fatalf("Failed to create User: %s", err)
		return
	}

	// Сценарий 1
	err = repo.PlaceOrder(ctx, clientID, []models.OrderItem{
		{ProductID: productID, Quantity: 1, Subtotal: 150.0},
	})

	if err != nil {
		log.Printf("Failed place order: %s", err)
		return
	}

	// Сценарий 2
	newEmail := fmt.Sprintf("%d@example.com", productID)
	err = repo.UpdateEmail(ctx, clientID, newEmail)
	if err != nil {
		log.Fatalf("Failed update email: %s", err)
		return
	}

	log.Println("Success transactions!")

}
