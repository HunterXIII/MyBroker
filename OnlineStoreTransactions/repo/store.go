package repo

import (
	"context"
	"database/sql"
	"fmt"
	"task1/models"
)

type StoreRepository struct {
	db *sql.DB
}

func NewRepository(db *sql.DB) *StoreRepository {
	return &StoreRepository{db: db}
}

func (r *StoreRepository) InitSchemas(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS Customers (
		CustomerID SERIAL PRIMARY KEY,
		FirstName TEXT NOT NULL,
		LastName TEXT NOT NULL,
		Email TEXT UNIQUE NOT NULL
	);

	CREATE TABLE IF NOT EXISTS Products (
		ProductID SERIAL PRIMARY KEY,
		ProductName TEXT NOT NULL,
		Price NUMERIC(10, 2) NOT NULL
	);

	CREATE TABLE IF NOT EXISTS Orders (
		OrderID SERIAL PRIMARY KEY,
		CustomerID INTEGER REFERENCES Customers(CustomerID),
		OrderDate TIMESTAMP DEFAULT NOW(),
		TotalAmount NUMERIC(10, 2) DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS OrderItems (
		OrderItemID SERIAL PRIMARY KEY,
		OrderID INTEGER REFERENCES Orders(OrderID),
		ProductID INTEGER REFERENCES Products(ProductID),
		Quantity INTEGER NOT NULL,
		Subtotal NUMERIC(10, 2) NOT NULL
	);
	`

	_, err := r.db.ExecContext(ctx, schema)
	if err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	return nil
}

func (r *StoreRepository) CreateCustomer(ctx context.Context, c models.Customer) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	var customerID int
	query := `
		INSERT INTO Customers (FirstName, LastName, Email) 
		VALUES ($1, $2, $3) 
		RETURNING CustomerID`

	err = tx.QueryRowContext(ctx, query, c.FirstName, c.LastName, c.Email).Scan(&customerID)
	if err != nil {
		return 0, fmt.Errorf("insert customer: %w", err)
	}

	return customerID, tx.Commit()
}

// Сценарий 1: Размещение заказа
func (r *StoreRepository) PlaceOrder(ctx context.Context, customerID int, items []models.OrderItem) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	var orderID int
	err = tx.QueryRowContext(ctx,
		"INSERT INTO Orders (CustomerID, OrderDate, TotalAmount) VALUES ($1, NOW(), 0) RETURNING OrderID",
		customerID,
	).Scan(&orderID)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	var totalAmount float64

	for _, item := range items {
		_, err = tx.ExecContext(ctx,
			"INSERT INTO OrderItems (OrderID, ProductID, Quantity, Subtotal) VALUES ($1, $2, $3, $4)",
			orderID, item.ProductID, item.Quantity, item.Subtotal,
		)
		if err != nil {
			return fmt.Errorf("insert item (product %d): %w", item.ProductID, err)
		}
		totalAmount += item.Subtotal
	}

	_, err = tx.ExecContext(ctx, "UPDATE Orders SET TotalAmount = $1 WHERE OrderID = $2", totalAmount, orderID)
	if err != nil {
		return fmt.Errorf("update order total: %w", err)
	}

	return tx.Commit()
}

// Сценарий 2: Обновление email
func (r *StoreRepository) UpdateEmail(ctx context.Context, customerID int, email string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "UPDATE Customers SET Email = $1 WHERE CustomerID = $2", email, customerID)
	if err != nil {
		return fmt.Errorf("update email: %w", err)
	}

	return tx.Commit()
}

// Сценарий 3: Добавление продукта
func (r *StoreRepository) AddProduct(ctx context.Context, p models.Product) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var id int
	err = tx.QueryRowContext(ctx, "INSERT INTO Products (ProductName, Price) VALUES ($1, $2) RETURNING ProductID", p.Name, p.Price).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("add product: %w", err)
	}

	return id, tx.Commit()
}
