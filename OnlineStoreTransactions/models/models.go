package models

import "time"

type Customer struct {
	ID        int
	FirstName string
	LastName  string
	Email     string
}

type Product struct {
	ID    int
	Name  string
	Price float64
}

type Order struct {
	ID          int
	CustomerID  int
	OrderDate   time.Time
	TotalAmount float64
}

type OrderItem struct {
	ID        int
	OrderID   int
	ProductID int
	Quantity  int
	Subtotal  float64
}
