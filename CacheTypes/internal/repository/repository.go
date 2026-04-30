package repository

import "context"

var Ctx = context.Background()

type Repository interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Name() string
}
