package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"cache-practice/internal/metrics"
	"cache-practice/internal/repository"
	"cache-practice/internal/tester"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {

	godotenv.Load()

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_NAME"))

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatal(err)
		return
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Minute)
	log.Printf("Connected to DB successfully %s", dsn)

	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT")),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       0,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Ошибка подключения к Redis: %v", err)
	}

	log.Printf("Connected to Redis successfully at %s:%s", os.Getenv("REDIS_HOST"), os.Getenv("REDIS_PORT"))
	defer rdb.Close()

	m := &metrics.Metrics{}

	strategies := []repository.Repository{
		&repository.CacheAside{DB: db, Cache: rdb, M: m},
		&repository.WriteThrough{DB: db, Cache: rdb, M: m},
		repository.NewWriteBack(db, rdb, m),
	}

	ratios := []float64{0.8, 0.5, 0.2}

	for _, s := range strategies {
		log.Printf("Start strategies: %s", s.Name())
		for _, r := range ratios {
			tester.RunTest(s, m, r)
			rdb.FlushAll(repository.Ctx)
			db.Exec("TRUNCATE kv")
			time.Sleep(1 * time.Second)
		}
	}
}
