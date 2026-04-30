package repository

import (
	"database/sql"
	"sync/atomic"
	"time"

	"cache-practice/internal/metrics"

	"github.com/redis/go-redis/v9"
)

type WriteThrough struct {
	DB    *sql.DB
	Cache *redis.Client
	M     *metrics.Metrics
}

func (s *WriteThrough) Get(key string) (string, error) {
	start := time.Now()
	atomic.AddInt64(&s.M.TotalRequests, 1)

	// Читаем из кеша
	val, err := s.Cache.Get(Ctx, key).Result()
	if err == nil {
		atomic.AddInt64(&s.M.CacheHits, 1)
		s.M.AddLatency(time.Since(start))
		return val, nil
	}

	atomic.AddInt64(&s.M.DBReads, 1)
	err = s.DB.QueryRow("SELECT val FROM kv WHERE key = $1", key).Scan(&val)
	if err == nil {
		s.Cache.Set(Ctx, key, val, time.Minute)
	}

	s.M.AddLatency(time.Since(start))
	return val, err
}

func (s *WriteThrough) Set(key, value string) error {
	start := time.Now()
	atomic.AddInt64(&s.M.TotalRequests, 1)
	atomic.AddInt64(&s.M.DBWrites, 1)

	_, err := s.DB.Exec("INSERT INTO kv (key, val) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET val = $2", key, value)
	if err != nil {
		return err
	}

	err = s.Cache.Set(Ctx, key, value, time.Minute).Err()

	s.M.AddLatency(time.Since(start))
	return err
}

func (s *WriteThrough) Name() string { return "Write-Through" }
