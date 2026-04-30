package repository

import (
	"cache-practice/internal/metrics"
	"database/sql"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type CacheAside struct {
	DB    *sql.DB
	Cache *redis.Client
	M     *metrics.Metrics
}

func (s *CacheAside) Get(key string) (string, error) {
	start := time.Now()
	atomic.AddInt64(&s.M.TotalRequests, 1)

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

func (s *CacheAside) Set(key, value string) error {
	start := time.Now()
	atomic.AddInt64(&s.M.TotalRequests, 1)
	atomic.AddInt64(&s.M.DBWrites, 1)

	_, err := s.DB.Exec("INSERT INTO kv (key, val) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET val = $2", key, value)
	s.Cache.Del(Ctx, key)

	s.M.AddLatency(time.Since(start))
	return err
}

func (s *CacheAside) Name() string { return "Cache-Aside" }
