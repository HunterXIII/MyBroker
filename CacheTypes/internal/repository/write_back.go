package repository

import (
	"database/sql"
	"sync/atomic"
	"time"

	"cache-practice/internal/metrics"

	"github.com/redis/go-redis/v9"
)

type WriteBack struct {
	DB    *sql.DB
	Cache *redis.Client
	M     *metrics.Metrics
	queue chan [2]string // Очередь для записи в БД
}

func NewWriteBack(db *sql.DB, cache *redis.Client, m *metrics.Metrics) *WriteBack {
	wb := &WriteBack{
		DB:    db,
		Cache: cache,
		M:     m,
		queue: make(chan [2]string, 10000),
	}

	for i := 0; i < 10; i++ {
		go wb.flushWorker()
	}
	return wb
}

func (s *WriteBack) flushWorker() {
	for item := range s.queue {
		atomic.AddInt64(&s.M.DBWrites, 1)
		_, _ = s.DB.Exec("INSERT INTO kv (key, val) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET val = $2", item[0], item[1])
	}
}

func (s *WriteBack) Get(key string) (string, error) {
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
	s.M.AddLatency(time.Since(start))
	return val, err
}

func (s *WriteBack) Set(key, value string) error {
	start := time.Now()
	atomic.AddInt64(&s.M.TotalRequests, 1)

	err := s.Cache.Set(Ctx, key, value, time.Minute).Err()
	if err != nil {
		return err
	}

	s.queue <- [2]string{key, value}

	s.M.AddLatency(time.Since(start))
	return nil
}

func (s *WriteBack) Name() string { return "Write-Back" }
