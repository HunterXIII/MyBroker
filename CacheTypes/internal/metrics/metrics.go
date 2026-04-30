package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

type Metrics struct {
	TotalRequests int64
	CacheHits     int64
	DBReads       int64
	DBWrites      int64
	Latencies     []time.Duration
	mu            sync.Mutex
}

func (m *Metrics) AddLatency(d time.Duration) {
	m.mu.Lock()
	m.Latencies = append(m.Latencies, d)
	m.mu.Unlock()
}

func (m *Metrics) Reset() {
	atomic.StoreInt64(&m.TotalRequests, 0)
	atomic.StoreInt64(&m.CacheHits, 0)
	atomic.StoreInt64(&m.DBReads, 0)
	atomic.StoreInt64(&m.DBWrites, 0)
	m.mu.Lock()
	m.Latencies = nil
	m.mu.Unlock()
}

func (m *Metrics) GetStats() (int64, int64, int64, time.Duration) {
	total := atomic.LoadInt64(&m.TotalRequests)
	hits := atomic.LoadInt64(&m.CacheHits)
	dbOps := atomic.LoadInt64(&m.DBReads) + atomic.LoadInt64(&m.DBWrites)

	var avgLat time.Duration
	m.mu.Lock()
	if len(m.Latencies) > 0 {
		var sum time.Duration
		for _, l := range m.Latencies {
			sum += l
		}
		avgLat = sum / time.Duration(len(m.Latencies))
	}
	m.mu.Unlock()

	return total, hits, dbOps, avgLat
}
