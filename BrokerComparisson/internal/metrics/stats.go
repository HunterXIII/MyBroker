package metrics

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	TotalSent     uint64
	TotalReceived uint64
	latencies     []time.Duration
	mu            sync.Mutex // Нужна только для записи в слайс latencies
	startTime     time.Time
}

func NewStats() *Stats {
	return &Stats{
		latencies: make([]time.Duration, 0, 100000), // Сразу аллоцируем место
		startTime: time.Now(),
	}
}

// AddLatency добавляет задержку сообщения в общий список
func (s *Stats) AddLatency(d time.Duration) {
	s.mu.Lock()
	s.latencies = append(s.latencies, d)
	s.mu.Unlock()
	atomic.AddUint64(&s.TotalReceived, 1)
}

// AddSent инкрементирует счетчик отправленных
func (s *Stats) AddSent() {
	atomic.AddUint64(&s.TotalSent, 1)
}

// GetResults возвращает итоговые расчеты
func (s *Stats) GetResults() (p95 time.Duration, avg time.Duration, msgPerSec float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.latencies) == 0 {
		return 0, 0, 0
	}

	// Считаем среднюю задержку
	var total time.Duration
	for _, l := range s.latencies {
		total += l
	}
	avg = total / time.Duration(len(s.latencies))

	// Считаем P95
	sort.Slice(s.latencies, func(i, j int) bool {
		return s.latencies[i] < s.latencies[j]
	})
	p95 = s.latencies[int(float64(len(s.latencies))*0.95)]

	// Считаем throughput
	duration := time.Since(s.startTime).Seconds()
	msgPerSec = float64(s.TotalReceived) / duration

	return p95, avg, msgPerSec
}
