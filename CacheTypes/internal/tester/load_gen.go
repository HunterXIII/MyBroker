package tester

import (
	"cache-practice/internal/metrics"
	"cache-practice/internal/repository"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

func RunTest(repo repository.Repository, m *metrics.Metrics, readRatio float64) {
	m.Reset()
	duration := 20 * time.Second
	totalRequests := 20000
	workers := 10
	reqPerWorker := totalRequests / workers
	fmt.Printf("\n>>> Тест: %s | Нагрузка: %.0f%% Read / %.0f%% Write\n", repo.Name(), readRatio*100, (1-readRatio)*100)

	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup

	go func() {
		time.Sleep(duration)
		close(stop)
	}()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < reqPerWorker; j++ {
				select {
				case <-stop:
					return
				default:
					key := fmt.Sprintf("u:%d", rand.Intn(1000))
					if rand.Float64() < readRatio {
						repo.Get(key)
					} else {
						repo.Set(key, "v")
					}
					time.Sleep(1 * time.Millisecond)
				}
			}
		}()
	}
	wg.Wait()
	fmt.Println(" Готово!")
	total, hits, dbReads, dbWrites, avgLat := m.GetStats()
	rps := float64(total) / time.Since(start).Seconds()

	fmt.Printf("Results: RPS: %.2f | Latency: %v | HitRate: %.2f%% | DB Reads: %d | DB Writes: %d\n",
		rps, avgLat, float64(hits)/float64(total)*100, dbReads, dbWrites)
}
