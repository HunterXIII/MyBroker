package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"broker-comparisson/internal/broker"

	"github.com/google/uuid"
)

func main() {
	brokerType := flag.String("type", "rabbit", "Тип брокера: rabbit или redis")
	msgSize := flag.Int("size", 128, "Размер сообщения в байтах")
	totalCount := flag.Int("count", 10000, "Общее количество сообщений")
	workers := flag.Int("workers", 10, "Количество параллельных воркеров")
	rate := flag.Int("rate", 0, "Лимит сообщений в секунду (0 - без лимита)")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var b broker.Broker
	var err error

	switch *brokerType {
	case "rabbit":
		b, err = broker.NewRabbitBroker("amqp://guest:guest@localhost:5672/", "test-queue")
	case "redis":
		b, err = broker.NewRedisBroker("localhost:6379", "", "test-stream")
	default:
		log.Fatalf("Неизвестный тип брокера: %s", *brokerType)
	}
	if err != nil {
		log.Fatalf("Ошибка подключения: %v", err)
	}
	defer b.Close()

	payload := make([]byte, *msgSize)
	for i := range payload {
		payload[i] = 'a'
	}

	fmt.Printf("Запуск теста [%s]\n", *brokerType)
	fmt.Printf("Сообщений: %d | Размер: %d байт | Воркеры: %d\n", *totalCount, *msgSize, *workers)

	start := time.Now()
	var wg sync.WaitGroup
	msgPerWorker := *totalCount / *workers

	var throttle <-chan time.Time
	if *rate > 0 {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
		throttle = ticker.C
	}

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < msgPerWorker; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					if throttle != nil {
						<-throttle
					}

					msg := broker.Message{
						ID:        uuid.New().String(),
						Payload:   payload,
						CreatedAt: time.Now().UnixNano(),
					}

					if err := b.Send(ctx, msg); err != nil {
						log.Printf("Worker %d: ошибка отправки: %v", workerID, err)
						continue
					}
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Println("---")
	fmt.Printf("Отправка завершена за %v\n", duration)
	fmt.Printf("Средняя скорость отправки: %.2f msg/sec\n", float64(*totalCount)/duration.Seconds())
}
