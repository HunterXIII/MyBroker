package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"broker-comparisson/internal/broker"
	"broker-comparisson/internal/metrics"
)

func main() {
	brokerType := flag.String("type", "rabbit", "Тип брокера для теста (rabbit или redis)")
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
		log.Fatalf("Неизвестный брокер: %s", *brokerType)
	}
	if err != nil {
		log.Fatalf("Ошибка подключения к %s: %v", *brokerType, err)
	}
	defer b.Close()

	stats := metrics.NewStats()

	msgChan, err := b.Receive(ctx)
	if err != nil {
		log.Fatalf("Ошибка при запуске consumer: %v", err)
	}

	fmt.Printf("Консьюмер [%s] готов к работе.\n", *brokerType)
	fmt.Println("Жду сообщения... Нажми Ctrl+C, когда продюсер закончит, чтобы увидеть отчет.")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgChan:
				if !ok {
					return
				}

				now := time.Now().UnixNano()
				if msg.CreatedAt > 0 {
					latency := time.Duration(now - msg.CreatedAt)
					stats.AddLatency(latency)
				}
			}
		}
	}()

	<-ctx.Done()

	p95, avg, throughput := stats.GetResults()

	fmt.Println("\n" + "========================================")
	fmt.Printf("ОТЧЕТ ПО ТЕСТИРОВАНИЮ [%s]\n", *brokerType)
	fmt.Println("========================================")
	fmt.Printf("Получено сообщений:  %d\n", stats.TotalReceived)
	fmt.Printf("Пропускная способность: %.2f msg/sec\n", throughput)
	fmt.Printf("Средняя задержка:    %v\n", avg)
	fmt.Printf("P95 Latency:         %v\n", p95)
	fmt.Println("========================================")
}
