package broker

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type RedisBroker struct {
	client     *redis.Client
	streamName string
}

func NewRedisBroker(addr, password, streamName string) (*RedisBroker, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisBroker{
		client:     rdb,
		streamName: streamName,
	}, nil
}

func (r *RedisBroker) Send(ctx context.Context, msg Message) error {
	err := r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.streamName,
		MaxLen: 100000, // Ограничиваем длину стрима, чтобы не забить RAM при долгом тесте
		Approx: true,   // Оптимизация удаления старых записей
		Values: map[string]interface{}{
			"id":         msg.ID,
			"created_at": msg.CreatedAt,
			"payload":    msg.Payload,
		},
	}).Err()

	return err
}

func (r *RedisBroker) Receive(ctx context.Context) (<-chan Message, error) {
	outChan := make(chan Message, 1000)

	go func() {
		defer close(outChan)

		lastID := "$"

		for {
			select {
			case <-ctx.Done():
				return
			default:

				streams, err := r.client.XRead(ctx, &redis.XReadArgs{
					Streams: []string{r.streamName, lastID},
					Count:   10, // Берем пачками по 10 для скорости
					Block:   0,  // Блокируемся до появления данных
				}).Result()

				if err != nil {
					if err == context.Canceled {
						return
					}
					// В реальном приложении тут нужен логгер
					continue
				}

				for _, stream := range streams {
					for _, xMsg := range stream.Messages {
						// Обновляем lastID, чтобы в следующий раз читать после этого сообщения
						lastID = xMsg.ID

						// Парсим поля
						var createdAt int64
						if val, ok := xMsg.Values["created_at"].(string); ok {
							createdAt, _ = strconv.ParseInt(val, 10, 64)
						}

						var payload []byte
						if val, ok := xMsg.Values["payload"].(string); ok {
							payload = []byte(val)
						}

						msgID := ""
						if val, ok := xMsg.Values["id"].(string); ok {
							msgID = val
						}

						outChan <- Message{
							ID:        msgID,
							Payload:   payload,
							CreatedAt: createdAt,
						}
					}
				}
			}
		}
	}()

	return outChan, nil
}

func (r *RedisBroker) Close() error {
	return r.client.Close()
}

func (r *RedisBroker) Purge(ctx context.Context) error {
	return r.client.Del(ctx, r.streamName).Err()
}
