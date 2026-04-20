package broker

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitBroker struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queueName string
}

func NewRabbitBroker(url, queueName string) (*RabbitBroker, error) {

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	// fmt.Println("Соединение установлено")

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}
	// fmt.Println("Канал открыт")

	_, err = ch.QueueDeclare(
		queueName,
		false, // durable (сохранять на диск) — для теста ставим false, чтобы тестировать RAM-лимиты
		false, // auto-delete (удалить когда все отключатся)
		false, // exclusive (только для этого соединения)
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}
	// fmt.Println("Очередь создана")

	return &RabbitBroker{
		conn:      conn,
		ch:        ch,
		queueName: queueName,
	}, nil
}

func (r *RabbitBroker) Send(ctx context.Context, msg Message) error {
	return r.ch.PublishWithContext(ctx,
		"",
		r.queueName,
		false,
		false,
		amqp.Publishing{
			MessageId: msg.ID,
			Headers: amqp.Table{
				"CreatedAt": msg.CreatedAt,
			},
			Body: msg.Payload,
		})
}

func (r *RabbitBroker) Receive(ctx context.Context) (<-chan Message, error) {
	msgs, err := r.ch.Consume(
		r.queueName,
		"",
		true, // auto-ack (ВАЖНО: true = макс. скорость, брокер "забывает" msg сразу после отправки. Для теста at-least-once меняют на false)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer: %w", err)
	}

	outChan := make(chan Message, 1000)

	go func() {
		defer close(outChan)
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}

				var createdAt int64
				if val, exists := d.Headers["CreatedAt"]; exists {
					if t, ok := val.(int64); ok {
						createdAt = t
					}
				}

				outChan <- Message{
					ID:        d.MessageId,
					Payload:   d.Body,
					CreatedAt: createdAt,
				}
			}
		}
	}()

	return outChan, nil
}

func (r *RabbitBroker) Close() error {
	if err := r.ch.Close(); err != nil {
		return err
	}
	return r.conn.Close()
}

func (r *RabbitBroker) Purge() error {
	_, err := r.ch.QueuePurge(r.queueName, false)
	return err
}
