package broker

import "context"

type Message struct {
	ID        string
	Payload   []byte
	CreatedAt int64 // Наносекунды для замера latency
}

type Broker interface {
	Send(ctx context.Context, msg Message) error
	Receive(ctx context.Context) (<-chan Message, error)
	Close() error
}
