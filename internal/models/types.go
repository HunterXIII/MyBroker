package models

import (
	"fmt"
	"net"
	"sync"
)

type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp int64
	TTL       int64
}

type Queue struct {
	Messages []Message
	Mu       sync.RWMutex
	NewData  chan struct{}
	MaxSize  int
}

func (q *Queue) AddMessage(msg *Message) error {
	q.Mu.Lock()
	defer q.Mu.Unlock()

	if len(q.Messages) >= q.MaxSize {
		return fmt.Errorf("The number of messages in the queue has reached the limit")
	}

	q.Messages = append(q.Messages, *msg)
	select {
	case q.NewData <- struct{}{}:
		// Let's try to send a signal about new message
	default:
		// If the signal already exists in the channel
	}
	return nil
}

type Topic struct {
	Name        string
	Queue       *Queue
	Subscribers map[string]*Subscriber
}

func NewTopic(name string, maxQueueSize int) *Topic {
	return &Topic{
		Name: name,
		Queue: &Queue{
			Messages: []Message{},
			MaxSize:  maxQueueSize,
		},
		Subscribers: make(map[string]*Subscriber),
	}
}

func (t *Topic) Push(msg *Message) error {
	return t.Queue.AddMessage(msg)
}

type Subscriber struct {
	ID         string
	Connection net.Conn
	Topics     map[string]*Topic
}
