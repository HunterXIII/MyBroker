package models

import (
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
	NewData  chan any
	MaxSize  int
}

type Topic struct {
	Name        string
	Queue       *Queue
	Subscribers map[string]*Subscriber
}

type Subscriber struct {
	ID         string
	Connection net.Conn
	Topics     map[string]*Topic
}
