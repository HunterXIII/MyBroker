package models

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
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
	Subscribers []*Subscriber
}

func NewTopic(name string, maxQueueSize int) *Topic {
	return &Topic{
		Name: name,
		Queue: &Queue{
			Messages: []Message{},
			MaxSize:  maxQueueSize,
		},
		Subscribers: []*Subscriber{},
	}
}

func (t *Topic) Push(msg *Message) error {
	return t.Queue.AddMessage(msg)
}

func (t *Topic) AddSubscriber(sub *Subscriber) error {
	t.Subscribers = append(t.Subscribers, sub)
	return nil
}

// TODO: Sub must be contein filed of topics
func (t *Topic) RemoveSubsciber(id string) bool {
	for idx, sub := range t.Subscribers {
		if sub.ID == id {
			t.Subscribers = append(t.Subscribers[:idx], t.Subscribers[idx+1:]...)
			return true
		}
	}
	return false
}

type Subscriber struct {
	ID       string
	Client   *mqtt.Client
	Messages chan *Message
	Done     chan struct{}
	Log      *slog.Logger
}

func (s *Subscriber) StartWorker(ctx context.Context) {
	s.Log.Info("Start Sub's worker")
	go func() {
		defer func() {
			// TODO
			s.Log.Info("Stop Sub's worker")
		}()

		for {
			select {
			case msg, ok := <-s.Messages:
				if !ok {
					return
				}

				pk := s.buildPacket(msg)

				err := s.Client.WritePacket(pk)
				if err != nil {
					s.Log.Error("Failed to send message", "ClientID", s.ID, "err", err)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// TODD: Change Qos with 1 for at least once
func (s *Subscriber) buildPacket(msg *Message) packets.Packet {
	return packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
			Qos:  0,
		},
		TopicName: msg.Topic,
		Payload:   msg.Payload,
	}
}
