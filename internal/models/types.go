package models

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

type Message struct {
	Topic     string
	Payload   []byte
	ExpiresAt time.Time
	TTL       int64
	Offset    uint64
	PacketID  uint16
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

	nextPacketID uint16
	InFlight     map[uint16]uint64
	InFlightMu   sync.RWMutex

	Log *slog.Logger
}

func (s *Subscriber) NextPacketID() uint16 {
	s.InFlightMu.Lock()
	defer s.InFlightMu.Unlock()

	s.nextPacketID++
	return s.nextPacketID
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

				packetID := s.NextPacketID()
				s.InFlightMu.Lock()
				s.InFlight[packetID] = msg.Offset
				s.InFlightMu.Unlock()

				pk := s.buildPacket(packetID, msg)

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
func (s *Subscriber) buildPacket(packetID uint16, msg *Message) packets.Packet {
	return packets.Packet{
		FixedHeader: packets.FixedHeader{
			Type: packets.Publish,
			Qos:  1,
		},
		TopicName: msg.Topic,
		Payload:   msg.Payload,
		PacketID:  packetID,
	}
}
