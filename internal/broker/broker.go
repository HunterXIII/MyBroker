package broker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/HunterXIII/MyBroker/delivery"
	"github.com/HunterXIII/MyBroker/internal/models"
	"github.com/HunterXIII/MyBroker/internal/storage"
	"github.com/google/uuid"
	mqtt "github.com/mochi-mqtt/server/v2"
)

type BrokerConfig struct {
	MaxQueueSize int
	TTL          int64
}

type BrokerService struct {
	Server      *mqtt.Server
	Topics      map[string]*models.Topic
	Subscribers []*models.Subscriber
	Storage     *storage.StorageService
	Delivery    *delivery.DeliveryEngine
	Mu          sync.RWMutex
	ctxCancel   context.CancelFunc
	Log         *slog.Logger
	Config      *BrokerConfig
}

func NewBrokerService(svr *mqtt.Server, s *storage.StorageService, d *delivery.DeliveryEngine, log *slog.Logger, cfg *BrokerConfig) *BrokerService {
	return &BrokerService{
		Server:      svr,
		Storage:     s,
		Delivery:    d,
		Log:         log,
		Config:      cfg,
		Topics:      make(map[string]*models.Topic),
		Subscribers: []*models.Subscriber{},
	}
}

func (b *BrokerService) Start() error {
	b.Log.Info("Starting Broker Service...")

	ctx, cancel := context.WithCancel(context.Background())
	b.ctxCancel = cancel

	b.Delivery.Run(ctx)

	b.Log.Info("Recovering unprocessed messages...")
	if err := b.RecoveryMessage(); err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}

	b.Log.Info("MQTT Server is listening on :1883")
	return b.Server.Serve()
}

func (b *BrokerService) AddSubscriber(cl *mqtt.Client) error {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	sub := &models.Subscriber{
		ID:       cl.ID,
		Client:   cl,
		Messages: make(chan *models.Message, 128), //HARD
		Done:     make(chan struct{}, 1),
		Log:      b.Log,
	}
	b.Subscribers = append(b.Subscribers, sub)

	b.Log.Info("Registry new subscriber", "ClientID", cl.ID, "service", "BrokerService")

	return nil
}

func (b *BrokerService) RemoveSubsciber(id string) error {

	var idx int
	var sub *models.Subscriber
	for i, s := range b.Subscribers {
		if s.ID == id {
			idx = i
			sub = s
			break
		}
	}

	b.Subscribers = append(b.Subscribers[:idx], b.Subscribers[idx+1:]...)

	for _, topic := range b.Topics {
		if ok := topic.RemoveSubsciber(sub.ID); ok {
			b.Log.Info("Client unscribe from the topic", "ClientID", sub.ID, "topic", topic.Name, "service", "BrokerService")
		}
	}

	b.Log.Info("Deleted subscriber", "ClientID", sub.ID, "service", "BrokerService")
	return nil
}

func (b *BrokerService) Subscribe(id string, topicName string) error {
	sub := b.GetSubscriber(id)
	topic := b.GetOrCreateNewTopic(topicName)
	topic.AddSubscriber(sub)
	b.Log.Info("Client subscribed to the topic", "ClientID", id, "topic", topicName, "service", "BrokerService")
	return nil
}

func (b *BrokerService) Unsubscribe(id string, topicName string) error {
	sub := b.GetSubscriber(id)
	topic := b.GetOrCreateNewTopic(topicName)
	topic.RemoveSubsciber(sub.ID)
	b.Log.Info("Client unscribe from the topic", "ClientID", sub.ID, "topic", topic.Name, "service", "BrokerService")
	return nil
}

func (b *BrokerService) GetSubscriber(id string) *models.Subscriber {
	for _, sub := range b.Subscribers {
		if sub.ID == id {
			return sub
		}
	}
	return nil
}

func (b *BrokerService) GetOrCreateNewTopic(name string) *models.Topic {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if topic, ok := b.Topics[name]; ok {
		return topic
	}

	b.Log.Info("Create new topic", "topic", name, "service", "BrokerService")
	b.Topics[name] = models.NewTopic(name, b.Config.MaxQueueSize)

	return b.Topics[name]
}

func (b *BrokerService) NewMessage(topicName string, payload []byte) error {
	topic := b.GetOrCreateNewTopic(topicName)
	message := &models.Message{
		ID:        uuid.NewString(),
		Topic:     topicName,
		Payload:   payload,
		Timestamp: time.Now().Unix(),
		TTL:       b.Config.TTL,
	}

	err := topic.Push(message)
	if err != nil {
		b.Log.Error("The message wasn't added to the queue", "error", err)
	}

	err = b.Storage.SaveMessage(message)
	if err != nil {
		b.Log.Error("The message wasn't logged", "error", err)
	}

	b.Delivery.Tasks <- &delivery.DeliveryTask{
		Msg:  message,
		Subs: topic.Subscribers,
	}

	return err
}

func (b *BrokerService) RecoveryMessage() error {
	messages, err := b.Storage.LoadUnprocessed()
	if err != nil {
		return err
	}

	if len(messages) == 0 {
		return nil
	}

	for _, msg := range messages {
		topic := b.GetOrCreateNewTopic(msg.Topic)
		if err := topic.Push(msg); err != nil {
			b.Log.Error("Recovery: failed to push msg", "id", msg.ID, "err", err)
			continue
		}
	}

	size, _ := b.Storage.GetCurrentFileSize()
	return b.Storage.SaveOffset(size)
}

func (b *BrokerService) Stop() {
	for _, sub := range b.Subscribers {
		close(sub.Messages)
		sub.Done <- struct{}{}
	}
	b.ctxCancel()

}
