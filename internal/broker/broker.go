package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/HunterXIII/MyBroker/internal/delivery"
	"github.com/HunterXIII/MyBroker/internal/models"
	"github.com/HunterXIII/MyBroker/internal/storage"
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
	Context     context.Context
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
	b.Context = ctx
	b.ctxCancel = cancel

	b.Delivery.Run(ctx)

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
		InFlight: make(map[uint16]uint64),
		Log:      b.Log.With("SubID", cl.ID),
	}
	b.Subscribers = append(b.Subscribers, sub)

	sub.StartWorker(b.Context)

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
	if err := b.Storage.SaveSubscription(id, topicName); err != nil {
		b.Log.Error("Failed to save subscription to disk", "err", err)
		return err
	}

	sub := b.GetSubscriber(id)
	topic := b.GetOrCreateNewTopic(topicName)
	topic.AddSubscriber(sub)

	b.Log.Info("Client subscribed", "ClientID", id, "topic", topicName)

	lastOffset := b.Storage.GetClientOffset(id)

	missedMsgs, err := b.Storage.GetMessagesSince(lastOffset)
	if err != nil {
		b.Log.Error("Failed to recover missed messages", "err", err)
		return err
	}

	if len(missedMsgs) > 0 {
		b.Log.Info("Recovering history", "ClientID", id, "Count", len(missedMsgs))

		for _, msg := range missedMsgs {
			if msg.Topic == topicName {
				b.Delivery.Tasks <- &delivery.DeliveryTask{
					Msg:  &msg,
					Subs: []*models.Subscriber{sub},
				}
			}
		}
	}

	return nil
}

func (b *BrokerService) Unsubscribe(id string, topicName string) error {
	b.Storage.RemoveSubscription(id, topicName)
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
	msg := &models.Message{
		Topic:     topicName,
		Payload:   payload,
		ExpiresAt: time.Now().Add(time.Duration(b.Config.TTL) * time.Second),
		TTL:       b.Config.TTL,
	}

	var err error
	msg.Offset, err = b.Storage.SaveMessage(msg)
	if err != nil {
		b.Log.Error("The message wasn't logged, aborting delivery", "error", err)
		return err
	}

	topic := b.GetOrCreateNewTopic(topicName)

	_ = topic.Push(msg)

	b.Delivery.Tasks <- &delivery.DeliveryTask{
		Msg:  msg,
		Subs: topic.Subscribers,
	}

	b.Log.Debug("Message logged and task created", "Offset", msg.Offset, "Topic", msg.Topic)
	return nil
}

func (b *BrokerService) Stop() {
	for _, sub := range b.Subscribers {
		close(sub.Messages)
		sub.Done <- struct{}{}
	}
	b.ctxCancel()

}
