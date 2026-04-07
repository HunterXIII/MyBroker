package broker

import (
	"log/slog"
	"sync"
	"time"

	"github.com/HunterXIII/MyBroker/internal/models"
	"github.com/HunterXIII/MyBroker/internal/storage"
	"github.com/google/uuid"
)

type BrokerConfig struct {
	MaxQueueSize int
	TTL          int64
}

type BrokerService struct {
	Topics  map[string]*models.Topic
	Storage *storage.StorageService
	Mu      sync.RWMutex
	Log     *slog.Logger
	Config  BrokerConfig
}

func NewBrokerService(storage *storage.StorageService, log *slog.Logger, cfg BrokerConfig) *BrokerService {
	return &BrokerService{
		Storage: storage,
		Log:     log,
		Config:  cfg,
	}
}

func (b *BrokerService) GetOrCreateNewTopic(name string) *models.Topic {
	b.Mu.Lock()
	defer b.Mu.Unlock()

	if topic, ok := b.Topics[name]; ok {
		return topic
	}

	b.Log.Info("Create new topic", "topic", name)
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
