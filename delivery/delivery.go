package delivery

import (
	"context"
	"log/slog"
	"sync"

	"github.com/HunterXIII/MyBroker/internal/models"
)

type DeliveryTask struct {
	Msg  *models.Message
	Subs []*models.Subscriber
}

type DeliveryEngine struct {
	Tasks        chan *DeliveryTask
	Mu           sync.RWMutex
	Log          *slog.Logger
	OfflineQueue []*models.Message
}

func NewDeliveryEngine(logger *slog.Logger) *DeliveryEngine {
	return &DeliveryEngine{
		Tasks: make(chan *DeliveryTask, 2048), // HARD
		Log:   logger,
	}
}

func (d *DeliveryEngine) Run(ctx context.Context) {
	go func() {
		d.Log.Info("Delivery Engine started")
		for {
			select {
			case task, ok := <-d.Tasks:
				if !ok {
					d.Log.Info("Channel is closed")
					d.Log.Info("Delivery Engine is stopping")
					return
				}
				d.dispatch(task)

			case <-ctx.Done():
				d.Log.Info("Delivery Engine is stopping")
				return
			}
		}
	}()
}

func (d *DeliveryEngine) dispatch(task *DeliveryTask) {
	// Send messages to subscribers

	for _, sub := range task.Subs {
		select {
		case sub.Messages <- task.Msg:
			// sucessful
		default:
			d.Log.Warn("Subscriber channel of messages is full", "SubID", sub.ID, "MsgID", task.Msg.ID)
		}
	}
}

func (d *DeliveryEngine) GetMessageFromOffQueue(topicName string) []*models.Message {
	d.Mu.Lock()
	defer d.Mu.Unlock()
	messages := []*models.Message{}
	idxDelete := []int{}
	for i, msg := range d.OfflineQueue {
		if msg.Topic == topicName {
			messages = append(messages, msg)
			idxDelete = append(idxDelete, i)
		}
	}
	d.Log.Debug("New msg to the OfflineQueue")
	for _, idx := range idxDelete {
		for range d.OfflineQueue {
			d.OfflineQueue = append(d.OfflineQueue[:idx], d.OfflineQueue[idx+1:]...)
		}
	}

	return messages
}
