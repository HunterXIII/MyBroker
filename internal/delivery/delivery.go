package delivery

import (
	"context"
	"log/slog"
	"time"

	"github.com/HunterXIII/MyBroker/internal/metrics"
	"github.com/HunterXIII/MyBroker/internal/models"
	"github.com/HunterXIII/MyBroker/internal/storage"
)

type DeliveryTask struct {
	Msg  *models.Message
	Subs []*models.Subscriber
}

type DeliveryEngine struct {
	Tasks   chan *DeliveryTask
	storage *storage.StorageService
	Log     *slog.Logger
}

func NewDeliveryEngine(logger *slog.Logger, storage *storage.StorageService) *DeliveryEngine {
	return &DeliveryEngine{
		Tasks:   make(chan *DeliveryTask, 2048), // TODO: HARD
		Log:     logger,
		storage: storage,
	}
}

func (d *DeliveryEngine) Run(ctx context.Context) {
	d.Log.Info("Delivery Engine started")

	go func() {
		ticker := time.NewTicker(time.Second * 5)
		for {
			select {
			case <-ticker.C:
				metrics.DeliveryQueueLength.Set(float64(len(d.Tasks)))
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case task, ok := <-d.Tasks:
				if !ok {
					d.Log.Info("Tasks channel closed, stopping engine")
					return
				}

				if time.Now().After(task.Msg.ExpiresAt) {
					d.Log.Info("Message expired, skipping delivery", "topic", task.Msg.Topic, "offset", task.Msg.Offset)
					for _, sub := range task.Subs {
						d.storage.MarkAsDelivered(sub.ID, task.Msg.Offset)
					}
					continue
				}
				d.dispatch(task)

			case <-ctx.Done():
				d.Log.Info("Delivery Engine received shutdown signal")
				return
			}
		}
	}()
}

func (d *DeliveryEngine) dispatch(task *DeliveryTask) {
	for _, sub := range task.Subs {
		msgToSend := *task.Msg

		select {
		case sub.Messages <- &msgToSend:
			d.Log.Debug("Message dispatched", "SubID", sub.ID, "Offset", task.Msg.Offset)
		case <-time.After(2 * time.Second):
			d.Log.Error("Delivery timeout - client too slow", "SubID", sub.ID)
		}
	}
}
