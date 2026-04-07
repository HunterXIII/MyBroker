package delivery

import (
	"context"
	"log/slog"

	"github.com/HunterXIII/MyBroker/internal/models"
)

type DeliveryTask struct {
	Msg  *models.Message
	Subs []*models.Subscriber
}

type DeliveryEngine struct {
	Tasks chan *DeliveryTask
	Log   *slog.Logger
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
