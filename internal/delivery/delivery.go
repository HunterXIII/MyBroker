package delivery

import (
	"context"
	"log/slog"
	"time"

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
		// packetID := sub.NextPacketID()

		// sub.InFlightMu.Lock()
		// sub.InFlight[packetID] = task.Msg.Offset
		// sub.InFlightMu.Unlock()
		// d.Log.Debug("created InFlight msg", "offset", task.Msg.Offset)

		msgToSend := *task.Msg
		// msgToSend.PacketID = packetID

		select {
		case sub.Messages <- &msgToSend:
			d.Log.Debug("Message dispatched to subscriber",
				"SubID", sub.ID,
				"Offset", task.Msg.Offset)
		default:
			d.Log.Warn("Subscriber buffer full, message dropped from memory",
				"SubID", sub.ID,
				"Offset", task.Msg.Offset)
		}
	}
}
