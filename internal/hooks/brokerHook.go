package hooks

import (
	"fmt"
	"log/slog"

	"github.com/HunterXIII/MyBroker/internal/broker"
	"github.com/HunterXIII/MyBroker/internal/metrics"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"
)

type BrokerHook struct {
	mqtt.HookBase
	Log    *slog.Logger
	Broker *broker.BrokerService
}

func (h *BrokerHook) ID() string {
	return "my-delivery-hook"
}

func (h *BrokerHook) Provides(b byte) bool {
	return b == mqtt.OnConnect ||
		b == mqtt.OnPacketRead ||
		b == mqtt.OnSubscribe ||
		b == mqtt.OnUnsubscribe ||
		b == mqtt.OnDisconnect
}

func (h *BrokerHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {

	h.Log.Info("[CONNECT]", "ClientID", cl.ID)
	h.Broker.AddSubscriber(cl)
	metrics.ActiveSubscribers.Inc()
	return nil
}

func (h *BrokerHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {

	topics := []string{}

	for _, filter := range pk.Filters {
		topics = append(topics, filter.Filter)
		h.Log.Info("[SUBSCRIBE]", "ClientID", cl.ID, "Topic", filter.Filter)
		h.Broker.Subscribe(cl.ID, filter.Filter)
		metrics.TopicSubscribers.WithLabelValues(filter.Filter).Inc()
	}

	pk.Filters = []packets.Subscription{}

	return pk
}

func (h *BrokerHook) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {

	topics := []string{}

	for _, filter := range pk.Filters {
		topics = append(topics, filter.Filter)
		h.Log.Info("[UNSUBSCRIBE]", "ClientID", cl.ID, "Topic", filter.Filter)
		h.Broker.Unsubscribe(cl.ID, filter.Filter)
		metrics.TopicSubscribers.WithLabelValues(filter.Filter).Dec()
	}

	return pk
}
func (h *BrokerHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	h.Log.Info("[DISCONNECT]", "ClientID", cl.ID, "err", err, "expire", expire)
	topics := h.Broker.Storage.GetTopicsByClient(cl.ID)
	for _, topic := range topics {
		h.Broker.Unsubscribe(cl.ID, topic)
		metrics.TopicSubscribers.WithLabelValues(topic).Dec()
	}
	h.Broker.RemoveSubsciber(cl.ID)
	metrics.ActiveSubscribers.Dec()
}

func (h *BrokerHook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {

	switch pk.FixedHeader.Type {
	case packets.Publish:
		h.Log.Info("[PUBLISH]", "ClientID", cl.ID, "Topic", pk.TopicName, "Payload", fmt.Sprintf("%s", pk.Payload))
		err := h.Broker.NewMessage(pk.TopicName, pk.Payload)
		if err != nil {
			h.Log.Error("Don't push new message", "err", err)
		}
		metrics.MessagesPublishedTotal.WithLabelValues(pk.TopicName).Inc()
	case packets.Puback:
		h.Log.Info("[PUBACK]", "ClientID", cl.ID, "PacketID", pk.PacketID, "ReasonCode", pk.ReasonCode)

		sub := h.Broker.GetSubscriber(cl.ID)
		if sub == nil {
			return pk, nil
		}

		sub.InFlightMu.Lock()
		offset, exists := sub.InFlight[pk.PacketID]
		if exists {
			delete(sub.InFlight, pk.PacketID)
		}
		sub.InFlightMu.Unlock()
		if !exists {
			return pk, nil
		}

		if pk.ReasonCode >= 0x80 {
			h.Log.Warn("Consumer failed to process message, moving to DLQ",
				"ClientID", cl.ID, "Offset", offset)

			err := h.Broker.Storage.MoveToDLQ(cl.ID, offset)
			if err != nil {
				h.Log.Error("Failed to move message to DLQ", "err", err)
				return pk, nil
			}

			h.Broker.Storage.MarkAsDelivered(cl.ID, offset)
			return pk, nil
		}

		h.Log.Info("Consumer processed message", "ClientID", cl.ID, "Offset", offset)

		h.Broker.Storage.MarkAsDelivered(cl.ID, offset)
		metrics.MessagesDeliveredTotal.WithLabelValues(cl.ID).Inc()

		lag := h.Broker.Storage.GetConsumerLag(cl.ID)
		metrics.ConsumerLag.WithLabelValues(cl.ID).Set(float64(lag))
	}

	return pk, nil
}
