package hooks

import (
	"log/slog"

	"github.com/HunterXIII/MyBroker/internal/broker"
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

	return nil
}

func (h *BrokerHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {

	topics := []string{}

	for _, filter := range pk.Filters {
		topics = append(topics, filter.Filter)
		h.Log.Info("[SUBSCRIBE]", "ClientID", cl.ID, "Topic", filter.Filter)
		h.Broker.Subscribe(cl.ID, filter.Filter)
	}

	return pk
}

func (h *BrokerHook) OnUnsubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {

	topics := []string{}

	for _, filter := range pk.Filters {
		topics = append(topics, filter.Filter)
		h.Log.Info("[UNSUBSCRIBE]", "ClientID", cl.ID, "Topic", filter.Filter)
		h.Broker.Unsubscribe(cl.ID, filter.Filter)
	}

	return pk
}
func (h *BrokerHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	h.Log.Info("[DISCONNECT]", "ClientID", cl.ID, "err", err, "expire", expire)
	h.Broker.RemoveSubsciber(cl.ID)
}

func (h *BrokerHook) OnPacketRead(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	if pk.FixedHeader.Type == packets.Publish {
		h.Log.Info("[PUBLISH]", "ClientID", cl.ID, "Topic", pk.TopicName, "Payload", pk.Payload)
		err := h.Broker.NewMessage(pk.TopicName, pk.Payload)
		if err != nil {
			h.Log.Error("Don't push new message", "err", err)
		}
	}
	return pk, nil
}
