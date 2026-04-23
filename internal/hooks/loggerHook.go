package hooks

import (
	"log"

	mqtt "github.com/mochi-mqtt/server/v2"

	"github.com/mochi-mqtt/server/v2/packets"
)

// LoggerHook логирует все события клиентов
type LoggerHook struct {
	mqtt.HookBase
}

func (l *LoggerHook) ID() string {
	return "logger-hook"
}

func (l *LoggerHook) Provides(b byte) bool {
	return b == mqtt.OnConnect ||
		b == mqtt.OnDisconnect ||
		b == mqtt.OnSubscribe ||
		b == mqtt.OnPublish
}

// Подключение клиента
func (l *LoggerHook) OnConnect(cl *mqtt.Client, pk packets.Packet) error {
	log.Printf("[CONNECT] ClientID=%s", cl.ID)
	return nil
}

// Отключение клиента
func (l *LoggerHook) OnDisconnect(cl *mqtt.Client, err error, expire bool) {
	log.Printf("[DISCONNECT] ClientID=%s err=%v expire=%v", cl.ID, err, expire)
}

// Подписка
func (l *LoggerHook) OnSubscribe(cl *mqtt.Client, pk packets.Packet) packets.Packet {
	for _, filter := range pk.Filters {
		log.Printf("[SUBSCRIBE] ClientID=%s Topic=%s QoS=%d", cl.ID, filter.Filter, filter.Qos)
	}
	return pk
}

func (l *LoggerHook) OnPublish(cl *mqtt.Client, pk packets.Packet) (packets.Packet, error) {
	log.Printf("[PUBLISH] ClientID=%s Topic=%s Payload=%s", cl.ID, pk.TopicName, string(pk.Payload))
	return pk, nil
}
