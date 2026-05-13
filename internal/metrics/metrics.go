package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Показывает сколько сообщений клиент еще не подтвердил из лога
	ConsumerLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_consumer_lag",
			Help: "Разница между максимальным офсетом и подтвержденным офсетом клиента",
		},
		[]string{"client_id"},
	)

	// Счетчик всех входящих сообщений (Total Throughput)
	MessagesPublishedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_messages_published_total",
			Help: "Общее количество опубликованных сообщений в разрезе топиков",
		},
		[]string{"topic"},
	)

	// Счетчик успешных обработок
	MessagesDeliveredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_messages_delivered_total",
			Help: "Количество сообщений, успешно подтвержденных клиентами (PUBACK)",
		},
		[]string{"client_id"},
	)

	// Текущее количество активных подписчиков
	ActiveSubscribers = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broker_active_subscribers",
		Help: "The current number of active subscribers",
	})

	// Размер очереди в DeliveryEngine
	DeliveryQueueLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "broker_delivery_queue_length",
		Help: "Current number of tasks in delivery channel",
	})

	// Ошибки доставки
	MsgInDLQ = promauto.NewCounter(prometheus.CounterOpts{
		Name: "broker_msg_in_dlq",
		Help: "The total number of dlq",
	})
)
