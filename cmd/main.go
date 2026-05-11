package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HunterXIII/MyBroker/internal/broker"
	"github.com/HunterXIII/MyBroker/internal/delivery"
	"github.com/HunterXIII/MyBroker/internal/hooks"
	"github.com/HunterXIII/MyBroker/internal/storage"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

func main() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	slog.SetDefault(logger)

	options := &mqtt.Options{
		Logger: logger,
	}

	server := mqtt.New(options)

	storageSvc, err := storage.NewStorageService(logger, "data")
	if err != nil {
		logger.Error("Storage failed", "err", err)
	}

	deliverySvc := delivery.NewDeliveryEngine(logger, storageSvc)
	brokerSvc := broker.NewBrokerService(server, storageSvc, deliverySvc, logger, &broker.BrokerConfig{
		TTL:             180,
		MaxQueueSize:    1024,
		IntervalCleanup: 10 * time.Minute,
	})

	brokerHook := &hooks.BrokerHook{
		Log:    logger,
		Broker: brokerSvc,
	}

	_ = server.AddHook(new(auth.AllowHook), nil)
	_ = server.AddHook(brokerHook, nil)

	tcp := listeners.NewTCP(listeners.Config{
		ID:      "t1",
		Address: ":1883",
	})

	err = server.AddListener(tcp)
	if err != nil {
		logger.Error("Server is not started to listen", "err", err)
	}

	go func() {
		if err := brokerSvc.Start(); err != nil {
			logger.Error("Server is not started", "err", err)
		}
	}()

	<-done
	server.Log.Warn("caught signal, stopping...")
	_ = server.Close()
	server.Log.Info("main.go finished")
	brokerSvc.Stop()
}
