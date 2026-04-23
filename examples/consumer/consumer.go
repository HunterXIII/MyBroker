package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/HunterXIII/MyBroker/pkg"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	cl := pkg.NewClient("localhost:1883", "consumer-1", log)

	if err := cl.Connect(); err != nil {
		log.Error("Failed to connect to broker", "err", err)
		return
	}
	defer cl.Disconnect()
	log.Info("Connected to broker")

	topic := "test/topic"
	err := cl.Subscribe(topic, func(t string, payload []byte) {
		log.Info("Received message", "topic", t, "payload", string(payload))
	})

	if err != nil {
		log.Error("Failed to subscribe", "topic", topic, "err", err)
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Info("Shutting down consumer")
}
