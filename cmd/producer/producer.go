package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/HunterXIII/MyBroker/pkg"
)

func main() {

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	cl := pkg.NewClient("localhost:1883", "producer-1", log)

	if err := cl.Connect(); err != nil {
		log.Error("Failed connect ro broker", "err", err)
		return
	}
	defer cl.Disconnect()
	log.Info("Connected to broker")

	for i := 10; i < 100; i++ {
		topic := "test/topic"
		payload := []byte(fmt.Sprintf("Hello #%d", i))
		if err := cl.Publish(topic, payload); err != nil {
			log.Error("Failed to publish message", "err", err)
		} else {
			log.Info("Published message", "topic", topic, "payload", string(payload))
		}
	}

}
