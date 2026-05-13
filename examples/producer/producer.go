package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/HunterXIII/MyBroker/pkg"
)

func main() {

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cl := pkg.NewClient("localhost:1883", "producer-1", log)

	if err := cl.Connect(); err != nil {
		log.Error("Failed connect ro broker", "err", err)
		return
	}
	defer cl.Disconnect()
	log.Info("Connected to broker")

	topic := "test/topic"

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	i := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			payload := []byte(fmt.Sprintf("Hello, world! #%d", i))
			if err := cl.Publish(topic, payload); err != nil {
				log.Error("Failed to publish message", "err", err)
			} else {
				log.Info("Published message", "topic", topic, "payload", string(payload))
			}
			i++
		}
	}

}
