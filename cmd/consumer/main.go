package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/andretop97/Queue_consumer_golang/internal/config"
	"github.com/andretop97/Queue_consumer_golang/internal/messaging"
	"github.com/andretop97/Queue_consumer_golang/internal/rabbitmq"
)

func main() {
	if err := run(); err != nil {
		slog.Error("encerrando com erro", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("carregando configuração: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer, err := rabbitmq.NewRabbitMQConsumer(cfg.RabbitMQ.URL(), cfg.ServiceName, logger)
	if err != nil {
		return err
	}
	defer consumer.StopConsumer()

	h := messaging.Chain(messaging.HandlerFunc(handle), messaging.Logging(logger), messaging.Recover())
	return consumer.Consume(ctx, h)
}

func handle(_ context.Context, msg messaging.Message) error {
	fmt.Println(string(msg.Body))
	return nil
}
