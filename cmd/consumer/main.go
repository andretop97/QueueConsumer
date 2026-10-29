package main

import (
	"fmt"
	"log/slog"
	"os"

	Config "github.com/andretop97/Queue_consumer_golang/internal/config"
	CustomErrors "github.com/andretop97/Queue_consumer_golang/internal/custom_errors"
	Logger "github.com/andretop97/Queue_consumer_golang/internal/logger"
	"github.com/andretop97/Queue_consumer_golang/internal/rabbitmq"
)

func handler(Body string) error {
	fmt.Println(Body)
	return nil
}

func main() {
	config, err := Config.Load()
	if err != nil {
		slog.Error("Erro ao carregar configurações", "error", err)
		os.Exit(1)
	}
	Logger.SetLoggerSettings()

	url := config.RabbitMQ.URL()

	consumer, err := rabbitmq.NewRabbitMQConsumer(url, config.ServiceName)
	CustomErrors.FailOnError(err, "Failed to create consumer")
	defer consumer.StopConsumer()

	consumer.Consume(handler)
}
