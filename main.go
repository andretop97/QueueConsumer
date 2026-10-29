package main

import (
	"fmt"
	"log/slog"

	"github.com/andretop97/Queue_consumer_golang/src/consumer"
	"github.com/andretop97/Queue_consumer_golang/src/logger"
	"github.com/andretop97/Queue_consumer_golang/src/utils"
)

func getRabbitURLfromEnv() string {
	username := utils.GetEnv("RABBITMQ_DEFAULT_USER")
	password := utils.GetEnv("RABBITMQ_DEFAULT_PASS")
	hostname := utils.GetEnvOrDefault("RABBITMQ_DEFAULT_HOST", "localhost")
	port := utils.GetEnvOrDefault("RABBITMQ_DEFAULT_PORT", "5672")

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", username, password, hostname, port) 
	slog.Info(url)
	return url

}

func handler(Body string) error {
	fmt.Println(Body)
	return nil
}

func main() {
	utils.LoadEnv()
	logger.SetLoggerSettings()

	url := getRabbitURLfromEnv()
	serviceName := utils.GetEnvOrDefault("SERVICE_NAME", "exemple")

	consumer, err := consumer.NewRabbitMQConsumer(url, serviceName)
	defer consumer.StopConsumer()

	utils.FailOnError(err, "Failed to create consumer")
	consumer.Consume(handler)
}