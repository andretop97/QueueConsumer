package main

import (
	"fmt"
	"net"
	"net/url"

	Config "github.com/andretop97/Queue_consumer_golang/internal/config"
	CustomErrors "github.com/andretop97/Queue_consumer_golang/internal/custom_errors"
	Logger "github.com/andretop97/Queue_consumer_golang/internal/logger"
	"github.com/andretop97/Queue_consumer_golang/internal/rabbitmq"
)

func getRabbitURLfromEnv() string {
	username := Config.GetEnv("RABBITMQ_DEFAULT_USER")
	password := Config.GetEnv("RABBITMQ_DEFAULT_PASS")
	hostname := Config.GetEnvOrDefault("RABBITMQ_DEFAULT_HOST", "localhost")
	port := Config.GetEnvOrDefault("RABBITMQ_DEFAULT_PORT", "5672")

	u := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(username, password),
		Host:   net.JoinHostPort(hostname, port),
		Path:   "/",
	}
	return u.String()

}

func handler(Body string) error {
	fmt.Println(Body)
	return nil
}

func main() {
	Config.LoadEnv()
	Logger.SetLoggerSettings()

	url := getRabbitURLfromEnv()
	serviceName := Config.GetEnvOrDefault("SERVICE_NAME", "exemple")

	consumer, err := rabbitmq.NewRabbitMQConsumer(url, serviceName)
	defer consumer.StopConsumer()

	CustomErrors.FailOnError(err, "Failed to create consumer")
	consumer.Consume(handler)
}
