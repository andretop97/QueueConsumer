package rabbitmq

import (
	"fmt"
	"log/slog"

	CustomErrors "github.com/andretop97/Queue_consumer_golang/internal/custom_errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQConsumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	topology   Topology
}

func (r *RabbitMQConsumer) createConnection(url string) {
	conn, err := amqp.Dial(url)
	CustomErrors.FailOnError(err, "Failed to initialize connection")
	r.connection = conn
}

func (r *RabbitMQConsumer) createChannel() {
	channel, err := r.connection.Channel()
	CustomErrors.FailOnError(err, "Failed to create channel")
	r.channel = channel
}

func NewRabbitMQConsumer(url string, serviceName string) (*RabbitMQConsumer, error) {
	consumer := new(RabbitMQConsumer)
	consumer.createConnection(url)
	consumer.createChannel()
	consumer.topology = NewTopology(serviceName)
	if err := consumer.topology.Declare(consumer.channel); err != nil {
		consumer.channel.Close()
		consumer.connection.Close()
		return nil, fmt.Errorf("declarando topologia: %w", err)
	}
	return consumer, nil
}

func (r *RabbitMQConsumer) getConsumer(queueName string) <-chan amqp.Delivery {
	delivery, err := r.channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	CustomErrors.FailOnError(err, "Failed to create consumer")
	return delivery
}

func (r *RabbitMQConsumer) Consume(handler func(string) error) {
	delivery := r.getConsumer(r.topology.Queue)

	var forever chan struct{}

	go func() {
		for message := range delivery {
			body := string(message.Body)
			slog.Info("Mensagem recebida", "msg", body)
			err := handler(body)

			if err != nil {
				slog.Error("Erro ao consumir mensagem", "error", err)
				message.Nack(false, false)
				// message.Reject(true)
			} else {
				message.Ack(false)
			}
		}
	}()

	slog.Info(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever

}

func (r *RabbitMQConsumer) StopConsumer() {
	err := r.channel.Close()
	if err != nil {
		slog.Error("Filed to close channel")
	}
	err = r.connection.Close()
	if err != nil {
		slog.Error("Filed to close connection")
	}
}
