package rabbitmq

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Topology struct {
	Exchange string
	Queue    string
	DLX      string
	DLQ      string
}

func NewTopology(serviceName string) Topology {
	return Topology{
		Exchange: serviceName + "-exchange",
		Queue:    serviceName + "-queue",
		DLX:      serviceName + "-dlx",
		DLQ:      serviceName + "-dlq",
	}
}

func (t Topology) Declare(ch *amqp.Channel) error {
	if err := createExchangeWithQueue(ch, t.DLX, t.DLQ, nil); err != nil {
		return fmt.Errorf("declarando DLQ: %w", err)
	}

	args := amqp.Table{
		"x-dead-letter-exchange": t.DLX,
	}
	if err := createExchangeWithQueue(ch, t.Exchange, t.Queue, args); err != nil {
		return fmt.Errorf("declarando fila principal: %w", err)
	}
	return nil
}

func createExchangeWithQueue(ch *amqp.Channel, exchangeName string, queueName string, args amqp.Table) error {
	err := ch.ExchangeDeclare(
		exchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("declarando exchange %q: %w", exchangeName, err)
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		return fmt.Errorf("declarando fila %q: %w", queueName, err)
	}

	err = ch.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("declarando bind entre fila %q e exchange %q: %w", queueName, exchangeName, err)
	}

	return nil
}
