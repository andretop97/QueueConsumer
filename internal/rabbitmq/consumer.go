package rabbitmq

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/andretop97/Queue_consumer_golang/internal/messaging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const maxRetries = 0

var _ messaging.Consumer = (*RabbitMQConsumer)(nil)

type RabbitMQConsumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	topology   Topology
	logger     *slog.Logger
}

func (r *RabbitMQConsumer) createConnection(url string) error {
	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("criando conexão: %w", err)
	}
	r.connection = conn
	return nil
}

func (r *RabbitMQConsumer) createChannel() error {
	channel, err := r.connection.Channel()
	if err != nil {
		return fmt.Errorf("criando canal: %w", err)
	}
	r.channel = channel
	return nil

}

func NewRabbitMQConsumer(url string, serviceName string, logger *slog.Logger) (*RabbitMQConsumer, error) {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	consumer := &RabbitMQConsumer{logger: logger.With("component", "rabbitmq")}
	err := consumer.createConnection(url)
	if err != nil {
		return nil, err
	}
	err = consumer.createChannel()
	if err != nil {
		consumer.connection.Close()
		return nil, err
	}
	consumer.topology = NewTopology(serviceName)
	if err := consumer.topology.Declare(consumer.channel); err != nil {
		consumer.channel.Close()
		consumer.connection.Close()
		return nil, fmt.Errorf("declarando topologia: %w", err)
	}
	return consumer, nil
}

func (r *RabbitMQConsumer) Consume(ctx context.Context, handler messaging.Handler) error {
	deliveries, err := r.channel.ConsumeWithContext(
		ctx, r.topology.Queue, "", false, false, false, false, nil,
	)

	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("iniciando consumo de %q: %w", r.topology.Queue, err)
	}

	r.logger.Info("aguardando mensagens", "queue", r.topology.Queue)
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("canal de entregas de %q foi fechado", r.topology.Queue)
			}

			r.handle(ctx, handler, d)

		}
	}

}

func (r *RabbitMQConsumer) handle(ctx context.Context, h messaging.Handler, d amqp.Delivery) {
	msg := messaging.Message{
		Body:     d.Body,
		Headers:  d.Headers,
		Attempts: 0,
	}

	err := h.Handle(context.WithoutCancel(ctx), msg)

	action := messaging.Decide(err, msg.Attempts, maxRetries)

	var ackErr error
	switch action {
	case messaging.Ack:
		ackErr = d.Ack(false)
	default:
		ackErr = d.Nack(false, false)
	}

	if ackErr != nil {
		r.logger.Error("falha ao confirmar mensagem", "action", action, "error", ackErr)
	}

}

func (r *RabbitMQConsumer) StopConsumer() error {
	return r.connection.Close()
}
