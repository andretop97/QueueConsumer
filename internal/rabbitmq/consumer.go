package rabbitmq

import (
	"fmt"
	"log/slog"

	CustomErrors "github.com/andretop97/Queue_consumer_golang/internal/custom_errors"
	amqp "github.com/rabbitmq/amqp091-go"
)

type queueConfig struct {
	dlxName *string
	msgTTL  *int64
}

type RabbitMQConsumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queueName  string
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

func (r *RabbitMQConsumer) createExchange(exchangeName string) {
	err := r.channel.ExchangeDeclare(
		exchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	CustomErrors.FailOnError(err, "Failed to declare Exchange")
}

func (r *RabbitMQConsumer) createQueue(queueName string, queueConfig queueConfig) {
	args := amqp.Table{}

	if queueConfig.dlxName != nil {
		args["x-dead-letter-exchange"] = *queueConfig.dlxName
	}

	if queueConfig.msgTTL != nil {
		args["x-message-ttl"] = *queueConfig.msgTTL
	}

	_, err := r.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		args,
	)

	CustomErrors.FailOnError(err, "Failed to declare queue")
}

func (r *RabbitMQConsumer) createBind(exchangeName string, queueName string) {
	err := r.channel.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil,
	)

	CustomErrors.FailOnError(err, "Failed to create bind between exchange and queue")
}

func (r *RabbitMQConsumer) createExchangeAndQueueWithBind(exchangeName string, queueName string, queueConfig queueConfig) {
	r.createExchange(exchangeName)
	slog.Info(fmt.Sprintf("Exchange %s created successfuly", exchangeName))
	r.createQueue(queueName, queueConfig)
	slog.Info(fmt.Sprintf("Queue %s created successfuly", queueName))
	r.createBind(exchangeName, queueName)
	slog.Info("Bind created successfuly")

}

func (r *RabbitMQConsumer) createSimpleQueue(serviceName string) {
	exchangeName := fmt.Sprintf("%s-exchange", serviceName)
	queueName := fmt.Sprintf("%s-queue", serviceName)
	r.queueName = queueName
	r.createExchangeAndQueueWithBind(exchangeName, queueName, queueConfig{})

}

func (r *RabbitMQConsumer) createQueueWithDlq(serviceName string) {
	exchangeName := fmt.Sprintf("%s-exchange", serviceName)
	queueName := fmt.Sprintf("%s-queue", serviceName)
	r.queueName = queueName

	dlxName := fmt.Sprintf("%s-dlx", serviceName)
	dlqName := fmt.Sprintf("%s-dlq", serviceName)

	qConfig := queueConfig{}
	qConfig.dlxName = &dlxName

	r.createExchangeAndQueueWithBind(dlxName, dlqName, queueConfig{})

	r.createExchangeAndQueueWithBind(exchangeName, queueName, qConfig)

}

func NewRabbitMQConsumer(url string, serviceName string) (*RabbitMQConsumer, error) {
	consumer := new(RabbitMQConsumer)
	consumer.createConnection(url)
	consumer.createChannel()

	consumer.createQueueWithDlq(serviceName)

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
	delivery := r.getConsumer(r.queueName)

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
