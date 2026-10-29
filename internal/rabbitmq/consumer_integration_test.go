//go:build integration

// Testes de integração contra o RabbitMQ do docker-compose.
// Rodar com: go test -tags=integration ./internal/rabbitmq -v
// Credenciais vêm do ambiente ou do .env na raiz do repositório; sem broker acessível, os testes são pulados.
package rabbitmq

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/andretop97/Queue_consumer_golang/internal/config"
)

const waitTimeout = 10 * time.Second

var serviceCounter atomic.Int64

type topologyNames struct {
	exchange, queue, dlx, dlq string
}

func namesFor(serviceName string) topologyNames {
	return topologyNames{
		exchange: serviceName + "-exchange",
		queue:    serviceName + "-queue",
		dlx:      serviceName + "-dlx",
		dlq:      serviceName + "-dlq",
	}
}

// brokerURL obtém a URL pelo mesmo config.Load() da aplicação e pula o teste se a config estiver incompleta
// ou o broker não responder. config.Load() procura o .env no diretório atual (internal/rabbitmq), por isso
// o .env da raiz é carregado antes.
func brokerURL(t *testing.T) string {
	t.Helper()
	_ = godotenv.Load("../../.env")

	cfg, err := config.Load()
	if err != nil {
		t.Skipf("configuração do RabbitMQ incompleta: %v", err)
	}
	url := cfg.RabbitMQ.URL()

	conn, err := amqp.Dial(url)
	if err != nil {
		t.Skipf("RabbitMQ inacessível em %s:%s (%v); suba com docker-compose up -d", cfg.RabbitMQ.Host, cfg.RabbitMQ.Port, err)
	}
	conn.Close()
	return url
}

// adminChannel abre uma conexão separada da do consumer, usada para publicar, inspecionar e limpar.
func adminChannel(t *testing.T, url string) *amqp.Channel {
	t.Helper()
	conn, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("falha ao conectar: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("falha ao abrir canal: %v", err)
	}
	return ch
}

// newTestConsumer cria um consumer com nome de serviço único e remove a topologia ao final do teste.
func newTestConsumer(t *testing.T, url string, admin *amqp.Channel) (*RabbitMQConsumer, topologyNames) {
	t.Helper()
	serviceName := fmt.Sprintf("it-%d-%d", time.Now().UnixNano(), serviceCounter.Add(1))
	names := namesFor(serviceName)

	consumer, err := NewRabbitMQConsumer(url, serviceName)
	if err != nil {
		t.Fatalf("NewRabbitMQConsumer() erro = %v", err)
	}

	t.Cleanup(func() {
		admin.QueueDelete(names.queue, false, false, false)
		admin.QueueDelete(names.dlq, false, false, false)
		admin.ExchangeDelete(names.exchange, false, false)
		admin.ExchangeDelete(names.dlx, false, false)
	})
	return consumer, names
}

func publish(t *testing.T, ch *amqp.Channel, exchange, body string) {
	t.Helper()
	err := ch.Publish(exchange, "", false, false, amqp.Publishing{Body: []byte(body)})
	if err != nil {
		t.Fatalf("falha ao publicar em %s: %v", exchange, err)
	}
}

// getEventually faz polling com basic.get até receber uma mensagem da fila ou estourar o timeout.
func getEventually(t *testing.T, ch *amqp.Channel, queue string) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			t.Fatalf("falha no Get de %s: %v", queue, err)
		}
		if ok {
			return msg
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nenhuma mensagem chegou em %s em %s", queue, waitTimeout)
	return amqp.Delivery{}
}

func readyMessages(t *testing.T, ch *amqp.Channel, queue string) int {
	t.Helper()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		t.Fatalf("fila %s não existe: %v", queue, err)
	}
	return q.Messages
}

func TestNewRabbitMQConsumerDeclaresTopology(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	for _, exchange := range []string{names.exchange, names.dlx} {
		if err := admin.ExchangeDeclarePassive(exchange, "fanout", true, false, false, false, nil); err != nil {
			t.Fatalf("exchange %s não declarada como fanout durável: %v", exchange, err)
		}
	}
	for _, queue := range []string{names.queue, names.dlq} {
		readyMessages(t, admin, queue)
	}
}

func TestMainQueueIsConfiguredWithDeadLetterExchange(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	// Redeclarar com argumentos idênticos só é aceito pelo broker se os argumentos existentes forem iguais.
	_, err := admin.QueueDeclare(names.queue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": names.dlx})
	if err != nil {
		t.Fatalf("fila %s não tem x-dead-letter-exchange=%s: %v", names.queue, names.dlx, err)
	}

	// Contraprova: sem o argumento, o broker recusa com PRECONDITION_FAILED (o erro fecha o canal, por isso um canal novo).
	probe := adminChannel(t, url)
	_, err = probe.QueueDeclare(names.queue, true, false, false, false, nil)
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.PreconditionFailed {
		t.Fatalf("esperado PRECONDITION_FAILED ao redeclarar sem DLX, obtido: %v", err)
	}
}

func TestExchangesAreBoundToTheirQueues(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	publish(t, admin, names.exchange, "para-fila-principal")
	if got := string(getEventually(t, admin, names.queue).Body); got != "para-fila-principal" {
		t.Errorf("fila principal recebeu %q", got)
	}

	publish(t, admin, names.dlx, "para-dlq")
	if got := string(getEventually(t, admin, names.dlq).Body); got != "para-dlq" {
		t.Errorf("DLQ recebeu %q", got)
	}
}

func TestConsumeSendsFailedMessageToDLQ(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	go consumer.Consume(func(string) error { return errors.New("falha no handler") })

	publish(t, admin, names.exchange, "vai-falhar")

	msg := getEventually(t, admin, names.dlq)
	if got := string(msg.Body); got != "vai-falhar" {
		t.Errorf("DLQ recebeu %q, esperado %q", got, "vai-falhar")
	}
	if deaths, ok := msg.Headers["x-death"].([]interface{}); !ok || len(deaths) == 0 {
		t.Errorf("mensagem na DLQ deveria ter header x-death, headers = %v", msg.Headers)
	}
}

func TestConsumeAcksSuccessfulMessage(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)

	// O loop do Consume é sequencial no mesmo canal: quando a sentinela (que falha) chega à DLQ,
	// o Ack da mensagem anterior já foi processado pelo broker. Isso evita depender de sleep.
	received := make(chan string, 2)
	go consumer.Consume(func(body string) error {
		received <- body
		if body == "sentinela" {
			return errors.New("sentinela")
		}
		return nil
	})

	publish(t, admin, names.exchange, "sucesso")
	publish(t, admin, names.exchange, "sentinela")

	if got := string(getEventually(t, admin, names.dlq).Body); got != "sentinela" {
		t.Fatalf("esperado que só a sentinela chegasse à DLQ, recebeu %q", got)
	}
	if got := <-received; got != "sucesso" {
		t.Errorf("handler recebeu %q primeiro, esperado %q", got, "sucesso")
	}

	// Fechar o consumer devolveria à fila qualquer mensagem sem Ack.
	consumer.StopConsumer()

	if n := readyMessages(t, admin, names.queue); n != 0 {
		t.Errorf("fila principal tem %d mensagens após StopConsumer, esperado 0 (mensagem não foi confirmada)", n)
	}
	if n := readyMessages(t, admin, names.dlq); n != 0 {
		t.Errorf("DLQ tem %d mensagens extras, esperado 0", n)
	}
}

func TestNewRabbitMQConsumerSetsQueueName(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	if consumer.queueName != names.queue {
		t.Errorf("queueName = %q, esperado %q", consumer.queueName, names.queue)
	}
}
