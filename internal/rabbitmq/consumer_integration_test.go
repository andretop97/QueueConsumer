//go:build integration

// Testes de integração contra o RabbitMQ do docker-compose.
// Rodar com: go test -tags=integration ./internal/rabbitmq -v
// Credenciais vêm do ambiente ou do .env na raiz do repositório; sem broker acessível, os testes são pulados.
package rabbitmq

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/andretop97/Queue_consumer_golang/internal/config"
	"github.com/andretop97/Queue_consumer_golang/internal/messaging"
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
// Passa logger nil de propósito: toda a suíte exercita o fallback para slog.DiscardHandler.
func newTestConsumer(t *testing.T, url string, admin *amqp.Channel) (*RabbitMQConsumer, topologyNames) {
	t.Helper()
	return newTestConsumerWithLogger(t, url, admin, nil)
}

func newTestConsumerWithLogger(t *testing.T, url string, admin *amqp.Channel, logger *slog.Logger) (*RabbitMQConsumer, topologyNames) {
	t.Helper()
	serviceName := fmt.Sprintf("it-%d-%d", time.Now().UnixNano(), serviceCounter.Add(1))
	names := namesFor(serviceName)

	consumer, err := NewRabbitMQConsumer(url, serviceName, logger)
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

// handlerFunc adapta uma função que só olha o corpo para messaging.Handler.
func handlerFunc(f func(body string) error) messaging.Handler {
	return messaging.HandlerFunc(func(_ context.Context, msg messaging.Message) error {
		return f(string(msg.Body))
	})
}

// consumeInBackground roda Consume numa goroutine e devolve o canal com o erro de retorno.
func consumeInBackground(ctx context.Context, consumer *RabbitMQConsumer, h messaging.Handler) <-chan error {
	done := make(chan error, 1)
	go func() { done <- consumer.Consume(ctx, h) }()
	return done
}

func waitReturn(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(waitTimeout):
		t.Fatalf("Consume não retornou em %s", waitTimeout)
		return nil
	}
}

func TestConsumeSendsFailedMessageToDLQ(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	consumeInBackground(t.Context(), consumer, handlerFunc(func(string) error { return errors.New("falha no handler") }))

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
	consumeInBackground(t.Context(), consumer, handlerFunc(func(body string) error {
		received <- body
		if body == "sentinela" {
			return errors.New("sentinela")
		}
		return nil
	}))

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

func TestNewRabbitMQConsumerConsumesFromMainQueue(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	if consumer.topology.Queue != names.queue {
		t.Errorf("topology.Queue = %q, esperado %q", consumer.topology.Queue, names.queue)
	}
}

func TestConsumePermanentErrorGoesToDLQ(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.StopConsumer()

	h := messaging.Chain(
		handlerFunc(func(string) error { panic("bug no handler") }),
		messaging.Recover(),
	)
	consumeInBackground(t.Context(), consumer, h)

	publish(t, admin, names.exchange, "causa-panic")

	if got := string(getEventually(t, admin, names.dlq).Body); got != "causa-panic" {
		t.Errorf("DLQ recebeu %q, esperado %q", got, "causa-panic")
	}
}

// waitForConsumer espera o broker registrar o consumidor na fila, para que o cancelamento do ctx
// aconteça depois do consumo ter começado (e não durante o ConsumeWithContext).
func waitForConsumer(t *testing.T, ch *amqp.Channel, queue string) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for time.Now().Before(deadline) {
		q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
		if err != nil {
			t.Fatalf("fila %s não existe: %v", queue, err)
		}
		if q.Consumers > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("nenhum consumidor registrado em %s em %s", queue, waitTimeout)
}

// stopWithTimeout chama StopConsumer sem deixar a suíte travar se ele não retornar.
func stopWithTimeout(t *testing.T, consumer *RabbitMQConsumer) {
	t.Helper()
	stopped := make(chan struct{})
	go func() {
		consumer.StopConsumer()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		consumer.connection.Close() // destrava o StopConsumer pendurado
		t.Errorf("StopConsumer não retornou em 3s")
	}
}

// Os testes de cancelamento encerram com connection.Close() em vez de StopConsumer, para não
// depender do bug isolado em TestStopConsumerRightAfterCancelDoesNotHang.

// Cancelar o ctx é o encerramento normal (Ctrl+C/SIGTERM): Consume retorna nil, não erro.
func TestConsumeReturnsNilWhenContextIsCanceled(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)

	t.Run("sem mensagem em andamento", func(t *testing.T) {
		consumer, names := newTestConsumer(t, url, admin)
		defer consumer.connection.Close()

		ctx, cancel := context.WithCancel(t.Context())
		done := consumeInBackground(ctx, consumer, handlerFunc(func(string) error { return nil }))
		waitForConsumer(t, admin, names.queue)
		cancel()

		if err := waitReturn(t, done); err != nil {
			t.Errorf("Consume() após cancelar o ctx = %v, esperado nil (encerramento normal)", err)
		}
	})

	// Cenário que expõe a corrida: o ctx é cancelado durante o handler e, enquanto ele termina,
	// a biblioteca fecha o canal de entregas; quando o laço volta ao select, os dois casos estão prontos.
	t.Run("cancelado durante o handler", func(t *testing.T) {
		consumer, names := newTestConsumer(t, url, admin)
		defer consumer.connection.Close()

		started, release := make(chan struct{}), make(chan struct{})
		ctx, cancel := context.WithCancel(t.Context())
		done := consumeInBackground(ctx, consumer, messaging.HandlerFunc(func(context.Context, messaging.Message) error {
			close(started)
			<-release
			return nil
		}))
		publish(t, admin, names.exchange, "x")
		<-started
		cancel()
		time.Sleep(300 * time.Millisecond) // dá tempo de a biblioteca fechar o canal de entregas
		close(release)

		if err := waitReturn(t, done); err != nil {
			t.Errorf("Consume() após cancelar o ctx = %v, esperado nil (encerramento normal)", err)
		}
	})
}

// Um SIGTERM durante a inicialização (antes do consumo começar) também é encerramento normal.
func TestConsumeReturnsNilWhenContextIsAlreadyCanceled(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, _ := newTestConsumer(t, url, admin)
	defer consumer.connection.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := consumer.Consume(ctx, handlerFunc(func(string) error { return nil })); err != nil {
		t.Errorf("Consume() com ctx já cancelado = %v, esperado nil", err)
	}
}

// No main, Consume retorna pelo cancelamento do ctx e o defer chama StopConsumer logo em seguida.
// Isso não pode travar, senão a aplicação nunca termina após um Ctrl+C.
func TestStopConsumerRightAfterCancelDoesNotHang(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)

	for i := range 10 {
		consumer, names := newTestConsumer(t, url, admin)
		started, release := make(chan struct{}), make(chan struct{})
		ctx, cancel := context.WithCancel(t.Context())
		done := consumeInBackground(ctx, consumer, messaging.HandlerFunc(func(context.Context, messaging.Message) error {
			close(started)
			<-release
			return nil
		}))
		publish(t, admin, names.exchange, "x")
		<-started
		cancel()
		close(release)
		waitReturn(t, done)

		stopWithTimeout(t, consumer)
		if t.Failed() {
			t.Fatalf("StopConsumer travou na execução %d", i)
		}
	}
}

// Um Ctrl+C durante o processamento não pode fazer a mensagem em andamento falhar e ir para a DLQ:
// o handler termina com um ctx não cancelado, a mensagem é confirmada e só então Consume retorna.
func TestConsumeFinishesInFlightMessageOnCancel(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, names := newTestConsumer(t, url, admin)
	defer consumer.connection.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	handlerCtxErr := make(chan error, 1)
	h := messaging.HandlerFunc(func(ctx context.Context, _ messaging.Message) error {
		close(started)
		<-release
		handlerCtxErr <- ctx.Err()
		return ctx.Err()
	})

	ctx, cancel := context.WithCancel(t.Context())
	done := consumeInBackground(ctx, consumer, h)
	publish(t, admin, names.exchange, "em-andamento")

	select {
	case <-started:
	case <-time.After(waitTimeout):
		t.Fatal("handler não começou a processar a mensagem")
	}
	cancel()
	close(release)

	if err := <-handlerCtxErr; err != nil {
		t.Errorf("ctx do handler foi cancelado (%v); a mensagem em andamento deveria terminar normalmente", err)
	}
	if err := waitReturn(t, done); err != nil {
		t.Errorf("Consume() = %v, esperado nil", err)
	}
	if n := readyMessages(t, admin, names.dlq); n != 0 {
		t.Errorf("DLQ tem %d mensagens; a mensagem em andamento não deveria ter ido para a DLQ", n)
	}
	consumer.connection.Close() // devolveria à fila uma mensagem sem Ack
	if n := readyMessages(t, admin, names.queue); n != 0 {
		t.Errorf("fila principal tem %d mensagens; a mensagem em andamento deveria ter sido confirmada", n)
	}
}

// Se o broker derruba a conexão, Consume precisa retornar erro em vez de ficar pendurado sem consumir.
func TestConsumeReturnsErrorWhenConnectionDrops(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)
	consumer, _ := newTestConsumer(t, url, admin)

	done := consumeInBackground(t.Context(), consumer, handlerFunc(func(string) error { return nil }))
	time.Sleep(200 * time.Millisecond) // garante que o consumo começou antes de derrubar a conexão

	consumer.connection.Close()

	if err := waitReturn(t, done); err == nil {
		t.Error("Consume() = nil após a conexão cair, esperado erro")
	}
}

// Os logs do consumer vão para o logger injetado (com component=rabbitmq), não para o slog global.
func TestConsumeLogsToInjectedLogger(t *testing.T) {
	url := brokerURL(t)
	admin := adminChannel(t, url)

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))
	consumer, names := newTestConsumerWithLogger(t, url, admin, logger)
	defer consumer.connection.Close()

	ctx, cancel := context.WithCancel(t.Context())
	done := consumeInBackground(ctx, consumer, handlerFunc(func(string) error { return nil }))
	waitForConsumer(t, admin, names.queue)
	cancel()
	waitReturn(t, done)

	for line := range bytes.Lines(buf.Bytes()) {
		var entry map[string]any
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("linha de log não é JSON: %q (%v)", line, err)
		}
		if entry["msg"] == "aguardando mensagens" {
			if entry["component"] != "rabbitmq" {
				t.Errorf("component = %v, esperado %q", entry["component"], "rabbitmq")
			}
			if entry["queue"] != names.queue {
				t.Errorf("queue = %v, esperado %q", entry["queue"], names.queue)
			}
			return
		}
	}
	t.Errorf("log %q não encontrado no logger injetado; saída:\n%s", "aguardando mensagens", buf.String())
}
