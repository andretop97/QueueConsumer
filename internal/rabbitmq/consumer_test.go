package rabbitmq

import "testing"

// Falhas ao conectar viram erro retornado (antes o FailOnError encerrava o processo).
func TestNewRabbitMQConsumerReturnsErrorWhenDialFails(t *testing.T) {
	tests := map[string]string{
		"esquema inválido":   "http://user:pass@localhost:5672/",
		"porta sem servidor": "amqp://user:pass@127.0.0.1:1/",
	}
	for name, url := range tests {
		t.Run(name, func(t *testing.T) {
			consumer, err := NewRabbitMQConsumer(url, "svc", nil)
			if err == nil {
				consumer.StopConsumer()
				t.Fatal("NewRabbitMQConsumer() sem erro, esperado falha ao conectar")
			}
			if consumer != nil {
				t.Errorf("NewRabbitMQConsumer() = %v, esperado nil junto com o erro", consumer)
			}
		})
	}
}
