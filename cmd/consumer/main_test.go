package main

import (
	"testing"

	"github.com/andretop97/Queue_consumer_golang/internal/messaging"
)

// A montagem da URL do RabbitMQ foi para config.RabbitMQ.URL() e é testada em internal/config.

func TestHandleReturnsNil(t *testing.T) {
	if err := handle(t.Context(), messaging.Message{Body: []byte("qualquer mensagem")}); err != nil {
		t.Errorf("handle() = %v, esperado nil", err)
	}
}
