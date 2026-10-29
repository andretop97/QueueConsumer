package main

import "testing"

// A montagem da URL do RabbitMQ foi para config.RabbitMQ.URL() e é testada em internal/config.

func TestHandlerReturnsNil(t *testing.T) {
	if err := handler("qualquer mensagem"); err != nil {
		t.Errorf("handler() = %v, esperado nil", err)
	}
}
