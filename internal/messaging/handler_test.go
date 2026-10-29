package messaging

import (
	"context"
	"errors"
	"testing"
)

// Verificação em tempo de compilação: HandlerFunc satisfaz Handler.
var _ Handler = HandlerFunc(nil)

type ctxKey struct{}

func TestHandlerFuncForwardsContextAndMessage(t *testing.T) {
	ctx := context.WithValue(t.Context(), ctxKey{}, "valor-do-ctx")
	msg := Message{
		Body:     []byte("corpo"),
		Headers:  map[string]any{"x-teste": "sim"},
		Attempts: 2,
	}

	var gotCtxValue any
	var gotMsg Message
	var h Handler = HandlerFunc(func(ctx context.Context, m Message) error {
		gotCtxValue = ctx.Value(ctxKey{})
		gotMsg = m
		return nil
	})

	if err := h.Handle(ctx, msg); err != nil {
		t.Fatalf("Handle() = %v, esperado nil", err)
	}
	if gotCtxValue != "valor-do-ctx" {
		t.Errorf("ctx não foi repassado: valor = %v", gotCtxValue)
	}
	if string(gotMsg.Body) != "corpo" || gotMsg.Headers["x-teste"] != "sim" || gotMsg.Attempts != 2 {
		t.Errorf("mensagem repassada = %+v, esperado %+v", gotMsg, msg)
	}
}

func TestHandlerFuncReturnsFunctionError(t *testing.T) {
	want := errors.New("falha do handler")
	h := HandlerFunc(func(context.Context, Message) error { return want })

	if err := h.Handle(t.Context(), Message{}); !errors.Is(err, want) {
		t.Errorf("Handle() = %v, esperado %v", err, want)
	}
}

func TestHandlerFuncResultFeedsDecide(t *testing.T) {
	ctx := t.Context()
	ok := HandlerFunc(func(context.Context, Message) error { return nil })
	permanent := HandlerFunc(func(context.Context, Message) error {
		return errors.Join(errors.New("payload inválido"), ErrPermanent)
	})

	if got := Decide(ok.Handle(ctx, Message{}), 0, 3); got != Ack {
		t.Errorf("handler com sucesso: Decide = %v, esperado %v", got, Ack)
	}
	if got := Decide(permanent.Handle(ctx, Message{}), 0, 3); got != DeadLetter {
		t.Errorf("handler com erro permanente: Decide = %v, esperado %v", got, DeadLetter)
	}
}
