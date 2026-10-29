package messaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// ---------- Chain ----------

// recorder devolve um middleware que registra a entrada e a saída na ordem em que acontecem.
func recorder(name string, calls *[]string) Middleware {
	return func(next Handler) Handler {
		return HandlerFunc(func(ctx context.Context, msg Message) error {
			*calls = append(*calls, name+"-entra")
			err := next.Handle(ctx, msg)
			*calls = append(*calls, name+"-sai")
			return err
		})
	}
}

func TestChainAppliesFirstMiddlewareOutermost(t *testing.T) {
	var calls []string
	h := HandlerFunc(func(context.Context, Message) error {
		calls = append(calls, "handler")
		return nil
	})

	chained := Chain(h, recorder("a", &calls), recorder("b", &calls))
	if err := chained.Handle(t.Context(), Message{}); err != nil {
		t.Fatalf("Handle() = %v", err)
	}

	want := "a-entra b-entra handler b-sai a-sai"
	if got := strings.Join(calls, " "); got != want {
		t.Errorf("ordem = %q, esperado %q", got, want)
	}
}

func TestChainWithoutMiddlewaresCallsHandler(t *testing.T) {
	want := errors.New("erro do handler")
	h := HandlerFunc(func(context.Context, Message) error { return want })

	if err := Chain(h).Handle(t.Context(), Message{}); !errors.Is(err, want) {
		t.Errorf("Handle() = %v, esperado %v", err, want)
	}
}

// ---------- Recover ----------

func TestRecoverConvertsPanicIntoPermanentError(t *testing.T) {
	h := Recover()(HandlerFunc(func(context.Context, Message) error {
		panic("índice fora do intervalo")
	}))

	err := h.Handle(t.Context(), Message{})

	if err == nil {
		t.Fatal("Handle() = nil, esperado erro gerado a partir do panic")
	}
	if !errors.Is(err, ErrPanic) {
		t.Errorf("errors.Is(err, ErrPanic) = false, err = %v", err)
	}
	// Panic costuma ser bug: repetir não resolve, então vai direto para a DLQ.
	if !errors.Is(err, ErrPermanent) {
		t.Errorf("errors.Is(err, ErrPermanent) = false, err = %v", err)
	}
	if got := Decide(err, 0, 3); got != DeadLetter {
		t.Errorf("Decide(panic) = %v, esperado %v", got, DeadLetter)
	}
	if !strings.Contains(err.Error(), "índice fora do intervalo") {
		t.Errorf("erro deveria conter o valor do panic, obtido: %v", err)
	}
}

func TestRecoverPreservesPanickedError(t *testing.T) {
	original := errors.New("erro usado no panic")
	h := Recover()(HandlerFunc(func(context.Context, Message) error {
		panic(original)
	}))

	err := h.Handle(t.Context(), Message{})
	if !errors.Is(err, original) {
		t.Errorf("errors.Is(err, original) = false; panic(err) deveria preservar o erro original, err = %v", err)
	}
}

func TestRecoverDoesNotChangeNormalResults(t *testing.T) {
	ctx := t.Context()

	ok := Recover()(HandlerFunc(func(context.Context, Message) error { return nil }))
	if err := ok.Handle(ctx, Message{}); err != nil {
		t.Errorf("sucesso: Handle() = %v, esperado nil", err)
	}

	want := errors.New("falha comum")
	failing := Recover()(HandlerFunc(func(context.Context, Message) error { return want }))
	err := failing.Handle(ctx, Message{})
	if !errors.Is(err, want) {
		t.Errorf("falha: Handle() = %v, esperado %v", err, want)
	}
	if errors.Is(err, ErrPanic) || errors.Is(err, ErrPermanent) {
		t.Errorf("erro comum não deveria virar ErrPanic/ErrPermanent: %v", err)
	}
}

// ---------- Logging ----------

func newJSONLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// singleEntry exige exatamente uma linha de log e a devolve decodificada.
func singleEntry(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 || lines[0] == "" {
		t.Fatalf("esperada exatamente 1 linha de log, obtido %d:\n%s", len(lines), buf.String())
	}
	var entry map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatalf("log não é JSON válido: %v\n%s", err, lines[0])
	}
	return entry
}

func TestLoggingLogsSuccessAtInfo(t *testing.T) {
	logger, buf := newJSONLogger()
	h := Logging(logger)(HandlerFunc(func(context.Context, Message) error { return nil }))

	if err := h.Handle(t.Context(), Message{Body: []byte("12345"), Attempts: 2}); err != nil {
		t.Fatalf("Handle() = %v", err)
	}

	entry := singleEntry(t, buf)
	if entry["level"] != "INFO" {
		t.Errorf("level = %v, esperado INFO", entry["level"])
	}
	if entry["attempts"] != float64(2) {
		t.Errorf("attempts = %v, esperado 2", entry["attempts"])
	}
	if entry["body_bytes"] != float64(5) {
		t.Errorf("body_bytes = %v, esperado 5", entry["body_bytes"])
	}
	if _, ok := entry["duration"]; !ok {
		t.Errorf("log deveria ter o campo duration: %v", entry)
	}
	if _, ok := entry["error"]; ok {
		t.Errorf("log de sucesso não deveria ter o campo error: %v", entry)
	}
}

func TestLoggingLogsFailureAtErrorWithMessage(t *testing.T) {
	logger, buf := newJSONLogger()
	h := Logging(logger)(HandlerFunc(func(context.Context, Message) error {
		return errors.New("timeout no banco")
	}))

	h.Handle(t.Context(), Message{Attempts: 1})

	entry := singleEntry(t, buf)
	if entry["level"] != "ERROR" {
		t.Errorf("level = %v, esperado ERROR", entry["level"])
	}
	if entry["error"] != "timeout no banco" {
		t.Errorf("error = %v, esperado %q", entry["error"], "timeout no banco")
	}
	if entry["attempts"] != float64(1) {
		t.Errorf("attempts = %v, esperado 1", entry["attempts"])
	}
}

// O corpo pode conter dados pessoais e ser grande: o log registra só o tamanho.
func TestLoggingDoesNotLogBody(t *testing.T) {
	logger, buf := newJSONLogger()
	h := Logging(logger)(HandlerFunc(func(context.Context, Message) error { return nil }))

	h.Handle(t.Context(), Message{Body: []byte("cpf=123.456.789-00")})

	if strings.Contains(buf.String(), "123.456.789-00") {
		t.Errorf("o log não deveria conter o corpo da mensagem:\n%s", buf.String())
	}
}

func TestLoggingIsTransparent(t *testing.T) {
	logger, _ := newJSONLogger()
	want := errors.New("falha do handler")
	ctx := context.WithValue(t.Context(), ctxKey{}, "valor")
	msg := Message{Body: []byte("corpo"), Attempts: 3}

	var gotCtxValue any
	var gotMsg Message
	h := Logging(logger)(HandlerFunc(func(ctx context.Context, m Message) error {
		gotCtxValue = ctx.Value(ctxKey{})
		gotMsg = m
		return want
	}))

	if err := h.Handle(ctx, msg); !errors.Is(err, want) {
		t.Errorf("Handle() = %v, esperado o erro do handler sem alteração", err)
	}
	if gotCtxValue != "valor" {
		t.Errorf("ctx não foi repassado ao handler")
	}
	if string(gotMsg.Body) != "corpo" || gotMsg.Attempts != 3 {
		t.Errorf("mensagem repassada = %+v, esperado %+v", gotMsg, msg)
	}
}

func TestLoggingWithNilLoggerDoesNotPanic(t *testing.T) {
	h := Logging(nil)(HandlerFunc(func(context.Context, Message) error { return errors.New("falha") }))

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Logging(nil) entrou em pânico: %v", r)
		}
	}()
	h.Handle(t.Context(), Message{})
}

// ---------- Composição ----------

// Logging por fora de Recover: o panic convertido em erro também aparece no log.
func TestLoggingOutsideRecoverLogsPanics(t *testing.T) {
	logger, buf := newJSONLogger()
	h := Chain(
		HandlerFunc(func(context.Context, Message) error { panic("bug no handler") }),
		Logging(logger),
		Recover(),
	)

	err := h.Handle(t.Context(), Message{})

	if !errors.Is(err, ErrPanic) {
		t.Errorf("Handle() = %v, esperado ErrPanic", err)
	}
	entry := singleEntry(t, buf)
	if entry["level"] != "ERROR" || !strings.Contains(entry["error"].(string), "bug no handler") {
		t.Errorf("log do panic = %v, esperado level ERROR com o valor do panic", entry)
	}
}
