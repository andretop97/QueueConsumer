package messaging

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"
)

func TestDecide(t *testing.T) {
	transient := errors.New("timeout no banco")

	tests := []struct {
		name       string
		err        error
		attempts   int
		maxRetries int
		want       Action
	}{
		{"sucesso confirma", nil, 0, 0, Ack},
		{"sucesso confirma mesmo após falhas anteriores", nil, 5, 3, Ack},

		{"erro sem retry configurado vai para a DLQ (comportamento atual)", transient, 0, 0, DeadLetter},
		{"primeira falha com retries disponíveis tenta de novo", transient, 0, 3, Retry},
		{"última tentativa disponível ainda tenta de novo", transient, 2, 3, Retry},
		{"retries esgotados vão para a DLQ", transient, 3, 3, DeadLetter},
		{"tentativas acima do limite vão para a DLQ", transient, 4, 3, DeadLetter},
		{"maxRetries negativo se comporta como zero", transient, 0, -1, DeadLetter},

		{"ErrPermanent vai direto para a DLQ", ErrPermanent, 0, 3, DeadLetter},
		{"ErrPermanent embrulhado com %w vai direto para a DLQ", fmt.Errorf("payload inválido: %w", ErrPermanent), 0, 3, DeadLetter},
		{"ErrPermanent dentro de errors.Join vai direto para a DLQ", errors.Join(transient, ErrPermanent), 0, 3, DeadLetter},
		{"erro que só cita 'permanent' no texto não é permanente", errors.New("permanent"), 0, 3, Retry},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Decide(tt.err, tt.attempts, tt.maxRetries); got != tt.want {
				t.Errorf("Decide(%v, %d, %d) = %v, esperado %v", tt.err, tt.attempts, tt.maxRetries, got, tt.want)
			}
		})
	}
}

// Fixa a semântica de Attempts (falhas anteriores, começando em 0): com maxRetries = N,
// uma mensagem que sempre falha é executada N+1 vezes antes de ir para a DLQ.
func TestDecideTotalExecutionsBeforeDeadLetter(t *testing.T) {
	failure := errors.New("sempre falha")

	for _, maxRetries := range []int{0, 1, 3, 10} {
		t.Run(fmt.Sprintf("maxRetries=%d", maxRetries), func(t *testing.T) {
			executions := 0
			for attempts := 0; ; attempts++ {
				executions++
				if Decide(failure, attempts, maxRetries) == DeadLetter {
					break
				}
				if executions > maxRetries+1 {
					t.Fatalf("mensagem ainda não foi para a DLQ após %d execuções", executions)
				}
			}
			if executions != maxRetries+1 {
				t.Errorf("execuções = %d, esperado %d", executions, maxRetries+1)
			}
		})
	}
}

// Uma Action não inicializada nunca pode significar Ack: isso confirmaria (e perderia) a mensagem em silêncio.
func TestZeroValueActionIsNotAValidAction(t *testing.T) {
	var zero Action
	for _, valid := range []Action{Ack, Retry, DeadLetter} {
		if zero == valid {
			t.Errorf("o valor zero de Action é igual a %v; deveria ser um valor inválido", valid)
		}
	}
}

func TestActionsAreDistinct(t *testing.T) {
	if Ack == Retry || Retry == DeadLetter || Ack == DeadLetter {
		t.Errorf("ações deveriam ser distintas: Ack=%d Retry=%d DeadLetter=%d", int(Ack), int(Retry), int(DeadLetter))
	}
}

func TestActionString(t *testing.T) {
	tests := []struct {
		action Action
		want   string
	}{
		{Ack, "Ack"},
		{Retry, "Retry"},
		{DeadLetter, "DeadLetter"},
		{Action(0), "Action(0)"}, // valor zero: visível no log, nunca confundido com Ack
		{Action(99), "Action(99)"},
	}
	for _, tt := range tests {
		if got := tt.action.String(); got != tt.want {
			t.Errorf("Action(%d).String() = %q, esperado %q", int(tt.action), got, tt.want)
		}
		if got := fmt.Sprintf("%v", tt.action); got != tt.want {
			t.Errorf("fmt %%v de Action(%d) = %q, esperado %q", int(tt.action), got, tt.want)
		}
	}
}

// O projeto loga em JSON, e o slog.JSONHandler ignora String(): só LogValue() faz o nome aparecer.
func TestActionLogsAsNameInJSON(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	logger.Info("mensagem processada", "action", DeadLetter)

	var entry map[string]any
	if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
		t.Fatalf("log não é JSON válido: %v\n%s", err, buf.String())
	}
	if entry["action"] != "DeadLetter" {
		t.Errorf(`campo "action" = %v (%T), esperado "DeadLetter"`, entry["action"], entry["action"])
	}
}
