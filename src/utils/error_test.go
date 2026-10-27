package utils

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestFailOnErrorWithNilDoesNothing(t *testing.T) {
	// Se FailOnError chamasse os.Exit aqui, o binário de teste morreria e o teste falharia.
	FailOnError(nil, "não deveria sair")
}

// FailOnError chama os.Exit(1), então o caso de erro roda num subprocesso do próprio binário de teste.
func TestFailOnErrorWithErrorExitsWithCode1(t *testing.T) {
	if os.Getenv("QUEUE_CONSUMER_FAIL_ON_ERROR_CHILD") == "1" {
		FailOnError(errors.New("boom"), "mensagem de falha")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestFailOnErrorWithErrorExitsWithCode1$")
	cmd.Env = append(os.Environ(), "QUEUE_CONSUMER_FAIL_ON_ERROR_CHILD=1")
	output, err := cmd.CombinedOutput()

	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("esperado que o processo saísse com erro, obtido err=%v, saída:\n%s", err, output)
	}
	if code := exitErr.ExitCode(); code != 1 {
		t.Errorf("código de saída = %d, esperado 1", code)
	}
	for _, want := range []string{"mensagem de falha", "boom"} {
		if !strings.Contains(string(output), want) {
			t.Errorf("saída deveria conter %q, obtido:\n%s", want, output)
		}
	}
}
