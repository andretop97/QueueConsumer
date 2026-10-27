package logger

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"testing"
)

func TestSetLoggerSettingsEmitsJSONAtDebugLevel(t *testing.T) {
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	// SetLoggerSettings captura os.Stdout no momento da chamada, então ele é trocado por um pipe antes.
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	originalStdout := os.Stdout
	os.Stdout = w
	SetLoggerSettings()
	os.Stdout = originalStdout

	slog.Debug("mensagem de debug", "chave", "valor")
	w.Close()

	output, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}

	var entry map[string]any
	if err := json.Unmarshal(output, &entry); err != nil {
		t.Fatalf("saída não é JSON válido: %v\n%s", err, output)
	}

	want := map[string]string{"level": "DEBUG", "msg": "mensagem de debug", "chave": "valor"}
	for field, value := range want {
		if entry[field] != value {
			t.Errorf("campo %q = %v, esperado %q", field, entry[field], value)
		}
	}
}
