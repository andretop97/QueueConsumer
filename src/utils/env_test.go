package utils

import (
	"os"
	"path/filepath"
	"testing"
)

// unsetEnv garante que a variável não existe durante o teste e restaura o valor original depois.
func unsetEnv(t *testing.T, key string) {
	t.Helper()
	t.Setenv(key, "")
	os.Unsetenv(key)
}

// chdir muda o diretório de trabalho durante o teste (t.Chdir só existe a partir do Go 1.24).
func chdir(t *testing.T, dir string) {
	t.Helper()
	original, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(original) })
}

func TestGetEnvOrDefault(t *testing.T) {
	const key = "QUEUE_CONSUMER_TEST_VAR"

	t.Run("variável definida retorna o valor", func(t *testing.T) {
		t.Setenv(key, "valor")
		if got := GetEnvOrDefault(key, "padrao"); got != "valor" {
			t.Errorf("GetEnvOrDefault() = %q, esperado %q", got, "valor")
		}
	})

	t.Run("variável ausente retorna o default", func(t *testing.T) {
		unsetEnv(t, key)
		if got := GetEnvOrDefault(key, "padrao"); got != "padrao" {
			t.Errorf("GetEnvOrDefault() = %q, esperado %q", got, "padrao")
		}
	})

	t.Run("variável definida como vazia retorna vazio, não o default", func(t *testing.T) {
		t.Setenv(key, "")
		if got := GetEnvOrDefault(key, "padrao"); got != "" {
			t.Errorf("GetEnvOrDefault() = %q, esperado string vazia", got)
		}
	})
}

func TestGetEnv(t *testing.T) {
	const key = "QUEUE_CONSUMER_TEST_VAR"

	t.Run("variável definida retorna o valor", func(t *testing.T) {
		t.Setenv(key, "valor")
		if got := GetEnv(key); got != "valor" {
			t.Errorf("GetEnv() = %q, esperado %q", got, "valor")
		}
	})

	t.Run("variável ausente retorna vazio", func(t *testing.T) {
		unsetEnv(t, key)
		if got := GetEnv(key); got != "" {
			t.Errorf("GetEnv() = %q, esperado string vazia", got)
		}
	})
}

func TestLoadEnv(t *testing.T) {
	const key = "QUEUE_CONSUMER_TEST_DOTENV"

	t.Run("carrega variáveis do .env no diretório atual", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(key+"=do_arquivo\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		chdir(t, dir)
		unsetEnv(t, key)

		LoadEnv()

		if got := os.Getenv(key); got != "do_arquivo" {
			t.Errorf("%s = %q, esperado %q", key, got, "do_arquivo")
		}
	})

	t.Run("não sobrescreve variável já definida no ambiente", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, ".env"), []byte(key+"=do_arquivo\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		chdir(t, dir)
		t.Setenv(key, "do_ambiente")

		LoadEnv()

		if got := os.Getenv(key); got != "do_ambiente" {
			t.Errorf("%s = %q, esperado %q", key, got, "do_ambiente")
		}
	})

	t.Run("sem .env não entra em pânico nem altera o ambiente", func(t *testing.T) {
		chdir(t, t.TempDir())
		unsetEnv(t, key)

		LoadEnv()

		if _, exists := os.LookupEnv(key); exists {
			t.Errorf("%s não deveria existir", key)
		}
	})
}
