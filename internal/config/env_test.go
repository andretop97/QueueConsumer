package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var configEnvVars = []string{
	"RABBITMQ_DEFAULT_USER",
	"RABBITMQ_DEFAULT_PASS",
	"RABBITMQ_DEFAULT_HOST",
	"RABBITMQ_DEFAULT_PORT",
	"SERVICE_NAME",
	"LOG_LEVEL",
}

// isolateEnv remove todas as variáveis lidas pelo Load e muda para um diretório vazio (sem .env).
// t.Setenv registra a restauração, o que também desfaz o que o godotenv gravar no processo durante o teste.
func isolateEnv(t *testing.T) {
	t.Helper()
	for _, key := range configEnvVars {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
	chdir(t, t.TempDir())
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

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("RABBITMQ_DEFAULT_USER", "user")
	t.Setenv("RABBITMQ_DEFAULT_PASS", "pass")
	t.Setenv("RABBITMQ_DEFAULT_HOST", "rabbit")
	t.Setenv("RABBITMQ_DEFAULT_PORT", "5672")
}

func writeDotEnv(t *testing.T, content string) {
	t.Helper()
	if err := os.WriteFile(".env", []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestLoadReadsAllVariables(t *testing.T) {
	isolateEnv(t)
	setRequiredEnv(t)
	t.Setenv("SERVICE_NAME", "pedidos")
	t.Setenv("LOG_LEVEL", "DEBUG")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() erro = %v", err)
	}

	want := Config{
		ServiceName: "pedidos",
		LogLevel:    slog.LevelDebug,
		RabbitMQ:    RabbitMQ{User: "user", Password: "pass", Host: "rabbit", Port: "5672"},
	}
	if *cfg != want {
		t.Errorf("Load() = %+v, esperado %+v", *cfg, want)
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	isolateEnv(t)
	t.Setenv("RABBITMQ_DEFAULT_USER", "user")
	t.Setenv("RABBITMQ_DEFAULT_PASS", "pass")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() erro = %v", err)
	}
	if cfg.ServiceName != "QueueConsumer" {
		t.Errorf("ServiceName = %q, esperado default %q", cfg.ServiceName, "QueueConsumer")
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, esperado default %v", cfg.LogLevel, slog.LevelInfo)
	}
	if cfg.RabbitMQ.Host != "localhost" || cfg.RabbitMQ.Port != "5672" {
		t.Errorf("Host:Port = %s:%s, esperado default localhost:5672", cfg.RabbitMQ.Host, cfg.RabbitMQ.Port)
	}
}

func TestLoadReportsAllMissingRequiredVariables(t *testing.T) {
	isolateEnv(t)

	_, err := Load()
	if err == nil {
		t.Fatal("Load() = nil, esperado erro com variáveis obrigatórias ausentes")
	}
	for _, key := range []string{"RABBITMQ_DEFAULT_USER", "RABBITMQ_DEFAULT_PASS"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("erro deveria mencionar %s, obtido: %v", key, err)
		}
	}
}

func TestLoadRejectsEmptyRequiredVariable(t *testing.T) {
	isolateEnv(t)
	setRequiredEnv(t)
	t.Setenv("RABBITMQ_DEFAULT_PASS", "")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "RABBITMQ_DEFAULT_PASS") {
		t.Errorf("Load() erro = %v, esperado erro mencionando RABBITMQ_DEFAULT_PASS", err)
	}
}

func TestLoadRejectsInvalidLogLevel(t *testing.T) {
	isolateEnv(t)
	setRequiredEnv(t)
	t.Setenv("LOG_LEVEL", "verbose")

	// Erros de conversão da caarlos0/env citam o campo Go ("LogLevel"), não a variável de ambiente.
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "LogLevel") || !strings.Contains(err.Error(), "verbose") {
		t.Errorf("Load() erro = %v, esperado erro de conversão do LogLevel com o valor inválido", err)
	}
}

func TestLoadRunsValidation(t *testing.T) {
	isolateEnv(t)
	setRequiredEnv(t)
	t.Setenv("SERVICE_NAME", "amq.pedidos")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "SERVICE_NAME") {
		t.Errorf("Load() erro = %v, esperado erro de validação mencionando SERVICE_NAME", err)
	}
}

func TestLoadReadsDotEnvFromWorkingDirectory(t *testing.T) {
	isolateEnv(t)
	writeDotEnv(t, strings.Join([]string{
		"RABBITMQ_DEFAULT_USER=do_arquivo",
		"RABBITMQ_DEFAULT_PASS=senha_arquivo",
		"RABBITMQ_DEFAULT_HOST=rabbit",
		"RABBITMQ_DEFAULT_PORT=5672",
	}, "\n"))

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() erro = %v", err)
	}
	if cfg.RabbitMQ.User != "do_arquivo" || cfg.RabbitMQ.Password != "senha_arquivo" {
		t.Errorf("credenciais = %q/%q, esperado valores do .env", cfg.RabbitMQ.User, cfg.RabbitMQ.Password)
	}
}

func TestLoadEnvironmentTakesPrecedenceOverDotEnv(t *testing.T) {
	isolateEnv(t)
	setRequiredEnv(t)
	writeDotEnv(t, "RABBITMQ_DEFAULT_USER=do_arquivo\n")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() erro = %v", err)
	}
	if cfg.RabbitMQ.User != "user" {
		t.Errorf("User = %q, esperado o valor do ambiente %q", cfg.RabbitMQ.User, "user")
	}
}

func TestLoadWithoutDotEnvUsesEnvironmentOnly(t *testing.T) {
	isolateEnv(t)
	setRequiredEnv(t)
	if _, err := os.Stat(filepath.Join(".", ".env")); !os.IsNotExist(err) {
		t.Fatalf("pré-condição: não deveria haver .env no diretório do teste")
	}

	if _, err := Load(); err != nil {
		t.Errorf("Load() erro = %v, esperado nil sem .env", err)
	}
}
