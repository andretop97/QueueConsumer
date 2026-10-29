package config

import (
	"log/slog"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// validConfig retorna uma configuração válida; cada caso de teste altera só o campo sob teste.
func validConfig() Config {
	return Config{
		ServiceName: "pedidos",
		LogLevel:    slog.LevelInfo,
		RabbitMQ: RabbitMQ{
			User:     "user",
			Password: "pass",
			Host:     "localhost",
			Port:     "5672",
		},
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := validConfig()
	if err := cfg.validate(); err != nil {
		t.Errorf("validate() = %v, esperado nil", err)
	}
}

func TestValidateServiceName(t *testing.T) {
	// Maior ServiceName aceito: 255 bytes (limite do RabbitMQ) - len("-exchange"), o sufixo mais longo.
	longest := strings.Repeat("a", 255-len("-exchange"))

	valid := []string{
		"pedidos",
		"QueueConsumer",
		"order-service_v2.1",
		longest,
	}
	for _, name := range valid {
		t.Run("válido/"+truncate(name), func(t *testing.T) {
			cfg := validConfig()
			cfg.ServiceName = name
			if err := cfg.validate(); err != nil {
				t.Errorf("validate() = %v, esperado nil para SERVICE_NAME=%q", err, name)
			}
		})
	}

	invalid := map[string]string{
		"vazio":                       "",
		"prefixo amq. reservado":      "amq.pedidos",
		"excede 255 bytes com sufixo": longest + "a",
		"espaço":                      "meus pedidos",
		"barra":                       "pedidos/v2",
		"caractere fora do conjunto":  "pedidos#1",
	}
	for caseName, name := range invalid {
		t.Run("inválido/"+caseName, func(t *testing.T) {
			cfg := validConfig()
			cfg.ServiceName = name
			assertInvalid(t, cfg, "SERVICE_NAME")
		})
	}
}

func TestValidateRabbitMQHost(t *testing.T) {
	valid := []string{
		"localhost",
		"rabbitmq",
		"rabbit.internal.example.com",
		"10.0.0.1",
		"::1", // IPv6 sem colchetes é um host válido; os colchetes são adicionados por net.JoinHostPort
	}
	for _, host := range valid {
		t.Run("válido/"+host, func(t *testing.T) {
			cfg := validConfig()
			cfg.RabbitMQ.Host = host
			if err := cfg.validate(); err != nil {
				t.Errorf("validate() = %v, esperado nil para RABBITMQ_DEFAULT_HOST=%q", err, host)
			}
		})
	}

	invalid := map[string]string{
		"vazio":          "",
		"com esquema":    "amqp://localhost",
		"com porta":      "localhost:5672",
		"com caminho":    "localhost/vhost",
		"IPv6 com porta": "[::1]:5672",
	}
	for caseName, host := range invalid {
		t.Run("inválido/"+caseName, func(t *testing.T) {
			cfg := validConfig()
			cfg.RabbitMQ.Host = host
			assertInvalid(t, cfg, "RABBITMQ_DEFAULT_HOST")
		})
	}
}

func TestValidateRabbitMQPort(t *testing.T) {
	for _, port := range []string{"1", "5672", "65535"} {
		t.Run("válida/"+port, func(t *testing.T) {
			cfg := validConfig()
			cfg.RabbitMQ.Port = port
			if err := cfg.validate(); err != nil {
				t.Errorf("validate() = %v, esperado nil para RABBITMQ_DEFAULT_PORT=%q", err, port)
			}
		})
	}

	invalid := map[string]string{
		"vazia":          "",
		"não numérica":   "abc",
		"zero":           "0",
		"negativa":       "-1",
		"acima de 65535": "65536",
	}
	for caseName, port := range invalid {
		t.Run("inválida/"+caseName, func(t *testing.T) {
			cfg := validConfig()
			cfg.RabbitMQ.Port = port
			assertInvalid(t, cfg, "RABBITMQ_DEFAULT_PORT")
		})
	}
}

func TestValidateReportsAllViolationsTogether(t *testing.T) {
	cfg := validConfig()
	cfg.ServiceName = "amq.pedidos"
	cfg.RabbitMQ.Host = "amqp://localhost"
	cfg.RabbitMQ.Port = "abc"

	err := cfg.validate()
	if err == nil {
		t.Fatal("validate() = nil, esperado erro")
	}
	for _, envVar := range []string{"SERVICE_NAME", "RABBITMQ_DEFAULT_HOST", "RABBITMQ_DEFAULT_PORT"} {
		if !strings.Contains(err.Error(), envVar) {
			t.Errorf("erro deveria mencionar %s, obtido: %v", envVar, err)
		}
	}
}

func TestRabbitMQURL(t *testing.T) {
	tests := []struct {
		name     string
		rabbit   RabbitMQ
		wantURL  string
		wantHost string
	}{
		{
			name:     "valores simples",
			rabbit:   RabbitMQ{User: "user", Password: "pass", Host: "rabbit", Port: "5672"},
			wantURL:  "amqp://user:pass@rabbit:5672/",
			wantHost: "rabbit",
		},
		{
			name:     "IPv6 recebe colchetes",
			rabbit:   RabbitMQ{User: "user", Password: "pass", Host: "::1", Port: "5672"},
			wantURL:  "amqp://user:pass@[::1]:5672/",
			wantHost: "::1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rabbit.URL(); got != tt.wantURL {
				t.Errorf("URL() = %q, esperado %q", got, tt.wantURL)
			}
			uri, err := amqp.ParseURI(tt.rabbit.URL())
			if err != nil {
				t.Fatalf("amqp.ParseURI() erro = %v", err)
			}
			if uri.Host != tt.wantHost || uri.Port != 5672 || uri.Vhost != "/" {
				t.Errorf("amqp.ParseURI() = %+v, esperado host %q, porta 5672, vhost /", uri, tt.wantHost)
			}
		})
	}
}

func TestRabbitMQURLEscapesCredentials(t *testing.T) {
	const user, password = "us@er", "p@ss:w/rd?#"
	r := RabbitMQ{User: user, Password: password, Host: "rabbit", Port: "5672"}

	uri, err := amqp.ParseURI(r.URL())
	if err != nil {
		t.Fatalf("amqp.ParseURI() erro = %v", err)
	}
	if uri.Username != user || uri.Password != password {
		t.Errorf("credenciais = %q/%q, esperado %q/%q", uri.Username, uri.Password, user, password)
	}
}

func assertInvalid(t *testing.T, cfg Config, envVar string) {
	t.Helper()
	err := cfg.validate()
	if err == nil {
		t.Fatalf("validate() = nil, esperado erro para %s", envVar)
	}
	if !strings.Contains(err.Error(), envVar) {
		t.Errorf("erro deveria mencionar %s, obtido: %v", envVar, err)
	}
}

func truncate(s string) string {
	if len(s) > 20 {
		return s[:20] + "..."
	}
	return s
}
