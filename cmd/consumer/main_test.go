package main

import (
	"bytes"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func setRabbitEnv(t *testing.T, user, pass, host, port string) {
	t.Helper()
	t.Setenv("RABBITMQ_DEFAULT_USER", user)
	t.Setenv("RABBITMQ_DEFAULT_PASS", pass)
	t.Setenv("RABBITMQ_DEFAULT_HOST", host)
	t.Setenv("RABBITMQ_DEFAULT_PORT", port)
}

// captureDefaultLogger redireciona o slog global para um buffer durante o teste.
func captureDefaultLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func TestGetRabbitURLfromEnv(t *testing.T) {
	captureDefaultLogger(t)

	t.Run("monta a URL com todas as variáveis", func(t *testing.T) {
		setRabbitEnv(t, "user", "pass", "rabbit", "1234")
		want := "amqp://user:pass@rabbit:1234/"
		if got := getRabbitURLfromEnv(); got != want {
			t.Errorf("getRabbitURLfromEnv() = %q, esperado %q", got, want)
		}
	})

	t.Run("usa localhost:5672 quando host e porta não estão definidos", func(t *testing.T) {
		setRabbitEnv(t, "user", "pass", "", "")
		os.Unsetenv("RABBITMQ_DEFAULT_HOST")
		os.Unsetenv("RABBITMQ_DEFAULT_PORT")
		want := "amqp://user:pass@localhost:5672/"
		if got := getRabbitURLfromEnv(); got != want {
			t.Errorf("getRabbitURLfromEnv() = %q, esperado %q", got, want)
		}
	})
}

func TestGetRabbitURLfromEnvEscapesSpecialCharacters(t *testing.T) {
	captureDefaultLogger(t)
	const password = "p@ss:w/rd"
	setRabbitEnv(t, "user", password, "rabbit", "5672")

	parsed, err := url.Parse(getRabbitURLfromEnv())
	if err != nil {
		t.Fatalf("URL inválida: %v", err)
	}
	if got, _ := parsed.User.Password(); got != password {
		t.Errorf("senha = %q, esperado %q", got, password)
	}
	if parsed.Host != "rabbit:5672" {
		t.Errorf("host = %q, esperado %q", parsed.Host, "rabbit:5672")
	}

	// O que importa no fim é o que o driver AMQP entende da URL.
	uri, err := amqp.ParseURI(getRabbitURLfromEnv())
	if err != nil {
		t.Fatalf("amqp.ParseURI() erro = %v", err)
	}
	if uri.Password != password || uri.Host != "rabbit" || uri.Port != 5672 || uri.Vhost != "/" {
		t.Errorf("amqp.ParseURI() = %+v, esperado senha %q, host rabbit, porta 5672, vhost /", uri, password)
	}
}

func TestGetRabbitURLfromEnvDoesNotLogPassword(t *testing.T) {
	buf := captureDefaultLogger(t)
	const password = "senha-super-secreta"
	setRabbitEnv(t, "user", password, "rabbit", "5672")

	getRabbitURLfromEnv()

	if strings.Contains(buf.String(), password) {
		t.Errorf("o log não deveria conter a senha, obtido:\n%s", buf.String())
	}
}

func TestHandlerReturnsNil(t *testing.T) {
	if err := handler("qualquer mensagem"); err != nil {
		t.Errorf("handler() = %v, esperado nil", err)
	}
}
