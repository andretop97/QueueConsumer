package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

const maxServiceNameLen = 255 - len("-exchange")

var serviceNamePattern = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type RabbitMQ struct {
	User     string `env:"RABBITMQ_DEFAULT_USER,notEmpty"`
	Password string `env:"RABBITMQ_DEFAULT_PASS,notEmpty"`
	Host     string `env:"RABBITMQ_DEFAULT_HOST" envDefault:"localhost"`
	Port     string `env:"RABBITMQ_DEFAULT_PORT" envDefault:"5672"`
}

func (r RabbitMQ) URL() string {
	u := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(r.User, r.Password),
		Host:   net.JoinHostPort(r.Host, r.Port),
		Path:   "/",
	}
	return u.String()
}

func validateHost(host string) error {
	if host == "" {
		return errors.New("RABBITMQ_DEFAULT_HOST não pode ser vazio")
	}
	if net.ParseIP(host) != nil { // aceita IPv6 como "::1" antes de rejeitar ':'
		return nil
	}
	if strings.ContainsAny(host, ":/") {
		return fmt.Errorf("RABBITMQ_DEFAULT_HOST deve ser apenas o host, sem esquema, porta ou caminho: %q", host)
	}
	return nil
}

func validatePort(port string) error {
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return fmt.Errorf("RABBITMQ_DEFAULT_PORT deve ser um inteiro entre 1 e 65535: %q", port)
	}
	return nil
}

func (r RabbitMQ) validate() error {
	return errors.Join(
		validateHost(r.Host),
		validatePort(r.Port),
	)
}

type Config struct {
	ServiceName string     `env:"SERVICE_NAME" envDefault:"QueueConsumer"`
	LogLevel    slog.Level `env:"LOG_LEVEL" envDefault:"INFO"`
	RabbitMQ    RabbitMQ
}

func validateServiceName(serviceName string) error {
	switch {
	case serviceName == "":
		return errors.New("SERVICE_NAME não pode ser vazio")
	case strings.HasPrefix(serviceName, "amq."):
		return fmt.Errorf("SERVICE_NAME não pode começar com \"amq.\" (prefixo reservado pelo RabbitMQ): %q", serviceName)
	case len(serviceName) > maxServiceNameLen:
		return fmt.Errorf("SERVICE_NAME deve ter no máximo %d bytes, tem %d", maxServiceNameLen, len(serviceName))
	case !serviceNamePattern.MatchString(serviceName):
		return fmt.Errorf("SERVICE_NAME deve conter apenas letras, números, '.', '_' ou '-': %q", serviceName)
	}
	return nil

}

func (c Config) validate() error {
	return errors.Join(
		validateServiceName(c.ServiceName),
		c.RabbitMQ.validate(),
	)
}

func Load() (*Config, error) {
	godotenv.Load()
	cfg, err := env.ParseAs[Config]()
	if err != nil {
		return nil, err
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}
