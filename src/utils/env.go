package utils

import (
	"log/slog"
	"os"

	"github.com/joho/godotenv"
)

func LoadEnv() {
	err := godotenv.Load(".env")

	if err != nil {
		slog.Error("Erro ao carregar variaveis de ambiente", "err", err)
	}
}

func GetEnvOrDefault(key string, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}

func GetEnv(key string) string {
	return os.Getenv(key)
}