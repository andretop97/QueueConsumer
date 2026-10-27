package customErrors

import (
	"log/slog"
	"os"
)

func FailOnError(err error, message string) {
	if err != nil {
		slog.Error(message, "error", err)
		os.Exit(1)
	}
}
