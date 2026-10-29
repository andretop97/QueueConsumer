package logger

import (
	"log/slog"
	"os"
)

func SetLoggerSettings() {
	level := slog.LevelDebug
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)
}

func Info(message string){
	slog.Info(message)
}

func Warn(message string){
	slog.Warn(message)
}

func Error(message string){
	slog.Error(message)
}

func Debug(message string){
	slog.Debug(message)
}