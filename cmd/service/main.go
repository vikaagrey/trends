package main

import (
	"log"
	"os"

	config "github.com/vikagrej/trends/configs"
	"github.com/vikagrej/trends/internal/app"
	"github.com/vikagrej/trends/internal/logger"
)

func main() {
	settings, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}

	zapLogger := logger.New(settings.LogLevel)
	defer func() { _ = zapLogger.Sync() }()

	if err := app.Run(settings, zapLogger); err != nil {
		log.Printf("Application failed: %s", err)
		_ = zapLogger.Sync()
		os.Exit(1)
	}
}
