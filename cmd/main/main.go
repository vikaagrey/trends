package main

import (
	"log"
	"os"

	config "github.com/vikagrej/trends/internal/configs"
	"github.com/vikagrej/trends/internal/logger"
	transportwiring "github.com/vikagrej/trends/internal/transport/wiring"
)

func main() {
	settings, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %s", err)
	}

	zapLogger := logger.New(settings.LogLevel)
	defer func() { _ = zapLogger.Sync() }()

	if err := transportwiring.Run(settings, zapLogger); err != nil {
		log.Printf("Application failed: %s", err)
		_ = zapLogger.Sync()
		os.Exit(1)
	}
}
