package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	HTTPAddr string

	PostgresDSN string
	RedisURL    string

	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string

	WindowSize      time.Duration
	BucketCount     int
	TopK            int
	RebuildInterval time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration

	LogLevel string
}

func Load() (*Config, error) {
	loadedConfig := &Config{
		HTTPAddr:        getenv("HTTP_ADDR", ":8080"),
		PostgresDSN:     getenv("POSTGRES_DSN", "postgres://trends:trends@localhost:5432/trends?sslmode=disable"),
		RedisURL:        getenv("REDIS_URL", ""),
		KafkaTopic:      getenv("KAFKA_TOPIC", "search.events"),
		KafkaGroupID:    getenv("KAFKA_GROUP_ID", "trends-service"),
		WindowSize:      durationEnv("WINDOW_SIZE", 5*time.Minute),
		BucketCount:     intEnv("BUCKET_COUNT", 30),
		TopK:            intEnv("TOP_K", 1000),
		RebuildInterval: durationEnv("REBUILD_INTERVAL", time.Second),
		RequestTimeout:  durationEnv("REQUEST_TIMEOUT", 10*time.Second),
		ShutdownTimeout: durationEnv("SHUTDOWN_TIMEOUT", 10*time.Second),
		LogLevel:        getenv("LOG_LEVEL", "info"),
	}
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		for _, part := range strings.Split(brokers, ",") {
			if broker := strings.TrimSpace(part); broker != "" {
				loadedConfig.KafkaBrokers = append(loadedConfig.KafkaBrokers, broker)
			}
		}
	} else {
		loadedConfig.KafkaBrokers = []string{"localhost:9092"}
	}
	return loadedConfig, nil
}

func getenv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func intEnv(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsedValue, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return parsedValue
}

func durationEnv(key string, defaultValue time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	parsedValue, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue
	}
	return parsedValue
}

func (config *Config) Validate() error {
	if config.HTTPAddr == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if len(config.KafkaBrokers) == 0 {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	if config.PostgresDSN == "" {
		return fmt.Errorf("POSTGRES_DSN is required")
	}
	if config.RequestTimeout <= 0 {
		return fmt.Errorf("REQUEST_TIMEOUT must be positive")
	}
	if config.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be positive")
	}
	return nil
}
