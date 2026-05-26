package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	config "github.com/vikagrej/trends/configs"
	"github.com/vikagrej/trends/internal/consumer"
	"github.com/vikagrej/trends/internal/handlers"
	infraPostgres "github.com/vikagrej/trends/internal/infra/postgres"
	"github.com/vikagrej/trends/internal/metrics"
	httpserver "github.com/vikagrej/trends/internal/server"
	"github.com/vikagrej/trends/internal/stoplist"
	"github.com/vikagrej/trends/internal/topn"
)

const (
	fatalErrorBufferSize = 1
	consumerRetryDelay   = 3 * time.Second
)

type App struct {
	settings        *config.Config
	engine          *topn.Engine
	consumer        *consumer.Consumer
	httpServer      *httpserver.Server
	stoplistService *stoplist.Service
	postgresDB      *infraPostgres.DB
	pubSub          *stoplist.PubSub
	logger          *zap.Logger
	metricsRegistry *metrics.Registry
}

func Run(settings *config.Config, logger *zap.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	application, err := New(ctx, settings, logger)
	if err != nil {
		return fmt.Errorf("initialize application: %w", err)
	}

	if err := application.Run(ctx); err != nil {
		return fmt.Errorf("run application: %w", err)
	}
	return nil
}

func New(ctx context.Context, settings *config.Config, logger *zap.Logger) (application *App, err error) {
	if err := settings.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}
	metricsRegistry := metrics.NewRegistry()
	var cleanup []func()
	defer func() {
		if err == nil {
			return
		}
		for index := len(cleanup) - 1; index >= 0; index-- {
			cleanup[index]()
		}
	}()

	postgresDB, err := infraPostgres.New(ctx, settings.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("initialize Postgres: %w", err)
	}
	cleanup = append(cleanup, postgresDB.Close)

	postgresRepo, err := stoplist.NewPostgresRepository(ctx, postgresDB.Pool())
	if err != nil {
		return nil, fmt.Errorf("initialize stoplist Postgres repository: %w", err)
	}

	aggregator := topn.NewAggregator(topn.Config{
		WindowSize:  settings.WindowSize,
		BucketCount: settings.BucketCount,
		TopK:        settings.TopK,
	})

	stoplistService := stoplist.NewService(postgresRepo)
	stoplistService.SetMetrics(metricsRegistry)
	if err := stoplistService.Init(ctx); err != nil {
		return nil, fmt.Errorf("initialize stoplist: %w", err)
	}

	var pubSub *stoplist.PubSub
	if settings.RedisURL != "" {
		pubSub, err = stoplist.NewPubSub(ctx, settings.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("initialize Redis Pub/Sub: %w", err)
		}
		cleanup = append(cleanup, func() { _ = pubSub.Close() })
		stoplistService.SetPubSub(pubSub)
	}

	engine := topn.NewEngine(topn.EngineConfig{
		Aggregator:      aggregator,
		RebuildInterval: settings.RebuildInterval,
		FilterProvider:  stoplistService.Filter,
		Metrics:         metricsRegistry,
	})

	kafkaSource := consumer.NewKafkaSource(consumer.KafkaConfig{
		Brokers: settings.KafkaBrokers,
		Topic:   settings.KafkaTopic,
		GroupID: settings.KafkaGroupID,
	})
	cleanup = append(cleanup, func() { _ = kafkaSource.Close() })
	kafkaConsumer := consumer.New(consumer.Config{
		Source:  kafkaSource,
		Sink:    engine,
		Metrics: metricsRegistry,
	})

	router := handlers.NewRouter(engine, stoplistService, metricsRegistry, logger.Named("http"))
	httpServer := httpserver.New(settings.HTTPAddr, router, settings.RequestTimeout, logger.Named("http_server"))

	return &App{
		settings:        settings,
		engine:          engine,
		consumer:        kafkaConsumer,
		stoplistService: stoplistService,
		postgresDB:      postgresDB,
		pubSub:          pubSub,
		logger:          logger,
		metricsRegistry: metricsRegistry,
		httpServer:      httpServer,
	}, nil
}

func (app *App) Run(ctx context.Context) error {
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	fatalErrCh := make(chan error, fatalErrorBufferSize)
	app.stoplistService.StartSync(runCtx, func(err error) {
		app.logger.Warn("Stoplist sync failed", zap.Error(err))
	})
	app.engine.Start(runCtx)
	app.engine.RebuildNow()

	go func() {
		// Keep serving the last snapshot while Kafka is temporarily unavailable.
		for runCtx.Err() == nil {
			if err := app.consumer.Run(runCtx); err != nil && runCtx.Err() == nil {
				app.metricsRegistry.ConsumerRetries.Inc()
				app.logger.Warn(
					"Consumer failed, retry scheduled",
					zap.Duration("retry_after", consumerRetryDelay),
					zap.Error(err),
				)
				select {
				case <-runCtx.Done():
					return
				case <-time.After(consumerRetryDelay):
				}
			} else {
				return
			}
		}
	}()

	go func() {
		if err := app.httpServer.Start(); err != nil && runCtx.Err() == nil {
			fatalErrCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		stop()
		return app.shutdownWithTimeout()
	case err := <-fatalErrCh:
		stop()
		shutdownErr := app.shutdownWithTimeout()
		return errors.Join(err, shutdownErr)
	}
}

func (app *App) shutdownWithTimeout() error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), app.settings.ShutdownTimeout)
	defer cancel()
	return app.Shutdown(shutdownCtx)
}

func (app *App) Shutdown(ctx context.Context) error {
	var errs []error
	if err := app.httpServer.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("shutdown HTTP server: %w", err))
	}
	if err := app.consumer.Close(); err != nil {
		errs = append(errs, fmt.Errorf("close consumer: %w", err))
	}
	app.engine.Stop()
	app.postgresDB.Close()
	if app.pubSub != nil {
		if err := app.pubSub.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close Redis Pub/Sub: %w", err))
		}
	}
	return errors.Join(errs...)
}
