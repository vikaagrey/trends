package transporthttp

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	appLogger "github.com/vikagrej/trends/internal/logger"
)

type Server struct {
	server       *http.Server
	logger       *zap.Logger
	shutdownOnce sync.Once
	shutdownErr  error
}

func NewServer(addr string, handler http.Handler, timeout time.Duration, logger *zap.Logger) *Server {
	return &Server{
		server: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadTimeout:       timeout,
			WriteTimeout:      timeout,
			IdleTimeout:       timeout,
			ReadHeaderTimeout: timeout,
		},
		logger: appLogger.Safe(logger),
	}
}

func (server *Server) Start() error {
	server.logger.Info("Starting HTTP server", zap.String("address", server.server.Addr))
	if err := server.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("HTTP server error: %w", err)
	}
	return nil
}

func (server *Server) Shutdown(ctx context.Context) error {
	server.shutdownOnce.Do(func() {
		server.logger.Info("Stopping HTTP server")
		if err := server.server.Shutdown(ctx); err != nil {
			server.shutdownErr = fmt.Errorf("shutdown HTTP server: %w", err)
			server.logger.Error("Failed to stop HTTP server", zap.Error(err))
			return
		}
		server.logger.Info("HTTP server stopped")
	})
	return server.shutdownErr
}
