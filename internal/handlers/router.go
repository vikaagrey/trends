package handlers

import (
	"net/http"

	"go.uber.org/zap"

	"github.com/vikagrej/trends/internal/metrics"
	"github.com/vikagrej/trends/internal/middleware"
	"github.com/vikagrej/trends/internal/service"
	"github.com/vikagrej/trends/internal/stoplist"
)

func NewRouter(engine service.TopReader, stoplistService *stoplist.Service, metricsRegistry *metrics.Registry, logger *zap.Logger) http.Handler {
	trendsService := service.NewTrendsService(engine, stoplistService)
	handler := NewHandler(trendsService, logger)

	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	if metricsRegistry != nil {
		mux.Handle("GET /metrics", metrics.HTTPHandler(metricsRegistry))
		return applyMiddleware(mux, logger, metricsRegistry)
	}

	return applyMiddleware(mux, logger, nil)
}

func applyMiddleware(handler http.Handler, logger *zap.Logger, metricsRegistry *metrics.Registry) http.Handler {
	handler = middleware.JSONValidator(logger, handler)
	handler = middleware.Recovery(logger, handler)
	handler = middleware.HTTPMetrics(metricsRegistry, handler)
	handler = middleware.RequestLogger(logger, handler)
	return middleware.CORS(handler)
}
