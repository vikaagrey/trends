package transporthttp

import (
	"net/http"

	"go.uber.org/zap"

	"github.com/vikagrej/trends/internal/metrics"
	"github.com/vikagrej/trends/internal/transport/http/handler"
	"github.com/vikagrej/trends/internal/transport/http/middleware"
	trendsuc "github.com/vikagrej/trends/internal/usecase/trends"
)

func NewRouter(trendsUseCase *trendsuc.UseCase, metricsRegistry *metrics.Registry, logger *zap.Logger) http.Handler {
	httpHandler := handler.New(trendsUseCase, logger)

	mux := http.NewServeMux()
	httpHandler.RegisterRoutes(mux)

	if metricsRegistry != nil {
		mux.Handle("GET /metrics", metrics.HTTPHandler(metricsRegistry))
		return middleware.Apply(mux, logger, metricsRegistry)
	}
	return middleware.Apply(mux, logger, nil)
}
