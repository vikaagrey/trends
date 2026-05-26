package middleware

import (
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/vikagrej/trends/internal/httpx"
	appLogger "github.com/vikagrej/trends/internal/logger"
	"github.com/vikagrej/trends/internal/metrics"
)

type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (writer *statusWriter) WriteHeader(code int) {
	writer.statusCode = code
	writer.ResponseWriter.WriteHeader(code)
}

func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method == http.MethodGet {
			w.Header().Set("Access-Control-Allow-Origin", "*")
		}

		next.ServeHTTP(w, r)
	})
}

func HTTPMetrics(metricsRegistry *metrics.Registry, next http.Handler) http.Handler {
	if metricsRegistry == nil {
		metricsRegistry = metrics.NewNoop()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		response := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		startedAt := time.Now()
		next.ServeHTTP(response, r)
		metricsRegistry.HTTPDuration.WithLabelValues(r.Method, routePattern(r), strconv.Itoa(response.statusCode)).
			Observe(time.Since(startedAt).Seconds())
	})
}

func RequestLogger(log *zap.Logger, next http.Handler) http.Handler {
	log = appLogger.Safe(log)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		response := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
		startedAt := time.Now()
		next.ServeHTTP(response, r)
		log.Info("Incoming HTTP request",
			zap.String("method", r.Method),
			zap.String("path", routePattern(r)),
			zap.Int("status", response.statusCode),
			zap.Int64("duration_ms", time.Since(startedAt).Milliseconds()),
		)
	})
}

func JSONValidator(log *zap.Logger, next http.Handler) http.Handler {
	log = appLogger.Safe(log)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
			if !isJSONContentType(contentType) {
				if err := httpx.Error(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json"); err != nil {
					log.Warn("JSONValidator: failed to write response",
						zap.Error(err),
						zap.String("method", r.Method),
						zap.String("path", r.URL.Path),
						zap.String("content_type", contentType),
					)
				}
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func Recovery(log *zap.Logger, next http.Handler) http.Handler {
	log = appLogger.Safe(log)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("Panic in HTTP handler",
					zap.Any("panic", rec),
					zap.String("stack", string(debug.Stack())),
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
				)
				if err := httpx.Error(w, http.StatusInternalServerError, "internal server error"); err != nil {
					log.Warn("Recovery: failed to write response",
						zap.Error(err),
						zap.String("method", r.Method),
						zap.String("path", r.URL.Path),
					)
				}
			}
		}()

		next.ServeHTTP(w, r)
	})
}

func routePattern(r *http.Request) string {
	if r.Pattern == "" {
		return "unmatched"
	}
	_, pathPattern, ok := strings.Cut(r.Pattern, " ")
	if ok && strings.HasPrefix(pathPattern, "/") {
		return pathPattern
	}
	return r.Pattern
}

func isJSONContentType(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	mediaType = strings.TrimSpace(mediaType)
	return mediaType == "application/json" || strings.HasSuffix(mediaType, "+json")
}
