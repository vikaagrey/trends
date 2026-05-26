package middleware

import (
	"net/http"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

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

func Apply(next http.Handler, logger *zap.Logger, metricsRegistry *metrics.Registry) http.Handler {
	next = jsonValidator(logger, next)
	next = recovery(logger, next)
	next = httpMetrics(metricsRegistry, next)
	next = requestLogger(logger, next)
	return cors(next)
}

func cors(next http.Handler) http.Handler {
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

func requestLogger(log *zap.Logger, next http.Handler) http.Handler {
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

func httpMetrics(metricsRegistry *metrics.Registry, next http.Handler) http.Handler {
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

func jsonValidator(log *zap.Logger, next http.Handler) http.Handler {
	log = appLogger.Safe(log)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
			if !isJSONContentType(contentType) {
				log.Warn("JSONValidator: unsupported content type",
					zap.String("method", r.Method),
					zap.String("path", r.URL.Path),
					zap.String("content_type", contentType),
				)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnsupportedMediaType)
				_, _ = w.Write([]byte(`{"error":"Content-Type must be application/json"}`))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func recovery(log *zap.Logger, next http.Handler) http.Handler {
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
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"error":"internal server error"}`))
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
