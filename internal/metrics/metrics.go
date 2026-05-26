package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Registry struct {
	prometheus *prometheus.Registry

	EventsConsumed  prometheus.Counter
	EventsDecoded   prometheus.Counter
	EventsInvalid   prometheus.Counter
	EventsNormDrop  prometheus.Counter
	EventsOutWindow prometheus.Counter
	ConsumerRetries prometheus.Counter

	RebuildDuration prometheus.Histogram
	SnapshotSize    prometheus.Gauge
	StoplistSize    prometheus.Gauge

	HTTPDuration *prometheus.HistogramVec
}

func NewRegistry() *Registry {
	prometheusRegistry := prometheus.NewRegistry()
	r := &Registry{
		prometheus: prometheusRegistry,
		EventsConsumed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trends_events_consumed_total",
			Help: "Kafka events successfully written into the sliding window.",
		}),
		EventsDecoded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trends_events_decoded_total",
			Help: "Kafka events decoded (before window filtering).",
		}),
		EventsInvalid: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trends_events_invalid_total",
			Help: "Kafka events dropped due to invalid payload.",
		}),
		EventsNormDrop: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trends_events_norm_dropped_total",
			Help: "Kafka events dropped because the query is empty after normalisation.",
		}),
		EventsOutWindow: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trends_events_out_of_window_total",
			Help: "Kafka events dropped because their timestamp is outside the window.",
		}),
		ConsumerRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "trends_consumer_retries_total",
			Help: "Kafka consumer retry attempts after read or commit errors.",
		}),
		RebuildDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "trends_rebuild_duration_seconds",
			Help:    "Time spent rebuilding the top-N snapshot.",
			Buckets: prometheus.DefBuckets,
		}),
		SnapshotSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trends_snapshot_size",
			Help: "Number of unique queries in the current snapshot.",
		}),
		StoplistSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "trends_stoplist_words_total",
			Help: "Number of words currently in the stop-list.",
		}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "trends_http_request_duration_seconds",
			Help:    "HTTP handler latency.",
			Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"method", "path", "status"}),
	}

	prometheusRegistry.MustRegister(
		r.EventsConsumed,
		r.EventsDecoded,
		r.EventsInvalid,
		r.EventsNormDrop,
		r.EventsOutWindow,
		r.ConsumerRetries,
		r.RebuildDuration,
		r.SnapshotSize,
		r.StoplistSize,
		r.HTTPDuration,
	)
	return r
}

func NewNoop() *Registry {
	return NewRegistry()
}

func HTTPHandler(registry *Registry) http.Handler {
	if registry == nil {
		return promhttp.Handler()
	}
	return promhttp.HandlerFor(registry.prometheus, promhttp.HandlerOpts{})
}
