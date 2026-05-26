package topn

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vikagrej/trends/internal/metrics"
)

type FilterProvider func() Filter

type Engine struct {
	aggregator      *Aggregator
	rebuildInterval time.Duration
	filterProvider  FilterProvider
	metricsRegistry *metrics.Registry

	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
	done      chan struct{}
	stopped   chan struct{}
}

type EngineConfig struct {
	Aggregator      *Aggregator
	RebuildInterval time.Duration
	FilterProvider  FilterProvider
	Metrics         *metrics.Registry
}

func NewEngine(config EngineConfig) *Engine {
	rebuildInterval := config.RebuildInterval
	if rebuildInterval <= 0 {
		rebuildInterval = time.Second
	}
	metricsRegistry := config.Metrics
	if metricsRegistry == nil {
		metricsRegistry = metrics.NewNoop()
	}
	return &Engine{
		aggregator:      config.Aggregator,
		rebuildInterval: rebuildInterval,
		filterProvider:  config.FilterProvider,
		metricsRegistry: metricsRegistry,
		done:            make(chan struct{}),
		stopped:         make(chan struct{}),
	}
}

func (e *Engine) Add(query, source string, eventTime time.Time) bool {
	return e.aggregator.Add(query, source, eventTime)
}

func (e *Engine) Top(limit int) Snapshot {
	return e.aggregator.Top(limit)
}

func (e *Engine) RebuildNow() {
	e.rebuild()
}

func (e *Engine) rebuild() {
	startedAt := time.Now()
	e.aggregator.Rebuild(e.currentFilter())
	e.metricsRegistry.RebuildDuration.Observe(time.Since(startedAt).Seconds())
	e.metricsRegistry.SnapshotSize.Set(float64(len(e.aggregator.snapshot.Load().Items)))
}

func (e *Engine) currentFilter() Filter {
	if e.filterProvider == nil {
		return nil
	}
	return e.filterProvider()
}

func (e *Engine) Start(ctx context.Context) {
	e.startOnce.Do(func() {
		e.started.Store(true)
		go e.loop(ctx)
	})
}

func (e *Engine) loop(ctx context.Context) {
	defer close(e.stopped)
	ticker := time.NewTicker(e.rebuildInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.done:
			return
		case <-ticker.C:
			e.rebuild()
		}
	}
}

func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		close(e.done)
	})
	if e.started.Load() {
		<-e.stopped
	}
}
