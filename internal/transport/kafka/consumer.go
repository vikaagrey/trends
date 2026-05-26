package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/vikagrej/trends/internal/metrics"
	"github.com/vikagrej/trends/internal/query"
)

type Message struct {
	Value  []byte
	handle any
}

func (message Message) Handle() any { return message.handle }

func NewMessage(value []byte, handle any) Message {
	return Message{Value: value, handle: handle}
}

type MessageSource interface {
	Fetch(ctx context.Context) (Message, error)
	Commit(ctx context.Context, msg Message) error
	Close() error
}

type Sink interface {
	Add(query, source string, ts time.Time) bool
}

type Stats struct {
	Consumed    uint64
	Decoded     uint64
	Invalid     uint64
	NormDropped uint64
	OutOfWindow uint64
}

type statsCounters struct {
	Consumed    atomic.Uint64
	Decoded     atomic.Uint64
	Invalid     atomic.Uint64
	NormDropped atomic.Uint64
	OutOfWindow atomic.Uint64
}

type Consumer struct {
	source MessageSource
	sink   Sink

	metricsRegistry *metrics.Registry
	stats           statsCounters
}

type Config struct {
	Source  MessageSource
	Sink    Sink
	Metrics *metrics.Registry
}

func New(cfg Config) *Consumer {
	metricsRegistry := cfg.Metrics
	if metricsRegistry == nil {
		metricsRegistry = metrics.NewNoop()
	}
	return &Consumer{
		source:          cfg.Source,
		sink:            cfg.Sink,
		metricsRegistry: metricsRegistry,
	}
}

func (consumer *Consumer) Run(ctx context.Context) error {
	for {
		message, err := consumer.source.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		consumer.process(message.Value)

		if err := consumer.source.Commit(ctx, message); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("commit message: %w", err)
		}
	}
}

func (consumer *Consumer) process(raw []byte) {
	event, err := decodeEvent(raw)
	if err != nil {
		consumer.stats.Invalid.Add(1)
		consumer.metricsRegistry.EventsInvalid.Inc()
		return
	}
	consumer.stats.Decoded.Add(1)
	consumer.metricsRegistry.EventsDecoded.Inc()

	normalizedQuery, ok := query.Normalize(event.Query)
	if !ok {
		consumer.stats.NormDropped.Add(1)
		consumer.metricsRegistry.EventsNormDrop.Inc()
		return
	}

	if consumer.sink.Add(normalizedQuery, event.Source, event.Timestamp()) {
		consumer.stats.Consumed.Add(1)
		consumer.metricsRegistry.EventsConsumed.Inc()
	} else {
		consumer.stats.OutOfWindow.Add(1)
		consumer.metricsRegistry.EventsOutWindow.Inc()
	}
}

func (consumer *Consumer) Stats() Stats {
	return Stats{
		Consumed:    consumer.stats.Consumed.Load(),
		Decoded:     consumer.stats.Decoded.Load(),
		Invalid:     consumer.stats.Invalid.Load(),
		NormDropped: consumer.stats.NormDropped.Load(),
		OutOfWindow: consumer.stats.OutOfWindow.Load(),
	}
}

func (consumer *Consumer) Close() error {
	if consumer.source != nil {
		return consumer.source.Close()
	}
	return nil
}
