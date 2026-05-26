package consumer

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

func (m Message) Handle() any { return m.handle }

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

func (c *Consumer) Run(ctx context.Context) error {
	for {
		message, err := c.source.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		c.process(message.Value)

		if err := c.source.Commit(ctx, message); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("commit message: %w", err)
		}
	}
}

func (c *Consumer) process(raw []byte) {
	event, err := decodeEvent(raw)
	if err != nil {
		c.stats.Invalid.Add(1)
		c.metricsRegistry.EventsInvalid.Inc()
		return
	}
	c.stats.Decoded.Add(1)
	c.metricsRegistry.EventsDecoded.Inc()

	q, ok := query.Normalize(event.Query)
	if !ok {
		c.stats.NormDropped.Add(1)
		c.metricsRegistry.EventsNormDrop.Inc()
		return
	}

	if c.sink.Add(q, event.Source, event.Timestamp()) {
		c.stats.Consumed.Add(1)
		c.metricsRegistry.EventsConsumed.Inc()
	} else {
		c.stats.OutOfWindow.Add(1)
		c.metricsRegistry.EventsOutWindow.Inc()
	}
}

func (c *Consumer) Stats() Stats {
	return Stats{
		Consumed:    c.stats.Consumed.Load(),
		Decoded:     c.stats.Decoded.Load(),
		Invalid:     c.stats.Invalid.Load(),
		NormDropped: c.stats.NormDropped.Load(),
		OutOfWindow: c.stats.OutOfWindow.Load(),
	}
}

func (c *Consumer) Close() error {
	if c.source != nil {
		return c.source.Close()
	}
	return nil
}
