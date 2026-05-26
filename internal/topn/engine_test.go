package topn

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestEngine_StartStop(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	engine := NewEngine(EngineConfig{
		Aggregator:      aggregator,
		RebuildInterval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)

	engine.Start(ctx)

	time.Sleep(120 * time.Millisecond)

	cancel()
	engine.Stop()
	engine.Stop()
}

func TestEngine_StopBeforeStart(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	engine := NewEngine(EngineConfig{Aggregator: aggregator})

	done := make(chan struct{})
	go func() {
		engine.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop blocked before Start")
	}
}

func TestEngine_RebuildNow(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	aggregator.clock = func() time.Time { return now }

	engine := NewEngine(EngineConfig{
		Aggregator:      aggregator,
		RebuildInterval: time.Hour,
	})

	aggregator.Add("go", "u1", now)
	engine.RebuildNow()

	snapshot := engine.Top(10)
	if len(snapshot.Items) != 1 || snapshot.Items[0].Query != "go" {
		t.Errorf("unexpected snapshot: %+v", snapshot.Items)
	}
}

func TestEngine_FilterProvider(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	aggregator.clock = func() time.Time { return now }

	var filterCalls atomic.Int32
	engine := NewEngine(EngineConfig{
		Aggregator:      aggregator,
		RebuildInterval: time.Hour,
		FilterProvider: func() Filter {
			filterCalls.Add(1)
			return func(q string) bool { return q == "spam" }
		},
	})

	aggregator.Add("spam", "u1", now)
	aggregator.Add("golang", "u1", now)
	engine.RebuildNow()

	if filterCalls.Load() == 0 {
		t.Error("FilterProvider was not called during RebuildNow")
	}

	snapshot := engine.Top(10)
	for _, item := range snapshot.Items {
		if item.Query == "spam" {
			t.Error("'spam' should have been filtered by FilterProvider")
		}
	}
}

func TestEngine_Add(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	aggregator.clock = func() time.Time { return now }

	engine := NewEngine(EngineConfig{Aggregator: aggregator, RebuildInterval: time.Hour})

	if !engine.Add("go", "u1", now) {
		t.Error("Add within window should return true")
	}
	if engine.Add("go", "u1", now.Add(-10*time.Minute)) {
		t.Error("Add outside window should return false")
	}
}

func TestEngine_RebuildLoop(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	aggregator.clock = func() time.Time { return now }
	aggregator.Add("query", "u1", now)

	var rebuilds atomic.Int32
	engine := NewEngine(EngineConfig{
		Aggregator:      aggregator,
		RebuildInterval: 20 * time.Millisecond,
		FilterProvider: func() Filter {
			rebuilds.Add(1)
			return nil
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	engine.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()
	engine.Stop()

	if rebuilds.Load() < 3 {
		t.Errorf("expected at least 3 rebuilds in 200ms with 20ms interval, got %d", rebuilds.Load())
	}
}
