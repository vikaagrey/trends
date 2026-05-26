package consumer

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type fakeSink struct {
	calls       []addCall
	returnFalse bool
}

type addCall struct {
	query  string
	source string
	ts     time.Time
}

func (sink *fakeSink) Add(query, source string, ts time.Time) bool {
	sink.calls = append(sink.calls, addCall{query, source, ts})
	return !sink.returnFalse
}

func makeEvent(query, source string, tsMs int64) []byte {
	event := SearchEvent{Query: query, Source: source, TsMs: tsMs}
	eventBytes, _ := json.Marshal(event)
	return eventBytes
}

func TestProcess_ValidEvent(t *testing.T) {
	sink := &fakeSink{}
	consumer := New(Config{Source: nil, Sink: sink})

	now := time.Now().UnixMilli()
	consumer.process(makeEvent("iphone 15", "u1", now))

	if len(sink.calls) != 1 {
		t.Fatalf("expected 1 Add call, got %d", len(sink.calls))
	}
	if sink.calls[0].query != "iphone 15" {
		t.Errorf("unexpected query: %s", sink.calls[0].query)
	}
}

func TestProcess_Normalization(t *testing.T) {
	sink := &fakeSink{}
	consumer := New(Config{Source: nil, Sink: sink})

	now := time.Now().UnixMilli()
	consumer.process(makeEvent("  iPhone  15  ", "u1", now))

	if len(sink.calls) != 1 {
		t.Fatalf("expected 1 Add call, got %d", len(sink.calls))
	}
	if sink.calls[0].query != "iphone 15" {
		t.Errorf("query not normalized: %q", sink.calls[0].query)
	}
}

func TestProcess_InvalidJSON(t *testing.T) {
	sink := &fakeSink{}
	consumer := New(Config{Source: nil, Sink: sink})

	consumer.process([]byte("{invalid json}"))

	if len(sink.calls) != 0 {
		t.Error("Add should not be called for invalid JSON")
	}
	stats := consumer.Stats()
	if stats.Invalid != 1 {
		t.Errorf("expected Invalid=1, got %d", stats.Invalid)
	}
}

func TestProcess_MissingSource(t *testing.T) {
	sink := &fakeSink{}
	consumer := New(Config{Source: nil, Sink: sink})

	event := map[string]any{"query": "golang", "ts_ms": time.Now().UnixMilli()}
	eventBytes, _ := json.Marshal(event)
	consumer.process(eventBytes)

	if len(sink.calls) != 0 {
		t.Error("Add should not be called when source is missing")
	}
	stats := consumer.Stats()
	if stats.Invalid != 1 {
		t.Errorf("expected Invalid=1, got %d", stats.Invalid)
	}
}

func TestProcess_EmptyQuery_AfterNormalize(t *testing.T) {
	sink := &fakeSink{}
	consumer := New(Config{Source: nil, Sink: sink})

	event := SearchEvent{Query: "   ", Source: "u1", TsMs: time.Now().UnixMilli()}
	eventBytes, _ := json.Marshal(event)
	consumer.process(eventBytes)

	if len(sink.calls) != 0 {
		t.Error("Add should not be called for whitespace-only query")
	}
	stats := consumer.Stats()
	if stats.NormDropped != 1 {
		t.Errorf("expected NormDropped=1, got %d", stats.NormDropped)
	}
}

func TestProcess_Stats(t *testing.T) {
	sink := &fakeSink{}
	consumer := New(Config{Source: nil, Sink: sink})

	now := time.Now().UnixMilli()
	consumer.process(makeEvent("go", "u1", now))
	consumer.process([]byte("{bad}"))
	consumer.process(makeEvent("   ", "u1", now))

	stats := consumer.Stats()
	if stats.Consumed != 1 {
		t.Errorf("Consumed=%d, want 1", stats.Consumed)
	}
	if stats.Invalid != 1 {
		t.Errorf("Invalid=%d, want 1", stats.Invalid)
	}
	if stats.NormDropped != 1 {
		t.Errorf("NormDropped=%d, want 1", stats.NormDropped)
	}
}

func TestDecodeEvent_MissingTimestamp(t *testing.T) {
	event := map[string]any{"query": "golang", "source": "u1"}
	eventBytes, _ := json.Marshal(event)
	_, err := decodeEvent(eventBytes)
	if err != ErrNoTimestamp {
		t.Errorf("expected ErrNoTimestamp, got %v", err)
	}
}

func TestProcess_OutOfWindow(t *testing.T) {
	sink := &fakeSink{returnFalse: true}
	consumer := New(Config{Source: nil, Sink: sink})

	now := time.Now().UnixMilli()
	consumer.process(makeEvent("golang", "u1", now))

	stats := consumer.Stats()
	if stats.OutOfWindow != 1 {
		t.Errorf("expected OutOfWindow=1, got %d", stats.OutOfWindow)
	}
	if stats.Consumed != 0 {
		t.Errorf("expected Consumed=0, got %d", stats.Consumed)
	}
}

func TestConsumer_Close(t *testing.T) {
	source := &fakeSource{closed: make(chan struct{})}
	consumer := New(Config{Source: source, Sink: &fakeSink{}})
	if err := consumer.Close(); err != nil {
		t.Errorf("Close() returned error: %v", err)
	}
	select {
	case <-source.closed:
	default:
		t.Error("fakeSource.Close() was not called")
	}
}

func TestConsumer_Close_NilSource(t *testing.T) {
	consumer := New(Config{Source: nil, Sink: &fakeSink{}})
	if err := consumer.Close(); err != nil {
		t.Errorf("Close() with nil source returned error: %v", err)
	}
}

func TestConsumer_RunStopsOnCancelledContext(t *testing.T) {
	source := &fakeSource{closed: make(chan struct{})}
	consumer := New(Config{Source: source, Sink: &fakeSink{}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected nil on context cancel, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

type fakeSource struct {
	closed chan struct{}
}

func (source *fakeSource) Fetch(ctx context.Context) (Message, error) {
	<-ctx.Done()
	return Message{}, ctx.Err()
}
func (source *fakeSource) Commit(_ context.Context, _ Message) error { return nil }
func (source *fakeSource) Close() error                              { close(source.closed); return nil }
