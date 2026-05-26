package consumer_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/vikagrej/trends/internal/consumer"
)

type fetchStep struct {
	message consumer.Message
	err     error
}

type scriptSource struct {
	steps      []fetchStep
	commits    []consumer.Message
	commitErrs []error
}

func (source *scriptSource) Fetch(context.Context) (consumer.Message, error) {
	if len(source.steps) == 0 {
		return consumer.Message{}, context.Canceled
	}
	step := source.steps[0]
	source.steps = source.steps[1:]
	return step.message, step.err
}

func (source *scriptSource) Commit(_ context.Context, message consumer.Message) error {
	source.commits = append(source.commits, message)
	if len(source.commitErrs) == 0 {
		return nil
	}
	err := source.commitErrs[0]
	source.commitErrs = source.commitErrs[1:]
	return err
}

func (source *scriptSource) Close() error { return nil }

type runSink struct {
	calls  []runSinkCall
	reject bool
}

type runSinkCall struct {
	query  string
	source string
	ts     time.Time
}

func (sink *runSink) Add(query, source string, ts time.Time) bool {
	sink.calls = append(sink.calls, runSinkCall{query: query, source: source, ts: ts})
	return !sink.reject
}

func msgBytes(query, source string, tsMs int64) []byte {
	eventBytes, _ := json.Marshal(consumer.SearchEvent{Query: query, Source: source, TsMs: tsMs})
	return eventBytes
}

func assertCommitted(t *testing.T, source *scriptSource, want ...consumer.Message) {
	t.Helper()
	if len(source.commits) != len(want) {
		t.Fatalf("commits=%d, want %d", len(source.commits), len(want))
	}
	for i := range want {
		if string(source.commits[i].Value) != string(want[i].Value) {
			t.Fatalf("commit[%d]=%q, want %q", i, source.commits[i].Value, want[i].Value)
		}
	}
}

func assertNoPendingFetches(t *testing.T, source *scriptSource) {
	t.Helper()
	if len(source.steps) != 0 {
		t.Fatalf("source has %d unconsumed fetch steps", len(source.steps))
	}
}

func TestRun_CommitsEveryMessage(t *testing.T) {
	now := time.Now().UnixMilli()
	validMsg := consumer.NewMessage(msgBytes("golang", "u1", now), nil)
	badMsg := consumer.NewMessage([]byte("{invalid}"), nil)

	source := &scriptSource{steps: []fetchStep{
		{message: validMsg},
		{message: badMsg},
		{err: context.Canceled},
	}}
	sink := &runSink{}

	consumerInstance := consumer.New(consumer.Config{Source: source, Sink: sink})
	if err := consumerInstance.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	assertNoPendingFetches(t, source)
	assertCommitted(t, source, validMsg, badMsg)
	if len(sink.calls) != 1 {
		t.Fatalf("Sink.Add calls=%d, want 1", len(sink.calls))
	}
	if sink.calls[0].query != "golang" || sink.calls[0].source != "u1" {
		t.Fatalf("Sink.Add call = %+v, want query=golang source=u1", sink.calls[0])
	}
	if sink.calls[0].ts.UnixMilli() != now {
		t.Fatalf("Sink.Add timestamp=%d, want %d", sink.calls[0].ts.UnixMilli(), now)
	}
}

func TestRun_CommitError_PropagatesAndStops(t *testing.T) {
	commitErr := errors.New("broker unavailable")
	now := time.Now().UnixMilli()
	message := consumer.NewMessage(msgBytes("rust", "u2", now), nil)

	source := &scriptSource{
		steps:      []fetchStep{{message: message}},
		commitErrs: []error{commitErr},
	}
	sink := &runSink{}

	consumerInstance := consumer.New(consumer.Config{Source: source, Sink: sink})
	err := consumerInstance.Run(context.Background())
	if !errors.Is(err, commitErr) {
		t.Fatalf("Run() error = %v, want %v", err, commitErr)
	}

	assertCommitted(t, source, message)
	if len(sink.calls) != 1 {
		t.Fatalf("Sink.Add calls=%d, want 1", len(sink.calls))
	}
}

func TestRun_FetchError_Propagates(t *testing.T) {
	fetchErr := errors.New("kafka partition error")
	source := &scriptSource{steps: []fetchStep{{err: fetchErr}}}
	sink := &runSink{}

	consumerInstance := consumer.New(consumer.Config{Source: source, Sink: sink})
	err := consumerInstance.Run(context.Background())
	if !errors.Is(err, fetchErr) {
		t.Fatalf("Run() error = %v, want %v", err, fetchErr)
	}
	if len(sink.calls) != 0 {
		t.Fatalf("Sink.Add calls=%d, want 0", len(sink.calls))
	}
	assertCommitted(t, source)
}

func TestRun_SinkReturnsOutOfWindow(t *testing.T) {
	now := time.Now().UnixMilli()
	message := consumer.NewMessage(msgBytes("old-query", "u3", now), nil)

	source := &scriptSource{steps: []fetchStep{
		{message: message},
		{err: context.Canceled},
	}}
	sink := &runSink{reject: true}

	consumerInstance := consumer.New(consumer.Config{Source: source, Sink: sink})
	if err := consumerInstance.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	assertNoPendingFetches(t, source)
	assertCommitted(t, source, message)
	if got := consumerInstance.Stats().OutOfWindow; got != 1 {
		t.Fatalf("OutOfWindow=%d, want 1", got)
	}
}

func TestRun_NormalizationDroppedMessage(t *testing.T) {
	now := time.Now().UnixMilli()
	message := consumer.NewMessage(msgBytes("   ", "u4", now), nil)

	source := &scriptSource{steps: []fetchStep{
		{message: message},
		{err: context.Canceled},
	}}
	sink := &runSink{}

	consumerInstance := consumer.New(consumer.Config{Source: source, Sink: sink})
	if err := consumerInstance.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v, want nil", err)
	}

	assertNoPendingFetches(t, source)
	assertCommitted(t, source, message)
	if len(sink.calls) != 0 {
		t.Fatalf("Sink.Add calls=%d, want 0", len(sink.calls))
	}
	if got := consumerInstance.Stats().NormDropped; got != 1 {
		t.Fatalf("NormDropped=%d, want 1", got)
	}
}
