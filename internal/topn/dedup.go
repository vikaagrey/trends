package topn

import "github.com/axiomhq/hyperloglog"

type uniqueCounter interface {
	Add(source string)
	Estimate() uint64
	Merge(other uniqueCounter)
	Clone() uniqueCounter
}

type uniqueCounterFactory func() uniqueCounter

type hllCounter struct {
	sketch *hyperloglog.Sketch
}

func newHLLCounter() uniqueCounter {
	return &hllCounter{sketch: hyperloglog.New()}
}

func (counter *hllCounter) Add(source string) {
	counter.sketch.Insert([]byte(source))
}

func (counter *hllCounter) Estimate() uint64 {
	return counter.sketch.Estimate()
}

func (counter *hllCounter) Merge(other uniqueCounter) {
	otherCounter, ok := other.(*hllCounter)
	if !ok {
		return
	}
	_ = counter.sketch.Merge(otherCounter.sketch)
}

func (counter *hllCounter) Clone() uniqueCounter {
	return &hllCounter{sketch: counter.sketch.Clone()}
}

type exactCounter struct {
	sources map[string]struct{}
}

func newExactCounter() uniqueCounter {
	return &exactCounter{sources: make(map[string]struct{})}
}

func (counter *exactCounter) Add(source string) {
	counter.sources[source] = struct{}{}
}

func (counter *exactCounter) Estimate() uint64 {
	return uint64(len(counter.sources))
}

func (counter *exactCounter) Merge(other uniqueCounter) {
	otherCounter, ok := other.(*exactCounter)
	if !ok {
		return
	}
	for source := range otherCounter.sources {
		counter.sources[source] = struct{}{}
	}
}

func (counter *exactCounter) Clone() uniqueCounter {
	sourcesCopy := make(map[string]struct{}, len(counter.sources))
	for source := range counter.sources {
		sourcesCopy[source] = struct{}{}
	}
	return &exactCounter{sources: sourcesCopy}
}
