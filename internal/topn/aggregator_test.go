package topn

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// Тесты используют точный счётчик, чтобы не зависеть от погрешности HLL.
func exactConfig(window time.Duration, bucketCount, topK int) Config {
	return Config{
		WindowSize:       window,
		BucketCount:      bucketCount,
		TopK:             topK,
		newUniqueCounter: newExactCounter,
	}
}

func fixedNow(aggregator *Aggregator, now time.Time) {
	aggregator.clock = func() time.Time { return now }
}

func TestAdd_InWindow(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	if !aggregator.Add("golang", "u1", now) {
		t.Fatal("expected Add to return true for event within window")
	}
}

func TestAdd_OldEvent_Rejected(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	old := now.Add(-10 * time.Minute)
	if aggregator.Add("golang", "u1", old) {
		t.Fatal("expected Add to return false for event older than window")
	}
}

func TestAdd_FutureEvent_Rejected(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	future := now.Add(10 * time.Minute)
	if aggregator.Add("golang", "u1", future) {
		t.Fatal("expected Add to return false for event from the future")
	}
}

func TestRebuild_TopOrder(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	for i := 0; i < 3; i++ {
		aggregator.Add("rust", fmt.Sprintf("u%d", i), now)
	}
	for i := 0; i < 5; i++ {
		aggregator.Add("python", fmt.Sprintf("u%d", i), now)
	}
	aggregator.Add("go", "u0", now)

	aggregator.Rebuild(nil)
	snapshot := aggregator.Top(10)

	if len(snapshot.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(snapshot.Items))
	}
	if snapshot.Items[0].Query != "python" || snapshot.Items[0].Count != 5 {
		t.Errorf("expected python/5 at position 0, got %+v", snapshot.Items[0])
	}
	if snapshot.Items[1].Query != "rust" || snapshot.Items[1].Count != 3 {
		t.Errorf("expected rust/3 at position 1, got %+v", snapshot.Items[1])
	}
	if snapshot.Items[2].Query != "go" || snapshot.Items[2].Count != 1 {
		t.Errorf("expected go/1 at position 2, got %+v", snapshot.Items[2])
	}
}

func TestRebuild_UniqueSource_Dedup(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	for i := 0; i < 100; i++ {
		aggregator.Add("iphone", "bot", now)
	}
	aggregator.Add("iphone", "u1", now)
	aggregator.Add("iphone", "u2", now)

	aggregator.Rebuild(nil)
	snapshot := aggregator.Top(1)

	if len(snapshot.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(snapshot.Items))
	}
	if snapshot.Items[0].Count != 3 {
		t.Errorf("expected count=3 (bot+u1+u2 unique), got %d", snapshot.Items[0].Count)
	}
}

func TestRebuild_WithFilter(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	aggregator.Add("spam", "u1", now)
	aggregator.Add("golang", "u1", now)

	stoplist := map[string]struct{}{"spam": {}}
	filter := func(q string) bool {
		_, banned := stoplist[q]
		return banned
	}

	aggregator.Rebuild(filter)
	snapshot := aggregator.Top(10)

	for _, item := range snapshot.Items {
		if item.Query == "spam" {
			t.Error("spam should have been filtered out")
		}
	}
	if len(snapshot.Items) != 1 || snapshot.Items[0].Query != "golang" {
		t.Errorf("expected only golang in results, got %+v", snapshot.Items)
	}
}

func TestBucketRotation_OldBucketDropped(t *testing.T) {
	aggregator := NewAggregator(exactConfig(30*time.Second, 3, 100))
	base := time.Unix(1_700_000_000, 0)

	aggregator.clock = func() time.Time { return base }
	aggregator.Add("old-query", "u1", base)

	later := base.Add(40 * time.Second)
	aggregator.clock = func() time.Time { return later }
	aggregator.Add("new-query", "u2", later)

	aggregator.Rebuild(nil)
	snapshot := aggregator.Top(10)

	for _, item := range snapshot.Items {
		if item.Query == "old-query" {
			t.Error("old-query should have rotated out of the window")
		}
	}
}

func TestTop_LimitN(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	for i := 0; i < 10; i++ {
		aggregator.Add(fmt.Sprintf("query-%d", i), "u1", now)
	}
	aggregator.Rebuild(nil)

	snapshot := aggregator.Top(3)
	if len(snapshot.Items) != 3 {
		t.Errorf("Top(3) returned %d items, want 3", len(snapshot.Items))
	}
}

func TestTop_ZeroAndNegative(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)
	aggregator.Add("q", "u", now)
	aggregator.Rebuild(nil)

	if len(aggregator.Top(0).Items) != 1 {
		t.Error("Top(0) should return all items")
	}
	if len(aggregator.Top(-5).Items) != 1 {
		t.Error("Top(-5) should return all items")
	}
}

func TestConcurrentAdd_RaceDetector(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	now := time.Now()
	fixedNow(aggregator, now)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			aggregator.Add("golang", fmt.Sprintf("u%d", n), now)
			aggregator.Rebuild(nil)
			aggregator.Top(10)
		}(i)
	}
	wg.Wait()
}

func TestWindowSec_InSnapshot(t *testing.T) {
	aggregator := NewAggregator(exactConfig(5*time.Minute, 30, 100))
	aggregator.Rebuild(nil)
	snapshot := aggregator.Top(10)
	if snapshot.WindowSec != 300 {
		t.Errorf("expected WindowSec=300, got %d", snapshot.WindowSec)
	}
}
