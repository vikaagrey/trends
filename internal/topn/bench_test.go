package topn

import (
	"fmt"
	"testing"
	"time"
)

func BenchmarkAdd(b *testing.B) {
	aggregator := NewAggregator(Config{
		WindowSize:  5 * time.Minute,
		BucketCount: 30,
		TopK:        1000,
	})
	now := time.Now()
	aggregator.clock = func() time.Time { return now }

	queries := make([]string, 100)
	for i := range queries {
		queries[i] = fmt.Sprintf("query-%d", i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregator.Add(queries[i%100], fmt.Sprintf("u%d", i%10_000), now)
	}
}

func BenchmarkTop(b *testing.B) {
	aggregator := NewAggregator(Config{
		WindowSize:  5 * time.Minute,
		BucketCount: 30,
		TopK:        1000,
	})
	now := time.Now()
	aggregator.clock = func() time.Time { return now }
	for i := 0; i < 500; i++ {
		aggregator.Add(fmt.Sprintf("q%d", i), "u1", now)
	}
	aggregator.Rebuild(nil)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = aggregator.Top(10)
	}
}

func BenchmarkRebuild(b *testing.B) {
	aggregator := NewAggregator(Config{
		WindowSize:  5 * time.Minute,
		BucketCount: 30,
		TopK:        1000,
	})
	now := time.Now()
	aggregator.clock = func() time.Time { return now }
	for i := 0; i < 1000; i++ {
		aggregator.Add(fmt.Sprintf("q%d", i), fmt.Sprintf("u%d", i), now)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregator.Rebuild(nil)
	}
}

func BenchmarkAdd_Parallel(b *testing.B) {
	aggregator := NewAggregator(Config{
		WindowSize:  5 * time.Minute,
		BucketCount: 30,
		TopK:        1000,
	})
	now := time.Now()
	aggregator.clock = func() time.Time { return now }
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			aggregator.Add(fmt.Sprintf("q%d", i%100), fmt.Sprintf("u%d", i%10_000), now)
			i++
		}
	})
}
