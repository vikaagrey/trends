package topn

import (
	"sync/atomic"
	"time"
)

type Aggregator struct {
	config           Config
	bucketWidthNanos int64

	timeBuckets []*bucket
	snapshot    atomic.Pointer[Snapshot]

	clock func() time.Time
}

func NewAggregator(config Config) *Aggregator {
	configWithDefaults := config.withDefaults()
	a := &Aggregator{
		config:           configWithDefaults,
		bucketWidthNanos: configWithDefaults.WindowSize.Nanoseconds() / int64(configWithDefaults.BucketCount),
		timeBuckets:      make([]*bucket, configWithDefaults.BucketCount),
		clock:            time.Now,
	}
	for index := range a.timeBuckets {
		a.timeBuckets[index] = &bucket{timeBucketID: -1}
	}
	a.snapshot.Store(a.emptySnapshot(a.clock()))
	return a
}

func (a *Aggregator) Add(query, source string, eventTime time.Time) bool {
	timeBucketID := a.timeBucketID(eventTime)
	oldestID, currentID := a.activeTimeBucketRange()
	if timeBucketID < oldestID || timeBucketID > currentID {
		return false
	}

	bucket := a.timeBuckets[a.timeBucketIndex(timeBucketID)]
	bucket.mu.Lock()
	if bucket.timeBucketID != timeBucketID {
		bucket.reset(timeBucketID)
	}
	counter, ok := bucket.queryCounters[query]
	if !ok {
		counter = a.config.newUniqueCounter()
		bucket.queryCounters[query] = counter
	}
	counter.Add(source)
	bucket.mu.Unlock()
	return true
}

func (a *Aggregator) Rebuild(filter Filter) {
	now := a.clock()
	oldestID, currentID := a.activeTimeBucketRange()

	mergedCounters := make(map[string]uniqueCounter)
	for _, bucket := range a.timeBuckets {
		bucket.mu.Lock()
		if bucket.timeBucketID < oldestID || bucket.timeBucketID > currentID {
			bucket.mu.Unlock()
			continue
		}
		for query, counter := range bucket.queryCounters {
			if mergedCounter, ok := mergedCounters[query]; ok {
				mergedCounter.Merge(counter)
			} else {
				mergedCounters[query] = counter.Clone()
			}
		}
		bucket.mu.Unlock()
	}

	a.publishSnapshot(buildTopItems(mergedCounters, filter, a.config.TopK), now)
}
