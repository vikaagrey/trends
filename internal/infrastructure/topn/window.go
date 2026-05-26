package topn

import (
	"sync"
	"time"
)

type bucket struct {
	mu sync.Mutex
	// timeBucketID показывает, за какой временной бакет сейчас отвечает слот
	// При переходе окна тот же слот переиспользуется и полностью очищается
	timeBucketID  int64
	queryCounters map[string]uniqueCounter
}

func (bucket *bucket) reset(timeBucketID int64) {
	bucket.timeBucketID = timeBucketID
	bucket.queryCounters = make(map[string]uniqueCounter)
}

type Config struct {
	WindowSize  time.Duration
	BucketCount int
	TopK        int

	newUniqueCounter uniqueCounterFactory
}

func (config *Config) withDefaults() Config {
	withDefaults := *config
	if withDefaults.WindowSize <= 0 {
		withDefaults.WindowSize = 5 * time.Minute
	}
	if withDefaults.BucketCount <= 0 {
		withDefaults.BucketCount = 30
	}
	if withDefaults.TopK <= 0 {
		withDefaults.TopK = 1000
	}
	if withDefaults.newUniqueCounter == nil {
		withDefaults.newUniqueCounter = newHLLCounter
	}
	return withDefaults
}

func (a *Aggregator) timeBucketID(eventTime time.Time) int64 {
	return eventTime.UnixNano() / a.bucketWidthNanos
}

func (a *Aggregator) activeTimeBucketRange() (oldestID, currentID int64) {
	currentID = a.timeBucketID(a.clock())
	oldestID = currentID - int64(a.config.BucketCount-1)
	return oldestID, currentID
}

func (a *Aggregator) timeBucketIndex(timeBucketID int64) int {
	index := int(timeBucketID % int64(a.config.BucketCount))
	if index < 0 {
		index += a.config.BucketCount
	}
	return index
}
