package topn

import (
	"sort"
	"time"
)

type Item struct {
	Query string `json:"query"`
	Count uint64 `json:"count"`
}

type Snapshot struct {
	Items       []Item
	GeneratedAt time.Time
	WindowSec   int
}

type Filter func(query string) bool

func (a *Aggregator) emptySnapshot(at time.Time) *Snapshot {
	return &Snapshot{
		Items:       []Item{},
		GeneratedAt: at,
		WindowSec:   int(a.config.WindowSize.Seconds()),
	}
}

func (a *Aggregator) publishSnapshot(items []Item, at time.Time) {
	// GET /top читает только этот готовый снапшот, поэтому запрос не ждёт rebuild
	// и не трогает бакеты, куда параллельно пишет consumer
	a.snapshot.Store(&Snapshot{
		Items:       items,
		GeneratedAt: at,
		WindowSec:   int(a.config.WindowSize.Seconds()),
	})
}

func buildTopItems(queryCounters map[string]uniqueCounter, filter Filter, topK int) []Item {
	if filter != nil {
		for query := range queryCounters {
			if filter(query) {
				delete(queryCounters, query)
			}
		}
	}

	items := make([]Item, 0, len(queryCounters))
	for query, counter := range queryCounters {
		items = append(items, Item{Query: query, Count: counter.Estimate()})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Count != items[j].Count {
			return items[i].Count > items[j].Count
		}
		return items[i].Query < items[j].Query
	})

	if len(items) > topK {
		items = items[:topK]
	}
	return items
}

func (a *Aggregator) Top(limit int) Snapshot {
	snapshot := a.snapshot.Load()
	if limit <= 0 || limit >= len(snapshot.Items) {
		return *snapshot
	}
	limitedSnapshot := *snapshot
	limitedSnapshot.Items = snapshot.Items[:limit]
	return limitedSnapshot
}
