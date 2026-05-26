package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type SearchEvent struct {
	Query   string `json:"query"`
	Source  string `json:"source"`
	TsMs    int64  `json:"ts_ms"`
	EventID string `json:"event_id,omitempty"`
}

func (event *SearchEvent) Timestamp() time.Time {
	return time.UnixMilli(event.TsMs)
}

var (
	ErrEmptyQuery  = errors.New("consumer: empty query")
	ErrNoSource    = errors.New("consumer: missing source")
	ErrNoTimestamp = errors.New("consumer: missing or invalid ts_ms")
)

func decodeEvent(raw []byte) (*SearchEvent, error) {
	var event SearchEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("decode JSON event: %w", err)
	}
	if event.Query == "" {
		return nil, ErrEmptyQuery
	}
	if event.Source == "" {
		return nil, ErrNoSource
	}
	if event.TsMs <= 0 {
		return nil, ErrNoTimestamp
	}
	return &event, nil
}
