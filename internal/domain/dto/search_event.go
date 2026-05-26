package dto

import "time"

type SearchEvent struct {
	Query   string `json:"query"`
	Source  string `json:"source"`
	TsMs    int64  `json:"ts_ms"`
	EventID string `json:"event_id,omitempty"`
}

func (event *SearchEvent) Timestamp() time.Time {
	return time.UnixMilli(event.TsMs)
}
