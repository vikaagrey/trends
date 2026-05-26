package kafka

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vikagrej/trends/internal/domain/dto"
)

var (
	ErrEmptyQuery  = errors.New("consumer: empty query")
	ErrNoSource    = errors.New("consumer: missing source")
	ErrNoTimestamp = errors.New("consumer: missing or invalid ts_ms")
)

func decodeEvent(raw []byte) (*dto.SearchEvent, error) {
	var event dto.SearchEvent
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
