package service

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/vikagrej/trends/internal/stoplist"
	"github.com/vikagrej/trends/internal/topn"
)

const (
	defaultTopLimit = 10
	maxTopLimit     = 1000
)

var (
	ErrTopLimitNotPositive = errors.New("parameter 'n' must be a positive integer")
	ErrTopLimitTooLarge    = fmt.Errorf("parameter 'n' must not exceed %d", maxTopLimit)
)

type TopReader interface {
	Top(limit int) topn.Snapshot
}

type TrendsService struct {
	engine   TopReader
	stoplist *stoplist.Service
}

func NewTrendsService(engine TopReader, stoplist *stoplist.Service) *TrendsService {
	return &TrendsService{engine: engine, stoplist: stoplist}
}

type TopResult struct {
	Data      []topn.Item
	Timestamp int64
	WindowSec int
}

func parseTopLimit(rawLimit string) (int, error) {
	limit := defaultTopLimit
	if rawLimit == "" {
		return limit, nil
	}
	parsedLimit, err := strconv.Atoi(rawLimit)
	if err != nil || parsedLimit < 1 {
		return 0, ErrTopLimitNotPositive
	}
	if parsedLimit > maxTopLimit {
		return 0, ErrTopLimitTooLarge
	}
	return parsedLimit, nil
}

func (service *TrendsService) GetTop(rawLimit string) (TopResult, error) {
	limit, err := parseTopLimit(rawLimit)
	if err != nil {
		return TopResult{}, fmt.Errorf("get top: %w", err)
	}

	snapshot := service.engine.Top(limit)

	return TopResult{
		Data:      snapshot.Items,
		Timestamp: snapshot.GeneratedAt.Unix(),
		WindowSec: snapshot.WindowSec,
	}, nil
}

func (service *TrendsService) ListStoplist() []string {
	words := service.stoplist.Snapshot()
	if words == nil {
		return []string{}
	}
	return words
}

func (service *TrendsService) AddStopword(ctx context.Context, word string) (string, error) {
	normalizedWord, err := service.stoplist.Add(ctx, word)
	if err != nil {
		return "", fmt.Errorf("add stopword via API: %w", err)
	}
	return normalizedWord, nil
}

func (service *TrendsService) RemoveStopword(ctx context.Context, word string) error {
	if err := service.stoplist.Remove(ctx, word); err != nil {
		return fmt.Errorf("remove stopword via API: %w", err)
	}
	return nil
}
