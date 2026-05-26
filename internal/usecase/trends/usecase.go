package trends

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/vikagrej/trends/internal/domain/model"
	"github.com/vikagrej/trends/internal/infrastructure/topn"
	stoplistuc "github.com/vikagrej/trends/internal/usecase/stoplist"
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

type UseCase struct {
	engine   TopReader
	stoplist *stoplistuc.Service
}

func NewUseCase(engine TopReader, stoplist *stoplistuc.Service) *UseCase {
	return &UseCase{engine: engine, stoplist: stoplist}
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

func (useCase *UseCase) GetTop(rawLimit string) (model.TopResult, error) {
	limit, err := parseTopLimit(rawLimit)
	if err != nil {
		return model.TopResult{}, fmt.Errorf("get top: %w", err)
	}

	snapshot := useCase.engine.Top(limit)
	items := make([]model.TopItem, len(snapshot.Items))
	for i, item := range snapshot.Items {
		items[i] = model.TopItem{
			Query: item.Query,
			Count: item.Count,
		}
	}

	return model.TopResult{
		Data:      items,
		Timestamp: snapshot.GeneratedAt.Unix(),
		WindowSec: snapshot.WindowSec,
	}, nil
}

func (useCase *UseCase) ListStoplist() []string {
	words := useCase.stoplist.Snapshot()
	if words == nil {
		return []string{}
	}
	return words
}

func (useCase *UseCase) AddStopword(ctx context.Context, word string) (string, error) {
	normalizedWord, err := useCase.stoplist.Add(ctx, word)
	if err != nil {
		return "", fmt.Errorf("add stopword via API: %w", err)
	}
	return normalizedWord, nil
}

func (useCase *UseCase) RemoveStopword(ctx context.Context, word string) error {
	if err := useCase.stoplist.Remove(ctx, word); err != nil {
		return fmt.Errorf("remove stopword via API: %w", err)
	}
	return nil
}
