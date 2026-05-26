package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vikagrej/trends/internal/service"
	"github.com/vikagrej/trends/internal/stoplist"
	"github.com/vikagrej/trends/internal/topn"
)

type fakeTopReader struct {
	items     []topn.Item
	windowSec int
	lastLimit int
}

func (reader *fakeTopReader) Top(limit int) topn.Snapshot {
	reader.lastLimit = limit
	items := reader.items
	if items == nil {
		items = []topn.Item{}
	}
	if limit > 0 && limit < len(items) {
		items = items[:limit]
	}
	return topn.Snapshot{
		Items:       items,
		GeneratedAt: time.Now(),
		WindowSec:   reader.windowSec,
	}
}

type fakeStoplistRepo struct {
	words map[string]struct{}
}

func newFakeStoplistRepo(words ...string) *fakeStoplistRepo {
	wordSet := make(map[string]struct{})
	for _, word := range words {
		wordSet[word] = struct{}{}
	}
	return &fakeStoplistRepo{words: wordSet}
}

func (repo *fakeStoplistRepo) List(_ context.Context) ([]string, error) {
	words := make([]string, 0, len(repo.words))
	for word := range repo.words {
		words = append(words, word)
	}
	return words, nil
}

func (repo *fakeStoplistRepo) Add(_ context.Context, word string) error {
	if _, exists := repo.words[word]; exists {
		return stoplist.ErrAlreadyExists
	}
	repo.words[word] = struct{}{}
	return nil
}

func (repo *fakeStoplistRepo) Remove(_ context.Context, word string) error {
	delete(repo.words, word)
	return nil
}

func newTrendsService(items []topn.Item, stopwords ...string) *service.TrendsService {
	trendsService, _ := newTrendsServiceWithReader(items, stopwords...)
	return trendsService
}

func newTrendsServiceWithReader(items []topn.Item, stopwords ...string) (*service.TrendsService, *fakeTopReader) {
	reader := &fakeTopReader{items: items, windowSec: 300}
	repo := newFakeStoplistRepo(stopwords...)
	stoplistService := stoplist.NewService(repo)
	_ = stoplistService.Init(context.Background())
	return service.NewTrendsService(reader, stoplistService), reader
}

func TestGetTop_DefaultLimit(t *testing.T) {
	trendsService, reader := newTrendsServiceWithReader(nil)

	if _, err := trendsService.GetTop(""); err != nil {
		t.Fatalf("GetTop failed: %v", err)
	}

	if reader.lastLimit != 10 {
		t.Errorf("expected default limit 10, got %d", reader.lastLimit)
	}
}

func TestGetTop_ValidLimit(t *testing.T) {
	for _, value := range []string{"1", "100", "1000"} {
		trendsService, reader := newTrendsServiceWithReader(nil)
		_, err := trendsService.GetTop(value)
		if err != nil {
			t.Errorf("GetTop(%q): unexpected error %v", value, err)
			continue
		}
		if reader.lastLimit < 1 || reader.lastLimit > 1000 {
			t.Errorf("GetTop(%q) used limit %d, out of range", value, reader.lastLimit)
		}
	}
}

func TestGetTop_TooLargeLimit(t *testing.T) {
	trendsService := newTrendsService(nil)
	_, err := trendsService.GetTop("9999")
	if !errors.Is(err, service.ErrTopLimitTooLarge) {
		t.Fatalf("expected ErrTopLimitTooLarge, got %v", err)
	}
}

func TestGetTop_NonPositiveLimit(t *testing.T) {
	for _, bad := range []string{"abc", "-1", "0", "1.5", " "} {
		trendsService := newTrendsService(nil)
		_, err := trendsService.GetTop(bad)
		if !errors.Is(err, service.ErrTopLimitNotPositive) {
			t.Errorf("GetTop(%q): expected ErrTopLimitNotPositive, got %v", bad, err)
		}
	}
}

func TestGetTop_ReturnsSnapshot(t *testing.T) {
	items := []topn.Item{
		{Query: "golang", Count: 42},
		{Query: "rust", Count: 17},
	}
	trendsService := newTrendsService(items)

	result, err := trendsService.GetTop("2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Data) != 2 {
		t.Errorf("expected 2 items, got %d", len(result.Data))
	}
	if result.WindowSec != 300 {
		t.Errorf("expected WindowSec=300, got %d", result.WindowSec)
	}
}

func TestGetTop_DefaultN(t *testing.T) {
	trendsService := newTrendsService(nil)
	result, err := trendsService.GetTop("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Data == nil {
		t.Error("Data should not be nil")
	}
}

func TestGetTop_BadN(t *testing.T) {
	trendsService := newTrendsService(nil)
	_, err := trendsService.GetTop("not-a-number")
	if err == nil {
		t.Error("expected error for non-numeric n")
	}
}

func TestListStoplist(t *testing.T) {
	trendsService := newTrendsService(nil, "spam", "ads")
	words := trendsService.ListStoplist()
	if len(words) != 2 {
		t.Errorf("expected 2 words, got %d: %v", len(words), words)
	}
}

func TestAddStopword(t *testing.T) {
	trendsService := newTrendsService(nil)
	normalizedWord, err := trendsService.AddStopword(context.Background(), " SPAM ")
	if err != nil {
		t.Fatalf("AddStopword failed: %v", err)
	}
	if normalizedWord != "spam" {
		t.Fatalf("normalized word=%q, want spam", normalizedWord)
	}
	words := trendsService.ListStoplist()
	if len(words) != 1 || words[0] != "spam" {
		t.Errorf("expected [spam], got %v", words)
	}
}

func TestRemoveStopword(t *testing.T) {
	trendsService := newTrendsService(nil, "spam")
	if err := trendsService.RemoveStopword(context.Background(), "spam"); err != nil {
		t.Fatalf("RemoveStopword failed: %v", err)
	}
	if len(trendsService.ListStoplist()) != 0 {
		t.Error("stoplist should be empty after remove")
	}
}
