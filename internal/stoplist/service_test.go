package stoplist

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/vikagrej/trends/internal/metrics"
	"github.com/vikagrej/trends/internal/stoplist/stoplisttest"
)

func TestService_Init(t *testing.T) {
	repo := stoplisttest.NewRepository("spam", "ads")
	svc := NewService(repo)

	if err := svc.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	words := svc.Snapshot()
	if len(words) != 2 {
		t.Errorf("expected 2 words after Init, got %d: %v", len(words), words)
	}
}

func TestService_Init_NormalizesWords(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository(" СПАМ  слово "))

	if err := svc.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}

	words := svc.Snapshot()
	if len(words) != 1 || words[0] != "спам слово" {
		t.Errorf("expected normalized stoplist, got %v", words)
	}
}

func TestService_Add(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository())
	_ = svc.Init(context.Background())

	if _, err := svc.Add(context.Background(), "spam"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	words := svc.Snapshot()
	if len(words) != 1 || words[0] != "spam" {
		t.Errorf("expected [spam], got %v", words)
	}
}

func TestService_Add_NormalizesWord(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository())
	_ = svc.Init(context.Background())

	normalizedWord, err := svc.Add(context.Background(), " СПАМ  слово ")
	if err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if normalizedWord != "спам слово" {
		t.Fatalf("normalized word=%q, want спам слово", normalizedWord)
	}

	words := svc.Snapshot()
	if len(words) != 1 || words[0] != "спам слово" {
		t.Errorf("expected [спам слово], got %v", words)
	}
}

func TestService_Add_AlreadyExists(t *testing.T) {
	repo := stoplisttest.NewRepository("spam")
	svc := NewService(repo)
	_ = svc.Init(context.Background())

	_, err := svc.Add(context.Background(), " SPAM ")

	if !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestService_Add_ConcurrentAddsPreserveCache(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository())
	_ = svc.Init(context.Background())

	var wg sync.WaitGroup
	for _, word := range []string{"spam", "ads"} {
		wg.Add(1)
		go func(word string) {
			defer wg.Done()
			if _, err := svc.Add(context.Background(), word); err != nil {
				t.Errorf("Add(%q) failed: %v", word, err)
			}
		}(word)
	}
	wg.Wait()

	words := svc.Snapshot()
	if !stoplisttest.ContainsWord(words, "spam") || !stoplisttest.ContainsWord(words, "ads") {
		t.Fatalf("expected both words in cache, got %v", words)
	}
}

func TestService_Add_InvalidWord(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository())
	_ = svc.Init(context.Background())

	_, err := svc.Add(context.Background(), "   ")

	if !errors.Is(err, ErrInvalidWord) {
		t.Fatalf("expected ErrInvalidWord, got %v", err)
	}
}

func TestService_Remove(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository("spam", "ads"))
	_ = svc.Init(context.Background())

	if err := svc.Remove(context.Background(), "spam"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	for _, w := range svc.Snapshot() {
		if w == "spam" {
			t.Error("spam should have been removed")
		}
	}
}

func TestService_Remove_NormalizesWord(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository("спам слово"))
	_ = svc.Init(context.Background())

	if err := svc.Remove(context.Background(), " СПАМ  слово "); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	if stoplisttest.ContainsWord(svc.Snapshot(), "спам слово") {
		t.Error("спам слово should have been removed")
	}
}

func TestService_Remove_MissingWordIsIdempotent(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository())
	_ = svc.Init(context.Background())

	if err := svc.Remove(context.Background(), "missing"); err != nil {
		t.Fatalf("Remove missing word failed: %v", err)
	}
}

func TestService_UpdatesStoplistSizeMetric(t *testing.T) {
	registry := metrics.NewRegistry()
	svc := NewService(stoplisttest.NewRepository("spam"))
	svc.SetMetrics(registry)

	if err := svc.Init(context.Background()); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	assertStoplistMetric(t, registry, 1)

	if _, err := svc.Add(context.Background(), "ads"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	assertStoplistMetric(t, registry, 2)

	if err := svc.Remove(context.Background(), "spam"); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	assertStoplistMetric(t, registry, 1)
}

func TestService_Filter_Blocks(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository("spam"))
	_ = svc.Init(context.Background())

	f := svc.Filter()
	if f == nil {
		t.Fatal("Filter should not be nil when stoplist is non-empty")
	}
	if !f("spam") {
		t.Error("filter should block 'spam'")
	}
	if f("golang") {
		t.Error("filter should not block 'golang'")
	}
}

func TestService_Filter_EmptyStoplist(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository())
	_ = svc.Init(context.Background())

	f := svc.Filter()
	if f != nil {
		t.Error("Filter should be nil when stoplist is empty")
	}
}

func TestCache_CopyOnWrite_NoRace(t *testing.T) {
	svc := NewService(stoplisttest.NewRepository("a"))
	_ = svc.Init(context.Background())

	snap1 := svc.cache.Load()
	_, _ = svc.Add(context.Background(), "b")

	if _, ok := snap1["b"]; ok {
		t.Error("Add mutated a previously loaded snapshot (copy-on-write violated)")
	}
}

func assertStoplistMetric(t *testing.T, registry *metrics.Registry, want int) {
	t.Helper()

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	metrics.HTTPHandler(registry).ServeHTTP(response, request)

	needle := fmt.Sprintf("trends_stoplist_words_total %d", want)
	if !strings.Contains(response.Body.String(), needle) {
		t.Fatalf("expected metric %q, got:\n%s", needle, response.Body.String())
	}
}
