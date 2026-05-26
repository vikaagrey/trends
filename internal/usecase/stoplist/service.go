package stoplist

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/vikagrej/trends/internal/infrastructure/topn"
	"github.com/vikagrej/trends/internal/metrics"
	"github.com/vikagrej/trends/internal/query"
	stoplistrepo "github.com/vikagrej/trends/internal/repository/stoplist"
)

var (
	ErrInvalidWord   = errors.New("invalid stopword")
	ErrAlreadyExists = stoplistrepo.ErrAlreadyExists
)

type Repository = stoplistrepo.Repository

type Service struct {
	repository Repository
	cache      *Cache
	pubSub     stoplistrepo.PubSub
	mu         sync.Mutex
	onError    func(error)
	metrics    *metrics.Registry
}

func NewService(repository Repository) *Service {
	return &Service{
		repository: repository,
		cache:      NewCache(),
		metrics:    metrics.NewNoop(),
	}
}

func (service *Service) SetPubSub(pubSub stoplistrepo.PubSub) {
	service.pubSub = pubSub
}

func (service *Service) SetMetrics(metricsRegistry *metrics.Registry) {
	service.mu.Lock()
	defer service.mu.Unlock()

	if metricsRegistry == nil {
		metricsRegistry = metrics.NewNoop()
	}
	service.metrics = metricsRegistry
	service.updateMetrics(len(service.cache.Load()))
}

func (service *Service) StartSync(ctx context.Context, onError func(error)) {
	service.mu.Lock()
	service.onError = onError
	service.mu.Unlock()

	if service.pubSub == nil {
		return
	}
	service.pubSub.Subscribe(ctx, func() {
		if err := service.reload(ctx); err != nil && onError != nil {
			onError(fmt.Errorf("reload stoplist via pub/sub: %w", err))
		}
	})
}

func (service *Service) Init(ctx context.Context) error {
	if err := service.reload(ctx); err != nil {
		return fmt.Errorf("load stoplist: %w", err)
	}
	return nil
}

func (service *Service) Filter() topn.Filter {
	cachedWords := service.cache.Load()
	if len(cachedWords) == 0 {
		return nil
	}

	return func(query string) bool {
		_, banned := cachedWords[query]
		return banned
	}
}

func (service *Service) Snapshot() []string {
	cachedWords := service.cache.Load()
	words := make([]string, 0, len(cachedWords))
	for word := range cachedWords {
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

func (service *Service) Add(ctx context.Context, word string) (string, error) {
	normalizedWord, err := normalizeWord(word)
	if err != nil {
		return "", err
	}

	service.mu.Lock()
	defer service.mu.Unlock()

	if err := service.repository.Add(ctx, normalizedWord); err != nil {
		if errors.Is(err, stoplistrepo.ErrAlreadyExists) {
			service.reloadBestEffortLocked(ctx)
		}
		return "", fmt.Errorf("add stopword %q: %w", normalizedWord, err)
	}
	if err := service.reloadLocked(ctx); err != nil {
		return "", fmt.Errorf("reload stoplist after adding %q: %w", normalizedWord, err)
	}
	service.publishBestEffortLocked(ctx)
	return normalizedWord, nil
}

func (service *Service) Remove(ctx context.Context, word string) error {
	normalizedWord, err := normalizeWord(word)
	if err != nil {
		return err
	}

	service.mu.Lock()
	defer service.mu.Unlock()

	if err := service.repository.Remove(ctx, normalizedWord); err != nil {
		return fmt.Errorf("remove stopword %q: %w", normalizedWord, err)
	}
	if err := service.reloadLocked(ctx); err != nil {
		return fmt.Errorf("reload stoplist after removing %q: %w", normalizedWord, err)
	}
	service.publishBestEffortLocked(ctx)
	return nil
}

func (service *Service) reload(ctx context.Context) error {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.reloadLocked(ctx)
}

func (service *Service) reloadLocked(ctx context.Context) error {
	words, err := service.repository.List(ctx)
	if err != nil {
		return err
	}

	nextWords := make(map[string]struct{}, len(words))
	for _, word := range words {
		normalizedWord, normalizeErr := normalizeWord(word)
		if normalizeErr == nil {
			nextWords[normalizedWord] = struct{}{}
		}
	}
	service.cache.Store(nextWords)
	service.updateMetrics(len(nextWords))
	return nil
}

func (service *Service) reloadBestEffortLocked(ctx context.Context) {
	if err := service.reloadLocked(ctx); err != nil && service.onError != nil {
		service.onError(fmt.Errorf("reload stoplist after conflict: %w", err))
	}
}

func (service *Service) publishBestEffortLocked(ctx context.Context) {
	if service.pubSub == nil {
		return
	}
	if err := service.pubSub.Publish(ctx); err != nil && service.onError != nil {
		service.onError(fmt.Errorf("publish stoplist update: %w", err))
	}
}

func (service *Service) updateMetrics(size int) {
	service.metrics.StoplistSize.Set(float64(size))
}

func normalizeWord(word string) (string, error) {
	normalizedWord, ok := query.Normalize(word)
	if !ok {
		return "", ErrInvalidWord
	}
	return normalizedWord, nil
}
