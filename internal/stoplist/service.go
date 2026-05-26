package stoplist

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/vikagrej/trends/internal/metrics"
	"github.com/vikagrej/trends/internal/query"
	"github.com/vikagrej/trends/internal/stoplist/stoplisterr"
	"github.com/vikagrej/trends/internal/topn"
)

var (
	ErrInvalidWord   = stoplisterr.ErrInvalidWord
	ErrAlreadyExists = stoplisterr.ErrAlreadyExists
)

type Repository interface {
	List(ctx context.Context) ([]string, error)
	Add(ctx context.Context, word string) error
	Remove(ctx context.Context, word string) error
}

type Service struct {
	repository Repository
	cache      *Cache
	pubSub     *PubSub
	mu         sync.Mutex
	onError    func(error)
	metrics    *metrics.Registry
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, cache: NewCache(), metrics: metrics.NewNoop()}
}

func (s *Service) SetPubSub(pubSub *PubSub) {
	s.pubSub = pubSub
}

func (s *Service) SetMetrics(metricsRegistry *metrics.Registry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if metricsRegistry == nil {
		metricsRegistry = metrics.NewNoop()
	}
	s.metrics = metricsRegistry
	s.updateMetrics(len(s.cache.Load()))
}

func (s *Service) StartSync(ctx context.Context, onError func(error)) {
	s.mu.Lock()
	s.onError = onError
	s.mu.Unlock()

	if s.pubSub == nil {
		return
	}
	s.pubSub.Subscribe(ctx, func() {
		if err := s.reload(ctx); err != nil {
			if onError != nil {
				onError(fmt.Errorf("reload stoplist via pub/sub: %w", err))
			}
		}
	})
}

func (s *Service) Init(ctx context.Context) error {
	if err := s.reload(ctx); err != nil {
		return fmt.Errorf("load stoplist: %w", err)
	}
	return nil
}

func (s *Service) Filter() topn.Filter {
	cachedWords := s.cache.Load()
	if len(cachedWords) == 0 {
		return nil
	}
	// Rebuild работает с одним снимком стоп-листа: изменения из API попадут
	// в следующий проход, зато текущая пересборка не меняет правила на середине
	return func(query string) bool {
		_, banned := cachedWords[query]
		return banned
	}
}

func (s *Service) Snapshot() []string {
	cachedWords := s.cache.Load()
	words := make([]string, 0, len(cachedWords))
	for word := range cachedWords {
		words = append(words, word)
	}
	sort.Strings(words)
	return words
}

func (s *Service) replace(words []string) {
	nextWords := make(map[string]struct{}, len(words))
	for _, word := range words {
		normalizedWord, err := normalizeWord(word)
		if err == nil {
			nextWords[normalizedWord] = struct{}{}
		}
	}
	s.cache.Store(nextWords)
	s.updateMetrics(len(nextWords))
}

func (s *Service) Add(ctx context.Context, word string) (string, error) {
	normalizedWord, err := normalizeWord(word)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.repository.Add(ctx, normalizedWord); err != nil {
		if errors.Is(err, ErrAlreadyExists) {
			s.reloadBestEffortLocked(ctx)
		}
		return "", fmt.Errorf("add stopword %q: %w", normalizedWord, err)
	}
	if err := s.reloadLocked(ctx); err != nil {
		return "", fmt.Errorf("reload stoplist after adding %q: %w", normalizedWord, err)
	}
	s.publishBestEffortLocked(ctx)
	return normalizedWord, nil
}

func (s *Service) Remove(ctx context.Context, word string) error {
	normalizedWord, err := normalizeWord(word)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.repository.Remove(ctx, normalizedWord); err != nil {
		return fmt.Errorf("remove stopword %q: %w", normalizedWord, err)
	}
	if err := s.reloadLocked(ctx); err != nil {
		return fmt.Errorf("reload stoplist after removing %q: %w", normalizedWord, err)
	}
	s.publishBestEffortLocked(ctx)
	return nil
}

func (s *Service) reload(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reloadLocked(ctx)
}

func (s *Service) reloadLocked(ctx context.Context) error {
	words, err := s.repository.List(ctx)
	if err != nil {
		return err
	}
	s.replace(words)
	return nil
}

func (s *Service) reloadBestEffortLocked(ctx context.Context) {
	if err := s.reloadLocked(ctx); err != nil && s.onError != nil {
		s.onError(fmt.Errorf("reload stoplist after conflict: %w", err))
	}
}

func (s *Service) publishBestEffortLocked(ctx context.Context) {
	if s.pubSub == nil {
		return
	}
	if err := s.pubSub.Publish(ctx); err != nil && s.onError != nil {
		s.onError(fmt.Errorf("publish stoplist update: %w", err))
	}
}

func (s *Service) updateMetrics(size int) {
	s.metrics.StoplistSize.Set(float64(size))
}

func normalizeWord(word string) (string, error) {
	normalizedWord, ok := query.Normalize(word)
	if !ok {
		return "", ErrInvalidWord
	}
	return normalizedWord, nil
}
