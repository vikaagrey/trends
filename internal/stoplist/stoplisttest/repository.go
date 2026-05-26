package stoplisttest

import (
	"context"
	"sync"

	"github.com/vikagrej/trends/internal/stoplist/stoplisterr"
)

type Repository struct {
	mu           sync.Mutex
	words        map[string]struct{}
	ListErr      error
	AddErr       error
	RemoveErr    error
	DuplicateErr error
}

func NewRepository(words ...string) *Repository {
	wordSet := make(map[string]struct{}, len(words))
	for _, word := range words {
		wordSet[word] = struct{}{}
	}
	return &Repository{words: wordSet, DuplicateErr: stoplisterr.ErrAlreadyExists}
}

func (repository *Repository) List(context.Context) ([]string, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()

	if repository.ListErr != nil {
		return nil, repository.ListErr
	}

	words := make([]string, 0, len(repository.words))
	for word := range repository.words {
		words = append(words, word)
	}
	return words, nil
}

func (repository *Repository) Add(_ context.Context, word string) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()

	if repository.AddErr != nil {
		return repository.AddErr
	}
	if _, exists := repository.words[word]; exists {
		return repository.DuplicateErr
	}
	repository.words[word] = struct{}{}
	return nil
}

func (repository *Repository) Remove(_ context.Context, word string) error {
	repository.mu.Lock()
	defer repository.mu.Unlock()

	if repository.RemoveErr != nil {
		return repository.RemoveErr
	}
	delete(repository.words, word)
	return nil
}

func ContainsWord(words []string, want string) bool {
	for _, word := range words {
		if word == want {
			return true
		}
	}
	return false
}
