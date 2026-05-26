package stoplist

import (
	"context"
	"errors"
)

var ErrAlreadyExists = errors.New("stopword already exists")

type Repository interface {
	List(ctx context.Context) ([]string, error)
	Add(ctx context.Context, word string) error
	Remove(ctx context.Context, word string) error
}

type PubSub interface {
	Publish(ctx context.Context) error
	Subscribe(ctx context.Context, onReload func())
	Close() error
}
