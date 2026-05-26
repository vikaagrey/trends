package stoplist

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const stoplistUpdatesChannel = "stoplist:updates"

type PubSub struct {
	client *redis.Client
}

func NewPubSub(ctx context.Context, redisURL string) (*PubSub, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect to Redis: %w", err)
	}
	return &PubSub{client: client}, nil
}

func (pubSub *PubSub) Publish(ctx context.Context) error {
	if err := pubSub.client.Publish(ctx, stoplistUpdatesChannel, "reload").Err(); err != nil {
		return fmt.Errorf("publish Redis event: %w", err)
	}
	return nil
}

func (pubSub *PubSub) Subscribe(ctx context.Context, onReload func()) {
	subscription := pubSub.client.Subscribe(ctx, stoplistUpdatesChannel)
	messages := subscription.Channel()
	go func() {
		defer subscription.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-messages:
				if !ok {
					return
				}
				onReload()
			}
		}
	}()
}

func (pubSub *PubSub) Close() error {
	if err := pubSub.client.Close(); err != nil {
		return fmt.Errorf("close Redis client: %w", err)
	}
	return nil
}
