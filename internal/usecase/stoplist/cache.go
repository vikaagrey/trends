package stoplist

import "sync/atomic"

type Cache struct {
	words atomic.Pointer[map[string]struct{}]
}

func NewCache() *Cache {
	cache := &Cache{}
	empty := make(map[string]struct{})
	cache.words.Store(&empty)
	return cache
}

func (cache *Cache) Load() map[string]struct{} {
	words := cache.words.Load()
	if words == nil {
		return nil
	}
	return *words
}

func (cache *Cache) Store(words map[string]struct{}) {
	cache.words.Store(&words)
}
