package drivers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dgraph-io/ristretto"
)

type InMemoryCache struct {
	cache *ristretto.Cache
}

func NewDefaultInMemoryCache() (*InMemoryCache, error) {
	return NewInMemoryCache(ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
}

func NewInMemoryCache(cfg ristretto.Config) (*InMemoryCache, error) {
	c, err := ristretto.NewCache(&cfg)
	if err != nil {
		return nil, err
	}

	return &InMemoryCache{cache: c}, nil
}

type inMemItem struct {
	item         interface{}
	expirationAt time.Time
}

func (i *inMemItem) isExpired() bool {
	return i.expirationAt.Before(time.Now())
}

func (c *InMemoryCache) Get(_ context.Context, key string, _ func(ctx context.Context, data []byte) (interface{}, error)) (item interface{}, found, isExpired bool, err error) {
	v, ok := c.cache.Get(key)
	if !ok {
		return nil, false, false, nil
	}

	i, ok := v.(*inMemItem)
	if !ok {
		return nil, false, false, fmt.Errorf("invalid inmemory cache item format")
	}

	return i.item, true, i.isExpired(), nil
}

func (c *InMemoryCache) MGet(ctx context.Context, keys []string, deserializer func(ctx context.Context, data []byte) (interface{}, error)) (items map[string]interface{}, expiredKeys []string, err error) {
	items = make(map[string]interface{}, len(keys))
	for _, key := range keys {
		item, found, isExpired, err := c.Get(ctx, key, deserializer)
		if err != nil {
			return nil, nil, err
		}
		if found {
			items[key] = item
		}
		if isExpired {
			expiredKeys = append(expiredKeys, key)
		}
	}

	return items, expiredKeys, nil
}

func (c *InMemoryCache) Set(_ context.Context, key string, item interface{}, _ func(ctx context.Context, item interface{}) ([]byte, error), expiration, ttl time.Duration) error {
	ci := &inMemItem{
		item:         item,
		expirationAt: time.Now().Add(expiration),
	}

	if !c.cache.SetWithTTL(key, ci, 1, ttl) {
		return errors.New("cache item was dropped")
	}

	c.cache.Wait()

	return nil
}

func (c *InMemoryCache) Clear(_ context.Context, key string) error {
	c.cache.Del(key)

	return nil
}
