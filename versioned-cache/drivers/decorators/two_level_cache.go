package decorators

import (
	"context"
	"sync"
	"time"
)

//go:generate go run github.com/golang/mock/mockgen -destination=two_level_cache_mock_test.go -package=decorators -source=two_level_cache.go decorated
type decorated interface {
	Get(ctx context.Context, key string, deserializer func(ctx context.Context, data []byte) (interface{}, error)) (item interface{}, found, isExpired bool, err error)
	MGet(ctx context.Context, keys []string, deserializer func(ctx context.Context, data []byte) (interface{}, error)) (items map[string]interface{}, expiredKeys []string, err error)
	Set(ctx context.Context, key string, item interface{}, serializer func(ctx context.Context, item interface{}) ([]byte, error), expiration, ttl time.Duration) error
	Clear(ctx context.Context, key string) error
}

type TwoLevelCacheDecorator struct {
	l1 decorated
	l2 decorated
}

func NewTwoLevelCacheDecorator(l1, l2 decorated) *TwoLevelCacheDecorator {
	return &TwoLevelCacheDecorator{l1: l1, l2: l2}
}

func (d *TwoLevelCacheDecorator) Get(ctx context.Context, key string, deserializer func(ctx context.Context, data []byte) (interface{}, error)) (item interface{}, found, isExpired bool, err error) {
	item, found, isExpired, err = d.l1.Get(ctx, key, deserializer)
	if err != nil {
		return nil, false, false, err
	}
	if found {
		return item, true, isExpired, nil
	}

	return d.l2.Get(ctx, key, deserializer)
}

var stringsPool = sync.Pool{
	New: func() interface{} { return make([]string, 0) },
}

func (d *TwoLevelCacheDecorator) MGet(ctx context.Context, keys []string, deserializer func(ctx context.Context, data []byte) (interface{}, error)) (items map[string]interface{}, expiredKeys []string, err error) {
	items, expiredKeys, err = d.l1.MGet(ctx, keys, deserializer)
	if err != nil {
		return nil, nil, err
	}

	// l1 cache doesnt contains all items
	if len(items) != len(keys) {
		keysToFetch := stringsPool.Get().([]string)
		defer func() {
			keysToFetch = keysToFetch[:0]
			stringsPool.Put(keysToFetch) // nolint:staticcheck
		}()

		for _, key := range keys {
			if _, ok := items[key]; !ok {
				keysToFetch = append(keysToFetch, key)
			}
		}

		l2Items, l2ExpiredKeys, err := d.l2.MGet(ctx, keysToFetch, deserializer)
		if err != nil {
			return nil, nil, err
		}

		expiredKeys = append(expiredKeys, l2ExpiredKeys...)

		if items == nil {
			items = make(map[string]interface{}, len(l2Items))
		}
		for k, v := range l2Items {
			items[k] = v
		}
	}

	return items, expiredKeys, nil
}

func (d *TwoLevelCacheDecorator) Set(ctx context.Context, key string, item interface{}, serializer func(ctx context.Context, item interface{}) ([]byte, error), expiration, ttl time.Duration) error {
	if err := d.l1.Set(ctx, key, item, serializer, expiration, ttl); err != nil {
		return err
	}

	return d.l2.Set(ctx, key, item, serializer, expiration, ttl)
}

func (d *TwoLevelCacheDecorator) Clear(ctx context.Context, key string) error {
	if err := d.l1.Clear(ctx, key); err != nil {
		return err
	}

	return d.l2.Clear(ctx, key)
}
