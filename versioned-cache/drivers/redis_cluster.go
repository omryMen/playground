package drivers

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	// MultiGetModeTheSameSlot means that all keys of this client will have the same slot and will be located on the same node.
	// Please use this strategy if you don't have huge amout of data and can't overload a specifig node.
	MultiGetModeTheSameSlot = "THE_SAME_SLOT"

	// MultiGetModeMultiGet means that we will use multi get operations to get all records. It less performent but all keys will be splitted between nodes.
	MultiGetModeMultiGet = "MULTIPLE_GET"

	// MultiParallelGetModeMultiGet means that we will use multi get operations to get all records in parallel.
	MultiParallelGetModeMultiGet = "MULTIPLE_PARALLEL_GET"

	DefaultMultiGetMode = MultiParallelGetModeMultiGet
	DefaultTimeout      = time.Second * 2
	DefaultPoolSize     = 100
)

// MGetMode controls how we will achive working of multi get in Redis Cluster.
// As you know, all keys in Redis Cluster are splitted between nodes and which node use will be decided by key slot.
// MGet will work only when all keys passed will have the same slot. This is how Redis works.
type MGetMode string

type RedisClusterCache struct {
	client       *redis.ClusterClient
	multiGetMode MGetMode
	slot         string
	rateLimiter  chan struct{}
}

//go:generate go run github.com/launchdarkly/go-options -prefix With -option=Option -new=false -output=redis_cluster_options.gen.go Options
type Options struct {

	// MultiGetMode is as strategy we want to use for multi get operation.
	MultiGetMode MGetMode

	// Used and required if multi get mode equals MultiGetModeTheSameSlot.
	Slot string

	// PoolSize is size of pool for redis connections.
	PoolSize uint

	// Timeout will be used as read/write timeout for opened redis connection.
	Timeout time.Duration
}

func NewRedisClusterCache(clusterAddresses string, options ...Option) (*RedisClusterCache, error) {
	opts := &Options{}
	opts.Timeout = DefaultTimeout
	opts.PoolSize = DefaultPoolSize
	opts.MultiGetMode = DefaultMultiGetMode

	if err := applyOptionsOptions(opts, options...); err != nil {
		return nil, err
	}

	if opts.MultiGetMode == MultiGetModeTheSameSlot && opts.Slot == "" {
		return nil, errors.New("please specify slot for keys")
	}

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:         strings.Split(clusterAddresses, ","),
		PoolSize:      int(opts.PoolSize),
		WriteTimeout:  opts.Timeout,
		ReadTimeout:   opts.Timeout,
		RouteRandomly: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), opts.Timeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	rateLimiter := make(chan struct{}, opts.PoolSize)
	for i := uint(0); i < opts.PoolSize; i++ {
		rateLimiter <- struct{}{}
	}

	return &RedisClusterCache{
		client:       client,
		multiGetMode: opts.MultiGetMode,
		slot:         opts.Slot,
		rateLimiter:  rateLimiter,
	}, nil
}

func NewRedisClusterCacheWithClient(client *redis.ClusterClient, options ...Option) (*RedisClusterCache, error) {
	opts := &Options{}
	opts.Timeout = DefaultTimeout
	opts.PoolSize = DefaultPoolSize
	opts.MultiGetMode = DefaultMultiGetMode

	if err := applyOptionsOptions(opts, options...); err != nil {
		return nil, err
	}

	if opts.MultiGetMode == MultiGetModeTheSameSlot && opts.Slot == "" {
		return nil, errors.New("please specify slot for keys")
	}

	rateLimiter := make(chan struct{}, opts.PoolSize)
	for i := uint(0); i < opts.PoolSize; i++ {
		rateLimiter <- struct{}{}
	}

	return &RedisClusterCache{
		client:       client,
		multiGetMode: opts.MultiGetMode,
		slot:         opts.Slot,
		rateLimiter:  rateLimiter,
	}, nil
}

type rcCacheItem struct {
	key          string
	item         []byte
	expirationAt time.Time
}

func (i *rcCacheItem) isExpired() bool {
	return i.expirationAt.Before(time.Now())
}

func (i *rcCacheItem) deserialize(buff []byte) error {
	if len(buff) < 10 {
		return fmt.Errorf("invalid commressed cache item value")
	}

	i.expirationAt = time.Unix(int64(binary.LittleEndian.Uint64(buff[:8])), 0)
	keyLen := binary.LittleEndian.Uint16(buff[8:10])
	i.key = string(buff[10 : 10+keyLen])
	i.item = buff[10+keyLen:]

	return nil
}

func (i *rcCacheItem) serialize() ([]byte, error) {
	buff := make([]byte, 10, 10+len(i.key)+len(i.item))
	binary.LittleEndian.PutUint64(buff[0:8], uint64(i.expirationAt.Unix()))
	binary.LittleEndian.PutUint16(buff[8:10], uint16(len(i.key)))
	buff = append(buff, []byte(i.key)...)
	buff = append(buff, i.item...)

	return buff, nil
}

func (c *RedisClusterCache) Get(ctx context.Context, key string, deserializer func(ctx context.Context, data []byte) (interface{}, error)) (item interface{}, found, isExpired bool, err error) {
	v, err := c.client.Get(ctx, c.keyWithSlot(key)).Result()
	if err == redis.Nil {
		return nil, false, false, nil
	}
	if err != nil {
		return nil, false, false, err
	}

	cacheItem := &rcCacheItem{}
	if desErr := cacheItem.deserialize([]byte(v)); desErr != nil {
		return nil, false, false, desErr
	}

	item, err = deserializer(ctx, cacheItem.item)
	if err != nil {
		return nil, false, false, err
	}

	return item, true, cacheItem.isExpired(), nil
}

func (c *RedisClusterCache) MGet(ctx context.Context, keys []string, deserializer func(ctx context.Context, data []byte) (interface{}, error)) (items map[string]interface{}, expiredKeys []string, err error) {
	values, err := c.mGet(ctx, keys)
	if err != nil {
		return nil, nil, err
	}

	cacheItem := &rcCacheItem{}
	items = make(map[string]interface{}, len(keys))
	for _, value := range values {
		vs, ok := value.([]byte)
		if !ok {
			bvs, ok2 := value.(string)
			if !ok2 {
				continue
			}

			vs = []byte(bvs)
		}

		if desErr := cacheItem.deserialize(vs); desErr != nil {
			return nil, nil, desErr
		}

		item, desErr := deserializer(ctx, cacheItem.item)
		if desErr != nil {
			return nil, nil, desErr
		}

		if cacheItem.isExpired() {
			expiredKeys = append(expiredKeys, cacheItem.key)
		}
		items[cacheItem.key] = item
	}

	return items, expiredKeys, nil
}

func (c *RedisClusterCache) mGet(ctx context.Context, keys []string) ([]interface{}, error) {
	switch c.multiGetMode {
	case MultiGetModeMultiGet:
		values := make([]interface{}, 0, len(keys))
		for _, key := range keys {
			v, err := c.client.Get(ctx, c.keyWithSlot(key)).Result()
			if err == redis.Nil {
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("failed to get key %q: %w", key, err)
			}
			values = append(values, v)
		}
		return values, nil
	case MultiGetModeTheSameSlot:
		return c.client.MGet(ctx, c.keysWithSlot(keys)...).Result()
	case MultiParallelGetModeMultiGet:
		results := make(chan *redis.StringCmd)
		done := make(chan struct{})
		defer close(results)
		defer close(done)

		for _, key := range keys {
			<-c.rateLimiter
			go func(k string) {
				defer func() {
					c.rateLimiter <- struct{}{}
				}()

				select {
				case <-done:
				case results <- c.client.Get(ctx, c.keyWithSlot(k)):
				}
			}(key)
		}

		values := make([]interface{}, 0, len(keys))
		for i := 0; i < len(keys); i++ {
			result := <-results
			if result.Err() == redis.Nil {
				continue
			}
			if result.Err() != nil {
				close(done) // stop to accept other values

				return nil, fmt.Errorf("failed to get item: %w", result.Err())
			}
			values = append(values, result.Val())
		}

		return values, nil
	default:
		return nil, fmt.Errorf("multi get mode %q not supported", c.multiGetMode)
	}
}

func (c *RedisClusterCache) Set(ctx context.Context, key string, item interface{}, serializer func(ctx context.Context, item interface{}) ([]byte, error), expiration, ttl time.Duration) error {
	rawItem, err := serializer(ctx, item)
	if err != nil {
		return err
	}

	cacheItem := &rcCacheItem{
		key:          key,
		item:         rawItem,
		expirationAt: time.Now().Add(expiration),
	}

	v, err := cacheItem.serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize cache item: %w", err)
	}

	return c.client.Set(ctx, c.keyWithSlot(key), v, ttl).Err()
}

func (c *RedisClusterCache) Clear(ctx context.Context, key string) error {
	return c.client.Del(ctx, c.keyWithSlot(key)).Err()
}

func (c *RedisClusterCache) keyWithSlot(key string) string {
	if c.multiGetMode == MultiGetModeTheSameSlot {
		return "{" + c.slot + "}" + key
	}

	return key
}

func (c *RedisClusterCache) keysWithSlot(keys []string) []string {
	r := make([]string, 0, len(keys))
	for _, key := range keys {
		r = append(r, c.keyWithSlot(key))
	}

	return r
}
