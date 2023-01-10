package versionedcache

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/omryMen/playground/versioned-cache/drivers"
	"github.com/omryMen/playground/versioned-cache/drivers/decorators"
	"golang.org/x/sync/singleflight"
)

const (
	keySeparator = ":"

	spanTagKeysFetched  = "cache_items.fetched"
	spanTagKeysFound    = "cache_items.found"
	spanTagKeysExpired  = "cache_items.expired"
	spanTagKeysMissed   = "cache_items.missed"
	spanTagIsMultiFetch = "cache_items.multi_fetch"
)

// driver is an abstraction to storage layer.
//
//go:generate go run github.com/golang/mock/mockgen -destination=cache_mock_test.go -package=versionedcache -source=cache.go driver
type driver interface {
	Get(
		ctx context.Context,
		key string,
		deserializer func(ctx context.Context, data []byte) (interface{}, error),
	) (item interface{}, found, isExpired bool, err error)
	MGet(
		ctx context.Context,
		keys []string,
		deserializer func(ctx context.Context, data []byte) (interface{}, error),
	) (items map[string]interface{}, expiredKeys []string, err error)
	Set(
		ctx context.Context,
		key string,
		item interface{},
		serializer func(ctx context.Context, item interface{}) ([]byte, error),
		expiration, ttl time.Duration,
	) error
	Clear(
		ctx context.Context,
		key string,
	) error
}

type Logger interface {
	Debug(args ...interface{})
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Panic(args ...interface{})
	Fatal(args ...interface{})
}

type locker interface {
	LockKey(ctx context.Context, key string, ttl time.Duration) (bool, error)
}

// Serializer is a function to convert item slice of bytes.
type Serializer func(ctx context.Context, item interface{}) ([]byte, error)

// Deserializer is a function to convert item from bytes to a new instance of it.
type Deserializer func(ctx context.Context, data []byte) (interface{}, error)

// Options in a configuration for cache.
//
//go:generate go run github.com/launchdarkly/go-options -prefix With -option=Option -new=false -output=options.gen.go Options
type Options struct {
	// Name is just a name indicator for metrics, logger. Usually it should be service name.
	Name string

	// CacheKeyPrefix is a prefix for all cache keys.
	CacheKeyPrefix string

	// Bypass disables cache usage.
	Bypass bool

	// UseInmemoryCache indicates use or not in memory cache.
	UseInmemoryCache bool

	// InmemoryCache driver for in memory cache.
	InmemoryCacheDriver driver

	// KeyLocker is a locking driver for preventing concurrent origin requests.
	KeyLocker locker
}

type Cache struct {
	driver driver
	logger Logger
	opts   Options
	sfg    singleflight.Group
	locker locker
}

// NewCache initializes a new cache library instance.
func NewCache(
	driver driver,
	logger Logger,
	options ...Option,
) (*Cache, error) {
	opts := Options{}
	if err := applyOptionsOptions(&opts, options...); err != nil {
		return nil, fmt.Errorf("failed to apply options: %w", err)
	}

	if opts.UseInmemoryCache {
		inMemDriver := opts.InmemoryCacheDriver
		if inMemDriver == nil {
			var inMemDriverErr error
			inMemDriver, inMemDriverErr = drivers.NewDefaultInMemoryCache()
			if inMemDriverErr != nil {
				return nil, inMemDriverErr
			}
		}

		driver = decorators.NewTwoLevelCacheDecorator(inMemDriver, driver)
	}

	return &Cache{
		driver: driver,
		opts:   opts,
		logger: logger,
		sfg:    singleflight.Group{},
		locker: opts.KeyLocker,
	}, nil
}

func (c *Cache) Clear(ctx context.Context, key, version string) error {
	fullKey := c.fullKey(key, version)

	if err := c.driver.Clear(ctx, fullKey); err != nil {
		return fmt.Errorf("failed to clear item in cache: %w", err)
	}

	return nil
}

func (c *Cache) wrapWithCompression(s Serializer, compression Compression) Serializer {
	return func(ctx context.Context, item interface{}) ([]byte, error) {
		raw, err := s(ctx, item)
		if err != nil {
			return nil, err
		}

		if compression != CompressionGzip {
			return raw, nil
		}

		buff := bytes.NewBuffer(nil)
		gzw, err := gzip.NewWriterLevel(buff, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}
		if _, err := gzw.Write(raw); err != nil {
			return nil, err
		}
		if err := gzw.Close(); err != nil {
			return nil, err
		}

		return buff.Bytes(), nil
	}
}

func (c *Cache) unwrapDeserializer(d Deserializer, compression Compression) Deserializer {
	return func(ctx context.Context, data []byte) (interface{}, error) {
		if compression == CompressionGzip {
			reader, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				return nil, err
			}

			result, err := io.ReadAll(reader)
			if err != nil {
				return nil, err
			}

			data = result
		}

		return d(ctx, data)
	}
}

func (c *Cache) fullKeys(keys []string, version string) []string {
	r := make([]string, len(keys))
	for i, key := range keys {
		r[i] = c.fullKey(key, version)
	}

	return r
}

func (c *Cache) keys(keys []string, version string) []string {
	r := make([]string, len(keys))
	for i, key := range keys {
		r[i] = strings.TrimPrefix(key, c.opts.CacheKeyPrefix+keySeparator+version+keySeparator)
	}

	return r
}

func (c *Cache) fullKey(key, version string) string {
	return c.opts.CacheKeyPrefix + keySeparator + version + keySeparator + key
}

var stringsPool = sync.Pool{
	New: func() interface{} { return make([]string, 0) },
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
