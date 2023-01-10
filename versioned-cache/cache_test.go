package versionedcache

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/omryMen/playground/versioned-cache/drivers"
	"github.com/omryMen/playground/versioned-cache/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type model struct {
	Name string
}

var (
	m1         = &model{Name: "m1"}
	serializer = func(ctx context.Context, v interface{}) ([]byte, error) {
		return json.Marshal(v)
	}
	deserializer = func(ctx context.Context, v []byte) (interface{}, error) {
		n := &model{}
		err := json.Unmarshal(v, n)
		return n, err
	}
	singleRetrieve = func(ctx context.Context) (interface{}, error) {
		return m1, nil
	}
	multiRetrieve = func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		return map[string]interface{}{
			"key1": m1,
		}, nil
	}
	cfg = ItemConfig{
		Expiration:    time.Second,
		KeyExpiration: time.Minute,
		Bypass:        false,
		Override:      false,
		Compression:   0,
	}
)

func Test_Compressions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	logger := mock.NewMockLogger(ctrl)

	mem1, err := drivers.NewDefaultInMemoryCache()
	assert.NoError(t, err)
	mem2, err := drivers.NewDefaultInMemoryCache()
	assert.NoError(t, err)

	c, err := NewCache(mem1, logger,
		WithInmemoryCacheDriver(mem2),
		WithCacheKeyPrefix("pre"),
	)
	assert.NoError(t, err)

	ctx := context.Background()

	ser := c.wrapWithCompression(serializer, CompressionGzip)
	deser := c.unwrapDeserializer(deserializer, CompressionGzip)

	raw, err := ser(ctx, m1)
	require.NoError(t, err)

	obj, err := deser(ctx, raw)
	require.NoError(t, err)
	require.Equal(t, interface{}(m1), obj)
}

func TestCache_RedisCluster(t *testing.T) {
	if os.Getenv("REDIS_ENABLED") != "true" {
		t.Skip("please run docker compose and add REDIS_ENABLED=true env")
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	logger := mock.NewMockLogger(ctrl)

	redisDriver, err := drivers.NewRedisClusterCache(":7000,:7001,:7002,:7003,:7004,:7005")
	require.NoError(t, err)

	c, err := NewCache(redisDriver, logger,
		WithUseInmemoryCache(false),
		WithCacheKeyPrefix("pre"),
	)
	assert.NoError(t, err)

	ctx := context.Background()
	res, err := c.SingleLoadOrStore(
		ctx,
		"key1",
		singleRetrieve,
		serializer,
		deserializer,
		cfg,
	)
	assert.NoError(t, err)
	assert.Equal(t, m1, res)

	// should be taken from cache
	res, err = c.SingleLoadOrStore(
		ctx,
		"key1",
		func(ctx context.Context) (interface{}, error) {
			require.True(t, false)
			return nil, nil
		},
		serializer,
		deserializer,
		cfg,
	)
	assert.NoError(t, err)
	assert.Equal(t, m1, res)

	items, err := c.MultiLoadOrStore(ctx, []string{"key1"}, func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		require.True(t, false)
		return nil, nil
	}, serializer, deserializer, cfg)
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{m1}, items)
	require.NoError(t, c.Clear(ctx, "key1", ""))
}

func BenchmarkCache_SingleLoadOrStore(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()
	logger := mock.NewMockLogger(ctrl)

	mem1, err := drivers.NewDefaultInMemoryCache()
	assert.NoError(b, err)

	b.ReportAllocs()

	c, err := NewCache(mem1, logger,
		WithUseInmemoryCache(false),
		WithCacheKeyPrefix("pre"),
	)
	assert.NoError(b, err)

	ctx := context.Background()
	itemCfg := ItemConfig{
		Expiration:    time.Minute,
		KeyExpiration: time.Minute * 10,
		Bypass:        false,
		Override:      false,
		Compression:   0,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.SingleLoadOrStore(
			ctx,
			"test1",
			func(ctx context.Context) (interface{}, error) {
				return m1, nil
			},
			serializer,
			deserializer,
			itemCfg,
		)
	}
}
