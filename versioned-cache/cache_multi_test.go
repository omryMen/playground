package versionedcache

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/omryMen/playground/versioned-cache/drivers"
	"github.com/omryMen/playground/versioned-cache/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCache_Load_MultiFirst(t *testing.T) {
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

	items, err := c.MultiLoadOrStore(ctx, []string{"key1"}, multiRetrieve, serializer, deserializer, cfg)
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{m1}, items)

	// should be taken from cache
	items, err = c.MultiLoadOrStore(ctx, []string{"key1"}, func(ctx context.Context, keys []string) (map[string]interface{}, error) {
		require.True(t, false)
		return nil, nil
	}, serializer, deserializer, cfg)
	assert.NoError(t, err)
	assert.Equal(t, []interface{}{m1}, items)

	res, err := c.SingleLoadOrStore(
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
}
