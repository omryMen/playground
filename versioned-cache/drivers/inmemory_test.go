package drivers

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewInMemoryCache(t *testing.T) {
	inst, err := NewDefaultInMemoryCache()
	assert.NoError(t, err)
	assert.NotNil(t, inst)

	ctx := context.Background()

	assert.NoError(t, inst.Set(ctx, "good", 1, nil, time.Minute, time.Minute))
	assert.NoError(t, inst.Set(ctx, "expired", 2, nil, -time.Minute, time.Minute))

	t.Run("empty state", func(t *testing.T) {
		item, found, isExpired, err := inst.Get(ctx, "missing_key", nil)
		assert.NoError(t, err)
		assert.False(t, isExpired)
		assert.False(t, found)
		assert.Nil(t, item)

		items, expiredItems, err := inst.MGet(ctx, []string{"missing_key"}, nil)
		assert.NoError(t, err)
		assert.Equal(t, map[string]interface{}{}, items)
		assert.Nil(t, expiredItems)
	})

	t.Run("single get", func(t *testing.T) {
		item, found, isExpired, err := inst.Get(ctx, "good", nil)
		assert.NoError(t, err)
		assert.False(t, isExpired)
		assert.True(t, found)
		assert.Equal(t, interface{}(1), item)

		item, found, isExpired, err = inst.Get(ctx, "expired", nil)
		assert.NoError(t, err)
		assert.True(t, isExpired)
		assert.True(t, found)
		assert.Equal(t, interface{}(2), item)
	})

	t.Run("multiple get", func(t *testing.T) {
		items, expiredKeys, err := inst.MGet(ctx, []string{"good", "expired"}, nil)
		assert.NoError(t, err)
		assert.Equal(t, []string{"expired"}, expiredKeys)
		assert.Equal(t, map[string]interface{}{
			"good":    1,
			"expired": 2,
		}, items)
	})

	t.Run("clear", func(t *testing.T) {
		assert.NoError(t, inst.Set(ctx, "additional", 1, nil, time.Minute, time.Minute))

		item, found, isExpired, err := inst.Get(ctx, "additional", nil)
		assert.NoError(t, err)
		assert.False(t, isExpired)
		assert.True(t, found)
		assert.Equal(t, interface{}(1), item)

		assert.NoError(t, inst.Clear(ctx, "additional"))
		item, found, isExpired, err = inst.Get(ctx, "additional", nil)
		assert.NoError(t, err)
		assert.False(t, isExpired)
		assert.False(t, found)
		assert.Nil(t, item)
	})

	t.Run("expiration", func(t *testing.T) {
		assert.NoError(t, inst.Set(ctx, "key_exp", 1, nil, time.Minute, time.Millisecond))
		time.Sleep(time.Millisecond)

		item, found, isExpired, err := inst.Get(ctx, "key_exp", nil)
		assert.NoError(t, err)
		assert.False(t, isExpired)
		assert.False(t, found)
		assert.Nil(t, item)
	})
}

func Test_itemIsExpired(t *testing.T) {
	item := inMemItem{expirationAt: time.Now().Add(-time.Second)}
	assert.True(t, item.isExpired())

	item = inMemItem{expirationAt: time.Now().Add(time.Second)}
	assert.False(t, item.isExpired())
}
