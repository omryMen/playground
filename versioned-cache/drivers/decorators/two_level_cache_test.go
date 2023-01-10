package decorators

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestTwoLevelCacheDecorator_Get(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	t.Run("l1 used", func(t *testing.T) {
		l1 := NewMockdecorated(ctrl)
		l2 := NewMockdecorated(ctrl)
		driver := NewTwoLevelCacheDecorator(l1, l2)

		l1.EXPECT().Get(gomock.Eq(ctx), gomock.Eq("key"), gomock.Any()).
			Return("ok", true, true, nil)

		item, found, isExpired, err := driver.Get(ctx, "key", nil)
		assert.NoError(t, err)
		assert.True(t, found)
		assert.True(t, isExpired)
		assert.Equal(t, interface{}("ok"), item)
	})

	t.Run("l2 used", func(t *testing.T) {
		l1 := NewMockdecorated(ctrl)
		l2 := NewMockdecorated(ctrl)
		driver := NewTwoLevelCacheDecorator(l1, l2)

		l1.EXPECT().Get(gomock.Eq(ctx), gomock.Eq("l1_missing"), gomock.Any()).
			Return(nil, false, false, nil)
		l2.EXPECT().Get(gomock.Eq(ctx), gomock.Eq("l1_missing"), gomock.Any()).
			Return("ok2", true, true, nil)

		item, found, isExpired, err := driver.Get(ctx, "l1_missing", nil)
		assert.NoError(t, err)
		assert.True(t, found)
		assert.True(t, isExpired)
		assert.Equal(t, interface{}("ok2"), item)
	})

	t.Run("both missing", func(t *testing.T) {
		l1 := NewMockdecorated(ctrl)
		l2 := NewMockdecorated(ctrl)
		driver := NewTwoLevelCacheDecorator(l1, l2)

		l1.EXPECT().Get(gomock.Eq(ctx), gomock.Eq("l2_missing"), gomock.Any()).
			Return(nil, false, false, nil)
		l2.EXPECT().Get(gomock.Eq(ctx), gomock.Eq("l2_missing"), gomock.Any()).
			Return(nil, false, false, nil)

		item, found, isExpired, err := driver.Get(ctx, "l2_missing", nil)
		assert.NoError(t, err)
		assert.False(t, found)
		assert.False(t, isExpired)
		assert.Equal(t, nil, item)
	})
}

func TestTwoLevelCacheDecorator_MGet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	t.Run("l1 used", func(t *testing.T) {
		l1 := NewMockdecorated(ctrl)
		l2 := NewMockdecorated(ctrl)
		driver := NewTwoLevelCacheDecorator(l1, l2)

		l1.EXPECT().MGet(gomock.Eq(ctx), gomock.Eq([]string{"key"}), gomock.Any()).
			Return(map[string]interface{}{"key": "ok"}, []string{}, nil)

		items, expiredKeys, err := driver.MGet(ctx, []string{"key"}, nil)
		assert.NoError(t, err)
		assert.Equal(t, []string{}, expiredKeys)
		assert.Equal(t, map[string]interface{}{"key": "ok"}, items)
	})

	t.Run("l2 used", func(t *testing.T) {
		l1 := NewMockdecorated(ctrl)
		l2 := NewMockdecorated(ctrl)
		driver := NewTwoLevelCacheDecorator(l1, l2)

		l1.EXPECT().MGet(gomock.Eq(ctx), gomock.Eq([]string{"key"}), gomock.Any()).
			Return(nil, []string{}, nil)
		l2.EXPECT().MGet(gomock.Eq(ctx), gomock.Eq([]string{"key"}), gomock.Any()).
			Return(map[string]interface{}{"key": "ok"}, []string{}, nil)

		items, expiredKeys, err := driver.MGet(ctx, []string{"key"}, nil)
		assert.NoError(t, err)
		assert.Equal(t, []string{}, expiredKeys)
		assert.Equal(t, map[string]interface{}{"key": "ok"}, items)
	})

	t.Run("l1 + l2 used", func(t *testing.T) {
		l1 := NewMockdecorated(ctrl)
		l2 := NewMockdecorated(ctrl)
		driver := NewTwoLevelCacheDecorator(l1, l2)

		l1.EXPECT().MGet(gomock.Eq(ctx), gomock.Eq([]string{"key1", "key2"}), gomock.Any()).
			Return(map[string]interface{}{"key2": "222"}, []string{"key2"}, nil)
		l2.EXPECT().MGet(gomock.Eq(ctx), gomock.Eq([]string{"key1"}), gomock.Any()).
			Return(map[string]interface{}{"key1": "111"}, []string{"key1"}, nil)

		items, expiredKeys, err := driver.MGet(ctx, []string{"key1", "key2"}, nil)
		assert.NoError(t, err)
		sort.Strings(expiredKeys)
		assert.Equal(t, []string{"key1", "key2"}, expiredKeys)
		assert.Len(t, items, 2)
		assert.Equal(t, "222", items["key2"])
		assert.Equal(t, "111", items["key1"])
	})

	t.Run("both missing", func(t *testing.T) {
		l1 := NewMockdecorated(ctrl)
		l2 := NewMockdecorated(ctrl)
		driver := NewTwoLevelCacheDecorator(l1, l2)

		l1.EXPECT().MGet(gomock.Eq(ctx), gomock.Eq([]string{"key"}), gomock.Any()).
			Return(nil, []string{}, nil)
		l2.EXPECT().MGet(gomock.Eq(ctx), gomock.Eq([]string{"key"}), gomock.Any()).
			Return(nil, []string{}, nil)

		items, expiredKeys, err := driver.MGet(ctx, []string{"key"}, nil)
		assert.NoError(t, err)
		assert.Equal(t, []string{}, expiredKeys)
		assert.Equal(t, map[string]interface{}{}, items)
	})
}

func TestTwoLevelCacheDecorator_Set(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	l1 := NewMockdecorated(ctrl)
	l1.EXPECT().Set(gomock.Eq(ctx), gomock.Eq("good"), gomock.Eq(5), gomock.Any(), gomock.Eq(time.Minute), gomock.Eq(time.Second)).
		Return(nil)

	l2 := NewMockdecorated(ctrl)
	l2.EXPECT().Set(gomock.Eq(ctx), gomock.Eq("good"), gomock.Eq(5), gomock.Any(), gomock.Eq(time.Minute), gomock.Eq(time.Second)).
		Return(nil)

	driver := NewTwoLevelCacheDecorator(l1, l2)
	err := driver.Set(ctx, "good", 5, nil, time.Minute, time.Second)
	assert.NoError(t, err)
}

func TestTwoLevelCacheDecorator_Clear(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	l1 := NewMockdecorated(ctrl)
	l1.EXPECT().Clear(gomock.Eq(ctx), gomock.Eq("good")).
		Return(nil)

	l2 := NewMockdecorated(ctrl)
	l2.EXPECT().Clear(gomock.Eq(ctx), gomock.Eq("good")).
		Return(nil)

	driver := NewTwoLevelCacheDecorator(l1, l2)
	err := driver.Clear(ctx, "good")
	assert.NoError(t, err)
}
