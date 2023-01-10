package drivers

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/go-redis/redismock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testModel struct {
	Name string `json:"name"`
}

var (
	m1 = &testModel{Name: "m1"}

	serialize = func(ctx context.Context, v interface{}) ([]byte, error) {
		return json.Marshal(v)
	}
	deserialize = func(ctx context.Context, v []byte) (interface{}, error) {
		n := &testModel{}
		err := json.Unmarshal(v, n)
		return n, err
	}
)

func TestRedisClusterCache_Empty(t *testing.T) {
	clusterClient, serverMock := redismock.NewClusterMock()

	serverMock.ExpectGet("missing_single").RedisNil()
	serverMock.ExpectGet("missing_multi").RedisNil()
	serverMock.ExpectMGet("{s}missing_multi2").SetVal(nil)

	inst := &RedisClusterCache{
		client:       clusterClient,
		multiGetMode: MultiGetModeMultiGet,
		slot:         "",
	}
	ctx := context.Background()

	item, found, isExpired, err := inst.Get(ctx, "missing_single", deserialize)
	assert.NoError(t, err)
	assert.False(t, isExpired)
	assert.False(t, found)
	assert.Nil(t, item)

	items, expiredItems, err := inst.MGet(ctx, []string{"missing_multi"}, deserialize)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{}, items)
	assert.Nil(t, expiredItems)

	inst.multiGetMode = MultiGetModeTheSameSlot
	inst.slot = "s"
	items, expiredItems, err = inst.MGet(ctx, []string{"missing_multi2"}, deserialize)
	assert.NoError(t, err)
	assert.Equal(t, map[string]interface{}{}, items)
	assert.Nil(t, expiredItems)
}

func TestRedisClusterCache_SingleGet(t *testing.T) {
	ctx := context.Background()
	m1Raw, err := serialize(ctx, &m1)
	require.NoError(t, err)

	tt := time.Now().Add(-time.Minute)
	rcItem := &rcCacheItem{key: "good", item: m1Raw, expirationAt: tt}
	rcItemRaw, err := rcItem.serialize()
	require.NoError(t, err)

	clusterClient, serverMock := redismock.NewClusterMock()
	serverMock.ExpectGet("good").SetVal(string(rcItemRaw))

	inst := &RedisClusterCache{
		client:       clusterClient,
		multiGetMode: MultiGetModeMultiGet,
		slot:         "",
	}

	item, found, isExpired, err := inst.Get(ctx, "good", deserialize)
	assert.NoError(t, err)
	assert.True(t, isExpired)
	assert.True(t, found)
	assert.Equal(t, interface{}(m1), item)
}

func TestRedisClusterCache_MultiGet(t *testing.T) {
	ctx := context.Background()
	m1Raw, err := serialize(ctx, &m1)
	require.NoError(t, err)

	tt := time.Now().Add(-time.Minute)
	rcItem := &rcCacheItem{key: "good", item: m1Raw, expirationAt: tt}
	rcItemRaw, err := rcItem.serialize()
	require.NoError(t, err)

	clusterClient, serverMock := redismock.NewClusterMock()
	serverMock.ExpectMGet("{s}good").SetVal([]interface{}{
		string(rcItemRaw),
	})

	inst := &RedisClusterCache{
		client:       clusterClient,
		multiGetMode: MultiGetModeTheSameSlot,
		slot:         "s",
	}

	items, expiredKeys, err := inst.MGet(ctx, []string{"good"}, deserialize)
	assert.NoError(t, err)
	assert.Equal(t, []string{"good"}, expiredKeys)
	assert.Equal(t, map[string]interface{}{
		"good": m1,
	}, items)
}

func TestRedisClusterCache_Clear(t *testing.T) {
	clusterClient, serverMock := redismock.NewClusterMock()
	serverMock.ExpectDel("good").SetVal(1)

	inst := &RedisClusterCache{
		client:       clusterClient,
		multiGetMode: MultiGetModeMultiGet,
		slot:         "",
	}

	ctx := context.Background()
	assert.NoError(t, inst.Clear(ctx, "good"))
}

func Test_ItemSerialization(t *testing.T) {
	tt := time.Now()

	item := &rcCacheItem{
		key:          "test key",
		item:         []byte("test value"),
		expirationAt: tt,
	}

	// serializing
	bytes, err := item.serialize()
	assert.NoError(t, err)
	assert.NotNil(t, bytes)

	// serializing
	item2 := &rcCacheItem{}
	assert.NoError(t, item2.deserialize(bytes))
	assert.Equal(t, "test key", item2.key)
	assert.Equal(t, []byte("test value"), item2.item)
	assert.Equal(t, tt.Unix(), item2.expirationAt.Unix())
}
