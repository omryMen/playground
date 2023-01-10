package lockers_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/omryMen/playground/versioned-cache/lockers"
	"github.com/stretchr/testify/require"
)

func setupLocker(t *testing.T) (*lockers.RedisClusterLocker, *miniredis.Miniredis) {
	s := miniredis.RunT(t)
	conn := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: strings.Split(s.Addr(), ","),
	})
	return lockers.NewRedisClusterLocker(conn), s
}

func TestRedisClusterLocker_LockKey_FirstSucceeds(t *testing.T) {
	key := "service:v2:keyA:lock"
	ttl := 10 * time.Second
	locker, _ := setupLocker(t)

	// Acquire lock for the key.
	firstOk, err := locker.LockKey(context.Background(), key, ttl)
	require.NoError(t, err)
	require.True(t, firstOk)

	// Ensure that consecutive call fails to acquire a lock for the same key.
	secondOk, err := locker.LockKey(context.Background(), key, ttl)
	require.NoError(t, err)
	require.False(t, secondOk)
}

func TestRedisClusterLocker_LockKey_Release(t *testing.T) {
	key := "service:v2:keyA:lock"
	ttl := 10 * time.Second
	locker, db := setupLocker(t)

	// Acquire lock for the key.
	ok, err := locker.LockKey(context.Background(), key, ttl)
	require.NoError(t, err)
	require.True(t, ok)

	// Wait until lock expires.
	db.FastForward(ttl)

	// Ensure that a lock is acquired for the same key.
	ok, err = locker.LockKey(context.Background(), key, ttl)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRedisClusterLocker_LockKey_SeparateKeys(t *testing.T) {
	keyA := "service:v2:keyA:lock"
	keyB := "service:v2:keyB:lock"
	ttl := 10 * time.Second
	locker, _ := setupLocker(t)

	// Acquire lock for the key.
	ok, err := locker.LockKey(context.Background(), keyA, ttl)
	require.NoError(t, err)
	require.True(t, ok)

	// Ensure that lock can be acquired fot another key.
	ok, err = locker.LockKey(context.Background(), keyB, ttl)
	require.NoError(t, err)
	require.True(t, ok)
}
