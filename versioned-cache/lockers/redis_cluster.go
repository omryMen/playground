package lockers

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisClusterLocker struct {
	client *redis.ClusterClient
}

func NewRedisClusterLocker(client *redis.ClusterClient) *RedisClusterLocker {
	return &RedisClusterLocker{client: client}
}

func (r *RedisClusterLocker) LockKey(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.client.SetNX(ctx, key, 1, ttl).Result()
}
