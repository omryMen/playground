package versionedcache

import (
	"context"
	"fmt"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type SingleRetrieve func(ctx context.Context) (interface{}, error)

func (c *Cache) SingleLoadOrStore(
	ctxIn context.Context,
	key string,
	retrieveFn SingleRetrieve,
	serializer Serializer,
	deserializer Deserializer,
	cfg ItemConfig,
) (interface{}, error) {
	span, ctx := tracer.StartSpanFromContext(ctxIn, "cache_get",
		tracer.SpanType("cache"),
		//tracing.Tag(spanTagKeysFetched, 1), // TODO: either change to metric or add Tag to telemetry
		//tracing.Tag(spanTagIsMultiFetch, false),
	)
	defer span.Finish()

	fullKey := c.fullKey(key, cfg.Version)

	resultChan := c.sfg.DoChan(fullKey, func() (interface{}, error) {
		if c.opts.Bypass || cfg.Bypass {
			return retrieveFn(ctx)
		}

		if cfg.Override {
			return c.singleRetrieveAndSet(ctx, fullKey, retrieveFn, serializer, cfg)
		}

		item, found, isExpired, err := c.driver.Get(ctx, fullKey, c.unwrapDeserializer(deserializer, cfg.Compression))
		if err != nil {
			span.SetTag(ext.Error, err)
			c.logger.Error("cache driver Get", "msg", err)

			return c.singleRetrieveAndSet(ctx, fullKey, retrieveFn, serializer, cfg)
		}
		if !found {
			span.SetTag(spanTagKeysMissed, 1)

			return c.singleRetrieveAndSet(ctx, fullKey, retrieveFn, serializer, cfg)
		}
		span.SetTag(spanTagKeysFound, 1)
		if isExpired {
			span.SetTag(spanTagKeysExpired, 1)

			go func() {
				_, err, _ := c.sfg.Do(fullKey+"-async", func() (interface{}, error) {
					ctx := context.Background()
					// Try to take a lock if enabled.
					if cfg.SourceRequestLockTTL > 0 && c.locker != nil {
						lockKey := fmt.Sprintf("%s%s%s", fullKey, keySeparator, "lock")
						if ok, err := c.locker.LockKey(ctx, lockKey, cfg.SourceRequestLockTTL); err != nil {
							// Something went wrong with locking functionality.
							// Continue execution to keep the main flow working.
							c.logger.Error(fmt.Errorf("failed to take a lock: %w", err))
							return nil, nil
						} else if !ok {
							// Someone has acquired the lock already. Stop execution.
							return nil, nil
						}
					}
					return c.singleRetrieveAndSet(ctx, fullKey, retrieveFn, serializer, cfg)
				})

				if err != nil {
					c.logger.Error(fmt.Errorf("failed to update cache item in async mode: %w", err))
				}
			}()
		}

		return item, nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultChan:
		if result.Err != nil {
			span.SetTag(ext.Error, result.Err)
		}
		return result.Val, result.Err
	}
}

func (c *Cache) singleRetrieveAndSet(
	ctxIn context.Context,
	fullKey string,
	retrieveFn SingleRetrieve,
	serializer Serializer,
	cfg ItemConfig,
) (interface{}, error) {
	span, ctx := tracer.StartSpanFromContext(ctxIn, "cache_get_retrieve",
		tracer.SpanType("db"),
	)
	item, err := retrieveFn(ctx)
	if err != nil {
		span.SetTag(ext.Error, err)
		span.Finish()

		return nil, err
	}
	span.Finish()

	itemExp, keyExp := cfg.Expiration, cfg.KeyExpiration
	if cfg.ExpirationFunc != nil {
		itemExp, keyExp = cfg.ExpirationFunc(item)
	}

	if setErr := c.driver.Set(
		ctx,
		fullKey,
		item,
		c.wrapWithCompression(serializer, cfg.Compression),
		itemExp,
		keyExp,
	); setErr != nil {
		return nil, fmt.Errorf("failed to store item to driver: %w", setErr)
	}

	return item, nil
}
