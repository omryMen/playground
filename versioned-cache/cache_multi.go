package versionedcache

import (
	"context"
	"fmt"
	"strings"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type MultipleRetrieveFn func(ctx context.Context, keys []string) (map[string]interface{}, error)

func (c *Cache) MultiLoadOrStore(
	ctxIn context.Context,
	keys []string,
	retrieveFn MultipleRetrieveFn,
	serializer Serializer,
	deserializer Deserializer,
	cfg ItemConfig,
) ([]interface{}, error) {
	span, ctx := tracer.StartSpanFromContext(ctxIn, "cache_get",
		tracer.SpanType("cache"),
		//tracing.Tag(spanTagKeysFetched, len(keys)), // TODO: either change to metric or add Tag to telemetry
		//tracing.Tag(spanTagIsMultiFetch, true),
	)
	defer span.Finish()

	fullKeys := c.fullKeys(keys, cfg.Version)
	groupKey := strings.Join(fullKeys, "_")
	resultChan := c.sfg.DoChan(groupKey, func() (interface{}, error) {
		if c.opts.Bypass || cfg.Bypass {
			values, err := retrieveFn(ctx, keys)
			if err != nil {
				return nil, err
			}

			return mapInterfaceToSlice(values), nil
		}

		if cfg.Override {
			return c.multiRetrieveAndSet(ctx, fullKeys, retrieveFn, serializer, cfg, nil)
		}

		items, expiredKeys, err := c.driver.MGet(ctx, fullKeys, c.unwrapDeserializer(deserializer, cfg.Compression))
		if err != nil {
			span.SetTag(ext.Error, err)
			c.logger.Error("cache driver MGet", "msg", err)

			return c.multiRetrieveAndSet(ctx, fullKeys, retrieveFn, serializer, cfg, nil)
		}

		span.SetTag(spanTagKeysFound, len(items))
		span.SetTag(spanTagKeysExpired, len(expiredKeys))
		span.SetTag(spanTagKeysMissed, len(fullKeys)-len(items))

		if len(expiredKeys) > 0 {
			go func() {
				_, err, _ := c.sfg.Do(groupKey+"-async", func() (interface{}, error) {
					return c.multiRetrieveAndSet(context.Background(), expiredKeys, retrieveFn, serializer, cfg, nil)
				})

				if err != nil {
					c.logger.Error(fmt.Errorf("failed to update cache item in async mode: %w", err))
				}
			}()
		}

		if len(items) != len(fullKeys) {
			fullKeysToFetch := stringsPool.Get().([]string)
			defer func() {
				fullKeysToFetch = fullKeysToFetch[:0]
				stringsPool.Put(fullKeysToFetch) // nolint:staticcheck
			}()

			for _, fullKey := range fullKeys {
				if _, ok := items[fullKey]; !ok {
					fullKeysToFetch = append(fullKeysToFetch, fullKey)
				}
			}

			return c.multiRetrieveAndSet(ctx, fullKeysToFetch, retrieveFn, serializer, cfg, mapInterfaceToSlice(items))
		}

		return mapInterfaceToSlice(items), nil
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultChan:
		if result.Err != nil {
			span.SetTag(ext.Error, result.Err)
		}
		return result.Val.([]interface{}), result.Err
	}
}

func (c *Cache) multiRetrieveAndSet(
	ctxIn context.Context,
	fullKeys []string,
	retrieveFn MultipleRetrieveFn,
	serializer Serializer,
	cfg ItemConfig,
	items []interface{},
) ([]interface{}, error) {
	span, ctx := tracer.StartSpanFromContext(ctxIn, "cache_get_retrieve",
		tracer.SpanType("db"),
	)

	itemsMap, err := retrieveFn(ctx, c.keys(fullKeys, cfg.Version))
	if err != nil {
		span.SetTag(ext.Error, err)
		span.Finish()

		return nil, err
	}
	span.Finish()

	if items == nil {
		items = make([]interface{}, 0, len(itemsMap))
	}

	for key, item := range itemsMap {
		if setErr := c.driver.Set(
			ctx, c.fullKey(key, cfg.Version),
			item,
			c.wrapWithCompression(serializer, cfg.Compression),
			cfg.Expiration,
			cfg.KeyExpiration,
		); setErr != nil {
			return nil, fmt.Errorf("failed to store item to driver: %w", setErr)
		}

		items = append(items, item)
	}

	return items, nil
}

func mapInterfaceToSlice(v map[string]interface{}) []interface{} {
	r := make([]interface{}, 0, len(v))
	for _, vi := range v {
		r = append(r, vi)
	}

	return r
}
