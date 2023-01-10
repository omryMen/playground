Cache
=================

> Note: To use this library tracing exporter to DataDog should be configured.
> Additional information can be found [here.](https://github.com/DataDog/dd-trace-go)


This library main goal is to simplify cache usage inside projects.
To create new instance of cache you need to use constructor function `NewCache`, currently it supports following options:
* `WithName(name string)` - to set cache instance name, will be used for logging and metrics.
* `WithCacheKeyPrefix(prefix string)` - all cache item keys will have this prefix. The separator is `:`. 
* `WithBypass(bypass bool)` - to skip cache layer and fetch items directly from a retriever.
* `WithUseInmemoryCache(u bool)` - indicates use or not additional in memory cache layer before standard one (a standard one you pass using constructor function). If value is `true` then `NewTwoLevelCacheDecorator` will be used as driver, where level one will be `DefaultInMemoryCache` and level two - passed one.
* `WithInmemoryCacheDriver(d driver)` - method to have ability to specify level one cache in case when `UseInmemoryCache` is `true`.
* `WithKeyLocker(d locker)` - method to have ability to specify locker. 


### Drivers
Currently, two drivers supported:
* **InMemoryCache** - simple in memory cache without de/serialization, under the hood [Ristretto](github.com/dgraph-io/ristretto) is used.
* **RedisClusterCache** - implementation for RedisCluster client.


### Decorators 
* **TwoLevelCacheDecorator** - decorator used to combine cache layers, it accepts two cache drivers, where level one will be used as a cache with higher priority.


### Lockers
Locker needed to ensure that only one origin request can be sent concurrently. It helps to prevent unnecessary calls of "retrieve function" if versioned cache launched in multiple processes.
Currently, only redis based implementation supported.


### Usage
In the following example you see how to use cache to fetch and load single items:
```go
ctx := context.Background()
logger := zerolog.NewLogger()

driver, err := drivers.NewRedisClusterCache(address, poolSize, timeout)
if err != nil {
    panic(err)
}

memDriver, err := drivers.NewDefaultInMemoryCache()
if err != nil {
    panic(err)
}

cache, err := NewCache(driver, logger,
    WithName("cache_for_model"),
    WithCacheKeyPrefix("prefix"),
    WithUseInmemoryCache(true),
    WithInmemoryCacheDriver(memDriver),
)
if err != nil {
    panic(err)
}

items, err := cache.SingleLoadOrStore(
    ctx,
    "item123",
    // retrieve function
    func(ctx context.Context) (interface{}, error) {
        return &Model{ID: "item123"}, nil
    },
    // serialize function
    func(ctx context.Context, item interface{}) ([]byte, error) {
        return json.Marshal(ctx)
    },
    // deserialize function
    func(ctx context.Context, data []byte) (interface{}, error) {
        inst := &Model{}
        err := json.Unmarshal(data, inst)

        return inst, err
    },
    ItemConfig{
        Expiration:    40 * time.Minute,
        KeyExpiration: time.Hour,
        Compression:   CompressionGzip, // please use this compression if you really need it
    },
)
```

And here we use it for caching and fetching multiple items at once:
```go
ctx := context.Background()
logger := zerolog.NewLogger()

driver, err := drivers.NewRedisClusterCache(address, poolSize, timeout)
if err != nil {
    panic(err)
}

cache, err := NewCache(driver, logger,
    WithName("cache_for_model"),
    WithCacheKeyPrefix("prefix"),
)
if err != nil {
    panic(err)
}

items, err := cache.MultiLoadOrStore(
    ctx,
    "item123",
    // retrieve function
    func(ctx context.Context, keys []string) (map[string]interface{}, error) {
        return map[string]interface{}{
            "item123": &Model{ID: "item123"},
        }, nil
    },
    // serialize function
    func(ctx context.Context, item interface{}) ([]byte, error) {
        return json.Marshal(ctx)
    },
    // deserialize function
    func(ctx context.Context, data []byte) (interface{}, error) {
        inst := &Model{}
        err := json.Unmarshal(data, inst)

        return inst, err
    },
    ItemConfig{
        Expiration:    40 * time.Minute,
        KeyExpiration: time.Hour,
        Compression:   CompressionGzip, // please use this compression if you really need it
    },
)
```