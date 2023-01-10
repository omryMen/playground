package versionedcache

import (
	"time"
)

const (
	CompressionNone Compression = 0
	CompressionGzip Compression = 1
)

type Compression int

// ItemConfig cache item configuration.
type ItemConfig struct {

	// Expiration means after what time an item will be async updated if it was fetched.
	Expiration time.Duration

	// KeyExpiration means how long an item will be in cache.
	KeyExpiration time.Duration

	// ExpirationFunc calculates expiration and key expiration based on received item.
	ExpirationFunc func(item interface{}) (expiration time.Duration, keyExpiration time.Duration)

	// Bypass means that cache won't be used at all.
	Bypass bool

	// Override means that cache won't be used for read, but only for set.
	Override bool

	// Compression is a type of compression.
	Compression Compression

	// Version is additional modification of item key.
	// The key equals  prefix:version:key
	Version string

	// SourceRequestLockTTL defines lock duration for asynchronous item retrieval from a source.
	// 0 means no lock acquired (default), i.e. max number of concurrent requests to the origin equals the
	// number of processes running versioned-cache.
	// If SourceRequestLockTTL is non-zero, only one request to a source per defined duration can be sent.
	SourceRequestLockTTL time.Duration
}

// Clone creates a new instance of ItemConfig from existing one.
func (cc ItemConfig) Clone() *ItemConfig {
	return &ItemConfig{
		Override:      cc.Override,
		Bypass:        cc.Bypass,
		Expiration:    cc.Expiration,
		KeyExpiration: cc.KeyExpiration,
		Compression:   cc.Compression,
	}
}
