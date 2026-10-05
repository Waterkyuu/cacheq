// Package query provides shared in-memory query caching without owning business data or persistence.
package query

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Cache reuses query results until their loader-supplied expiration and merges concurrent loads by key.
// Values are shared between callers and must be treated as immutable.
type Cache[K comparable, V any] struct {
	// mu protects entries and pending loads; loaders run outside this lock.
	mu sync.Mutex
	// entries retains completed results, including errors with an explicit retry expiration.
	entries map[K]entry[V]
	// pending identifies the single active loader for each key.
	pending map[K]*flight[V]
	// now supplies expiration checks independently of wall-clock time in tests.
	now func() time.Time
}

// entry holds one completed query result and the time after which it must be refreshed.
type entry[V any] struct {
	// value is the immutable result supplied by the loader.
	value V
	// expiresAt determines whether this result can be reused.
	expiresAt time.Time
	// err retains a loader failure when the loader explicitly requests a retry delay.
	err error
}

// flight shares a loader's completion with callers waiting for the same key.
type flight[V any] struct {
	// done closes after value and err have been published.
	done chan struct{}
	// value is the result made visible by closing done.
	value V
	// err is the loader outcome made visible by closing done.
	err error
}

// New constructs an independently owned cache with an explicit expiration clock.
func New[K comparable, V any](now func() time.Time) *Cache[K, V] {
	return &Cache[K, V]{
		entries: make(map[K]entry[V]), pending: make(map[K]*flight[V]), now: now,
	}
}

// Get returns a fresh result or runs load once for concurrent callers of the same key.
// The loader supplies an absolute expiration, allowing disk freshness and retry delays to be preserved.
// An expiration in the past disables caching. Canceled callers never publish a cached result;
// canceling a waiter does not cancel another caller's load.
func (c *Cache[K, V]) Get(
	ctx context.Context,
	key K,
	load func(context.Context) (V, time.Time, error),
) (V, error) {
	var zero V
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		c.mu.Lock()
		if cached, ok := c.entries[key]; ok && c.now().Before(cached.expiresAt) {
			c.mu.Unlock()
			return cached.value, cached.err
		}
		if pending, ok := c.pending[key]; ok {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-pending.done:
				// A dismissed caller can cancel the shared loader while another
				// caller still needs the value. Let the remaining caller retry.
				if errors.Is(pending.err, context.Canceled) || errors.Is(pending.err, context.DeadlineExceeded) {
					continue
				}
				if err := ctx.Err(); err != nil {
					return zero, err
				}
				return pending.value, pending.err
			}
		}
		pending := &flight[V]{done: make(chan struct{})}
		c.pending[key] = pending
		c.mu.Unlock()

		value, expiresAt, err := load(ctx)
		if canceled := ctx.Err(); canceled != nil {
			value, expiresAt, err = zero, time.Time{}, canceled
		}
		c.mu.Lock()
		delete(c.pending, key)
		if c.now().Before(expiresAt) {
			c.entries[key] = entry[V]{value: value, expiresAt: expiresAt, err: err}
		}
		pending.value, pending.err = value, err
		close(pending.done)
		c.mu.Unlock()
		return value, err
	}
}
