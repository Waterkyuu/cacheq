// Package query provides typed shared query state, caching, retries, and background refreshes.
package query

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Client owns shared results and merges concurrent requests by key.
// Reuse one client across components and call Close to stop background work.
type Client[K comparable, V any] struct {
	// mu protects state transitions; loaders always run outside the lock.
	mu sync.Mutex
	// entries contains completed query state, including expired data for background refresh.
	entries map[K]entry[V]
	// pending contains at most one active loader per key.
	pending map[K]*flight[V]
	// observers receives the latest state for each subscribed key.
	observers map[K]map[chan Snapshot[V]]struct{}
	// gcTasks owns at most one scheduled cleanup for each inactive cached key.
	gcTasks map[K]*gcTask
	// gcAfterFunc schedules cleanup and returns a stop function; tests can replace the timer source.
	gcAfterFunc func(time.Duration, func()) func()
	// options supplies freshness and request policies.
	options Options
	// ctx owns background loads until Close is called.
	ctx context.Context
	// cancel stops the client's background lifetime.
	cancel context.CancelFunc
	// closed prevents new work after resource release.
	closed bool
}

// entry retains one completed query result and its refresh function.
type entry[V any] struct {
	// value is shared, immutable query data.
	value V
	// hasData distinguishes a usable result from an initial failure.
	hasData bool
	// expiresAt controls reuse independently of how long data stays available.
	expiresAt time.Time
	// updatedAt records the last installation of usable data.
	updatedAt time.Time
	// err records the most recent failure while earlier data can remain available.
	err error
	// load permits an observed query to refresh after invalidation.
	load Loader[V]
}

// flight shares one loader's completion with all waiting callers.
type flight[V any] struct {
	// done closes after value and err have been published.
	done chan struct{}
	// cancel stops this load without canceling unrelated query keys.
	cancel context.CancelFunc
	// owner distinguishes caller cancellation from the client's configured timeout.
	owner context.Context
	// detached prevents explicit Cancel, Remove, and Set from restarting the canceled load.
	detached bool
	// value becomes visible when done closes.
	value V
	// err becomes visible when done closes.
	err error
	// invalidated prevents a refresh invalidated in flight from being marked fresh.
	invalidated bool
}

// NewClient constructs an independently owned client with optional freshness and retry policies.
func NewClient[K comparable, V any](options Options) *Client[K, V] {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.RetryDelay == nil {
		options.RetryDelay = retryDelay
	}
	// The cancellation is transferred to Client.Close rather than a constructor defer.
	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118 -- Close owns the client's lifetime.
	return &Client[K, V]{
		entries: make(map[K]entry[V]), pending: make(map[K]*flight[V]),
		observers: make(map[K]map[chan Snapshot[V]]struct{}),
		gcTasks:   make(map[K]*gcTask), gcAfterFunc: scheduleGC,
		options: options, ctx: ctx, cancel: cancel,
	}
}

// Fetch waits for fresh data using StaleTime to determine the result's expiration.
func (c *Client[K, V]) Fetch(ctx context.Context, key K, fetch Fetcher[V]) (V, error) {
	return c.FetchWithExpiry(ctx, key, c.loader(fetch))
}

// FetchWithExpiry returns a fresh result or shares one loader with concurrent callers of the same key.
// Loaders control absolute freshness, preserving the age of data restored from disk.
// A canceled waiter leaves the owner's load running. If the owner cancels, remaining callers may retry.
func (c *Client[K, V]) FetchWithExpiry(ctx context.Context, key K, load Loader[V]) (V, error) {
	var zero V
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return zero, ErrClosed
		}
		if cached, ok := c.entries[key]; ok && c.options.Clock().Before(cached.expiresAt) {
			c.touchGCLocked(key)
			c.mu.Unlock()
			return cached.value, cached.err
		}
		pending := c.pending[key]
		if pending == nil {
			pending = c.startLocked(ctx, key, load)
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-pending.done:
			if err := ctx.Err(); err != nil {
				return zero, err
			}
			// A component can dismiss its request while another still needs
			// the shared value. Retry using the remaining caller's context.
			if pending.owner.Err() != nil && !pending.detached {
				continue
			}
			return pending.value, pending.err
		}
	}
}

// Query returns current data immediately and refreshes stale or missing data in the background.
func (c *Client[K, V]) Query(key K, fetch Fetcher[V]) Snapshot[V] {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed && c.snapshotLocked(key).Stale && c.pending[key] == nil {
		c.startLocked(c.ctx, key, c.loader(fetch))
	}
	c.touchGCLocked(key)
	return c.snapshotLocked(key)
}

// Snapshot reads data and freshness without starting a network request.
func (c *Client[K, V]) Snapshot(key K) Snapshot[V] {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.touchGCLocked(key)
	return c.snapshotLocked(key)
}

// Prefetch warms the shared cache using the same freshness and request policy as Fetch.
func (c *Client[K, V]) Prefetch(ctx context.Context, key K, fetch Fetcher[V]) error {
	_, err := c.Fetch(ctx, key, fetch)
	return err
}

// Refetch requests a new result even if existing data is fresh, sharing any active load.
func (c *Client[K, V]) Refetch(ctx context.Context, key K, fetch Fetcher[V]) (V, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		var zero V
		return zero, ErrClosed
	}
	cached := c.entries[key]
	cached.expiresAt = time.Time{}
	c.entries[key] = cached
	c.touchGCLocked(key)
	c.notifyLocked(key)
	c.mu.Unlock()
	return c.Fetch(ctx, key, fetch)
}

// Invalidate marks data stale and refreshes subscribed queries that have a retained loader.
func (c *Client[K, V]) Invalidate(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.invalidateLocked(key, RefetchObserved)
}

// InvalidateMany marks each distinct key stale while retaining its data and notifying subscribers.
// Zero-value options refresh subscribed queries with retained loaders; RefetchNone defers new work.
// Active loads are neither canceled nor duplicated, and their results remain stale on completion.
// An empty key list or a closed client has no effect.
func (c *Client[K, V]) InvalidateMany(keys []K, options InvalidateOptions) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	seen := make(map[K]struct{}, len(keys))
	for _, key := range keys {
		// Repeating a key would invalidate the refresh just started for its
		// first occurrence, leaving an otherwise current result stale.
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		c.invalidateLocked(key, options.Refetch)
	}
}

// invalidateLocked applies one key's invalidation and refresh policy while the client lock is held.
func (c *Client[K, V]) invalidateLocked(key K, refetch RefetchMode) {
	cached := c.entries[key]
	cached.expiresAt = time.Time{}
	c.entries[key] = cached
	if pending := c.pending[key]; pending != nil {
		pending.invalidated = true
	} else if refetch == RefetchObserved {
		if len(c.observers[key]) > 0 && cached.load != nil {
			c.startLocked(c.ctx, key, cached.load)
		}
	}
	c.touchGCLocked(key)
	c.notifyLocked(key)
}

// Set installs shared data and stops an older load from overwriting an optimistic or local update.
func (c *Client[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.cancelLocked(key)
	now := c.options.Clock()
	c.entries[key] = entry[V]{
		value: value, hasData: true, updatedAt: now,
		expiresAt: now.Add(c.options.StaleTime), load: c.entries[key].load,
	}
	c.touchGCLocked(key)
	c.notifyLocked(key)
}

// Subscribe delivers the initial and latest query states; slow readers may skip intermediate states.
// Canceling the returned subscription closes its channel and releases the observer.
func (c *Client[K, V]) Subscribe(key K) (<-chan Snapshot[V], func()) {
	c.mu.Lock()
	updates := make(chan Snapshot[V], 1)
	updates <- c.snapshotLocked(key)
	if c.closed {
		close(updates)
	} else {
		if c.observers[key] == nil {
			c.observers[key] = make(map[chan Snapshot[V]]struct{})
		}
		c.observers[key][updates] = struct{}{}
		c.stopGCLocked(key)
	}
	c.mu.Unlock()
	var once sync.Once
	return updates, func() {
		once.Do(func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			if _, exists := c.observers[key][updates]; exists {
				delete(c.observers[key], updates)
				if len(c.observers[key]) == 0 {
					delete(c.observers, key)
				}
				close(updates)
				c.touchGCLocked(key)
			}
		})
	}
}

// Cancel stops an active load while retaining previously completed data.
func (c *Client[K, V]) Cancel(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelLocked(key)
	c.touchGCLocked(key)
	c.notifyLocked(key)
}

// Remove discards a key's result and prevents its active loader from restoring removed data.
func (c *Client[K, V]) Remove(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelLocked(key)
	delete(c.entries, key)
	c.stopGCLocked(key)
	c.notifyLocked(key)
}

// Clear removes all results and cancels active loads while retaining live subscriptions.
func (c *Client[K, V]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.pending {
		c.cancelLocked(key)
	}
	clear(c.entries)
	c.clearGCLocked()
	for key := range c.observers {
		c.notifyLocked(key)
	}
}

// Close cancels active work, closes subscriptions, and prevents new requests; it is idempotent.
func (c *Client[K, V]) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	c.cancel()
	for key := range c.pending {
		c.cancelLocked(key)
	}
	for _, observers := range c.observers {
		for updates := range observers {
			close(updates)
		}
	}
	clear(c.observers)
	clear(c.entries)
	c.clearGCLocked()
}

// loader converts a fetcher into an absolute-expiration loader without introducing transport ownership.
func (c *Client[K, V]) loader(fetch Fetcher[V]) Loader[V] {
	return func(ctx context.Context) (V, time.Time, error) {
		value, err := fetch(ctx)
		if err != nil {
			return value, time.Time{}, err
		}
		return value, c.options.Clock().Add(c.options.StaleTime), nil
	}
}

// startLocked publishes pending state before starting one asynchronous load.
func (c *Client[K, V]) startLocked(ctx context.Context, key K, load Loader[V]) *flight[V] {
	c.stopGCLocked(key)
	owner := ctx
	ctx, cancel := context.WithCancel(ctx)
	if c.options.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, c.options.Timeout)
		previousCancel := cancel
		cancel = func() { timeoutCancel(); previousCancel() }
	}
	pending := &flight[V]{done: make(chan struct{}), cancel: cancel, owner: owner}
	c.pending[key] = pending
	c.notifyLocked(key)
	go c.execute(ctx, key, pending, load)
	return pending
}

// execute publishes a completed load only while it still owns the key's active request.
func (c *Client[K, V]) execute(ctx context.Context, key K, pending *flight[V], load Loader[V]) {
	defer pending.cancel()
	value, expiresAt, err := c.run(ctx, load)
	if ctx.Err() != nil {
		var zero V
		value, expiresAt, err = zero, time.Time{}, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pending[key] == pending {
		delete(c.pending, key)
		if !c.closed && ctx.Err() == nil {
			cached := c.entries[key]
			if err == nil || c.options.Clock().Before(expiresAt) {
				cached.value, cached.hasData, cached.updatedAt = value, true, c.options.Clock()
			}
			if pending.invalidated {
				expiresAt = time.Time{}
			}
			cached.err, cached.expiresAt, cached.load = err, expiresAt, load
			c.entries[key] = cached
		}
		c.touchGCLocked(key)
	}
	pending.value, pending.err = value, err
	c.notifyLocked(key)
	close(pending.done)
}

// run applies bounded retries and stops immediately for cancellation or deadline errors.
func (c *Client[K, V]) run(ctx context.Context, load Loader[V]) (V, time.Time, error) {
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			var zero V
			return zero, time.Time{}, ctx.Err()
		}
		value, expiresAt, err := load(ctx)
		if err == nil || attempt >= c.options.Retry || ctx.Err() != nil ||
			errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return value, expiresAt, err
		}
		timer := time.NewTimer(c.options.RetryDelay(attempt + 1))
		select {
		case <-ctx.Done():
			timer.Stop()
			var zero V
			return zero, time.Time{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// snapshotLocked derives freshness from the clock without scheduling a request.
func (c *Client[K, V]) snapshotLocked(key K) Snapshot[V] {
	cached := c.entries[key]
	state := Snapshot[V]{
		Data: cached.value, HasData: cached.hasData, Err: cached.err,
		Stale:    !cached.hasData || !c.options.Clock().Before(cached.expiresAt),
		Fetching: c.pending[key] != nil, UpdatedAt: cached.updatedAt, ExpiresAt: cached.expiresAt,
	}
	switch {
	case c.closed:
		state.Status, state.Err = Error, ErrClosed
	case cached.err != nil:
		state.Status = Error
	case cached.hasData:
		state.Status = Success
	case state.Fetching:
		state.Status = Pending
	default:
		state.Status = Idle
	}
	return state
}

// notifyLocked publishes the most recent state without allowing a slow observer to block a loader.
func (c *Client[K, V]) notifyLocked(key K) {
	if len(c.observers[key]) == 0 {
		return
	}
	state := c.snapshotLocked(key)
	for updates := range c.observers[key] {
		select {
		case updates <- state:
		default:
			select {
			case <-updates:
			default:
			}
			select {
			case updates <- state:
			default:
			}
		}
	}
}

// cancelLocked detaches the active load so canceled or obsolete results cannot overwrite current data.
func (c *Client[K, V]) cancelLocked(key K) {
	if pending := c.pending[key]; pending != nil {
		pending.detached = true
		pending.cancel()
		delete(c.pending, key)
	}
}
