// Package cacheq provides typed shared query state, caching, retries, and background refreshes.
package cacheq

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// Client owns shared results of different types and merges concurrent requests by key.
// Reuse one client across components and call Close to stop background work.
type Client struct {
	// mu protects state transitions; loaders always run outside the lock.
	mu sync.Mutex
	// stats accumulates this client's cache decisions, load outcomes, and cleanup activity under mu.
	stats Stats
	// loadNow measures elapsed load time independently of the freshness clock; tests may replace it.
	loadNow func() time.Time
	// entries contains completed query state, including expired data for background refresh.
	entries map[any]entry
	// recency orders retained results and errors from least to most recently used.
	recency list.List
	// recencyEntries connects cached keys to their positions without scanning the cache.
	recencyEntries map[any]*list.Element
	// pending contains at most one active loader per key.
	pending map[any]*flight
	// observers receives shared state and tracks each subscriber's automatic loading permission.
	observers map[any]map[*subscription]struct{}
	// nextObserver orders subscriptions so automatic refresh chooses the oldest enabled consumer.
	nextObserver uint64
	// gcTasks owns at most one scheduled cleanup for each inactive cached key.
	gcTasks map[any]*gcTask
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

// entry retains one typed result, including stale data and the latest load error.
type entry struct {
	// typ binds this key to one static result type until its state and subscriptions are released.
	typ reflect.Type
	// value is shared, immutable query data.
	value any
	// hasData distinguishes a usable result from an initial failure.
	hasData bool
	// expiresAt controls reuse independently of how long data stays available.
	expiresAt time.Time
	// ordinary allows consumers to evaluate freshness independently for data without an explicit deadline.
	ordinary bool
	// invalidated forces ordinary data stale independently of consumer freshness after invalidation or failure.
	invalidated bool
	// updatedAt records the last installation of usable data.
	updatedAt time.Time
	// err records the most recent failure while earlier data can remain available.
	err error
}

// queryRequest owns an erased loader and the initiating consumer's copied request policy.
type queryRequest struct {
	// load produces data and an optional explicit expiration outside the client lock.
	load Loader[any]
	// options supplies the initiator's retry, timeout, and ordinary freshness settings.
	options Options
	// ordinary distinguishes fetchers from loaders with authoritative absolute deadlines.
	ordinary bool
}

// flight shares one loader's completion with all waiting callers.
type flight struct {
	// request preserves the initiator's policy while other consumers join the load.
	request queryRequest
	// startedAt marks the beginning of shared work, including scheduling and retry waits.
	startedAt time.Time
	// done closes after value and err have been published.
	done chan struct{}
	// cancel stops this load without canceling unrelated query keys.
	cancel context.CancelFunc
	// owner distinguishes caller cancellation from the client's configured timeout.
	owner context.Context
	// detached prevents explicit Cancel, Remove, and Set from restarting the canceled load.
	detached bool
	// value becomes visible when done closes.
	value any
	// err becomes visible when done closes.
	err error
	// invalidated prevents a refresh invalidated in flight from being marked fresh.
	invalidated bool
}

// NewClient constructs an independently owned client with optional freshness and retry policies.
func NewClient(options Options) *Client {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.RetryDelay == nil {
		options.RetryDelay = retryDelay
	}
	// The cancellation is transferred to Client.Close rather than a constructor defer.
	ctx, cancel := context.WithCancel(context.Background()) // #nosec G118 -- Close owns the client's lifetime.
	return &Client{
		entries: make(map[any]entry), pending: make(map[any]*flight),
		recencyEntries: make(map[any]*list.Element),
		observers:      make(map[any]map[*subscription]struct{}),
		gcTasks:        make(map[any]*gcTask), gcAfterFunc: scheduleGC,
		options: options, ctx: ctx, cancel: cancel,
		loadNow: time.Now,
	}
}

// Invalidate marks a key, a []any batch, or keys matching func(any) bool stale while retaining data.
// By default it refreshes queries with enabled observers and loaders; the last option wins.
// Batches are validated before any changes, and duplicate keys are invalidated once.
// Predicates run outside the client lock against a snapshot of cached or loading keys.
// Newly added keys are excluded, and keys removed before invalidation are skipped.
// Nil targets and predicates return ErrInvalidKey; empty batches do nothing.
// Closed clients return ErrClosed without evaluating predicates.
func (c *Client) Invalidate(target any, options ...InvalidateOptions) error {
	mode := RefetchObserved
	for _, option := range options {
		mode = option.Refetch
	}
	switch selected := target.(type) {
	case []any:
		return c.invalidateKeys(selected, mode)
	case func(any) bool:
		return c.invalidateMatching(selected, mode)
	default:
		return c.invalidateKeys([]any{target}, mode)
	}
}

// invalidateKeys validates the entire selection before invalidating each distinct key.
func (c *Client) invalidateKeys(keys []any, mode RefetchMode) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	for _, key := range keys {
		if err := c.checkLocked(key, nil); err != nil {
			return err
		}
	}
	seen := make(map[any]struct{}, len(keys))
	for _, key := range keys {
		// Repeating a key would invalidate the refresh just started for its
		// first occurrence, leaving an otherwise current result stale.
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		c.invalidateLocked(key, mode)
	}
	return nil
}

// invalidateMatching evaluates user code outside the lock and invalidates surviving selected keys.
func (c *Client) invalidateMatching(matches func(any) bool, mode RefetchMode) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if matches == nil {
		c.mu.Unlock()
		return fmt.Errorf("%w: nil invalidation predicate", ErrInvalidKey)
	}
	keys := make([]any, 0, len(c.entries)+len(c.pending))
	for key := range c.entries {
		keys = append(keys, key)
	}
	for key := range c.pending {
		if _, cached := c.entries[key]; !cached {
			keys = append(keys, key)
		}
	}
	c.mu.Unlock()
	selected := keys[:0]
	for _, key := range keys {
		if matches(key) {
			selected = append(selected, key)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	for _, key := range selected {
		// A predicate or concurrent cleanup can remove state while matching
		// runs outside the lock. Do not recreate that removed cache entry.
		if _, cached := c.entries[key]; !cached && c.pending[key] == nil {
			continue
		}
		c.invalidateLocked(key, mode)
	}
	return nil
}

// invalidateLocked applies one key's invalidation and refresh policy while the client lock is held.
func (c *Client) invalidateLocked(key any, refetch RefetchMode) {
	cached := c.entries[key]
	cached.expiresAt = time.Time{}
	cached.invalidated = true
	c.entries[key] = cached
	if pending := c.pending[key]; pending != nil {
		pending.invalidated = true
	} else if refetch == RefetchObserved {
		if request := c.observedRequestLocked(key); request.load != nil {
			c.startLocked(c.ctx, key, request)
		}
	}
	c.touchCapacityLocked(key)
	c.touchGCLocked(key)
	c.notifyLocked(key)
}

// Cancel stops an active load while retaining previously completed data.
func (c *Client) Cancel(key any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkLocked(key, nil); err != nil {
		return err
	}
	c.cancelLocked(key)
	c.touchGCLocked(key)
	c.notifyLocked(key)
	return nil
}

// Remove discards a key's result and prevents its active loader from restoring removed data.
func (c *Client) Remove(key any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkLocked(key, nil); err != nil {
		return err
	}
	c.cancelLocked(key)
	c.discardLocked(key)
	c.stopGCLocked(key)
	c.notifyLocked(key)
	return nil
}

// Clear removes all results and cancels active loads while retaining live subscriptions.
func (c *Client) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.pending {
		c.cancelLocked(key)
	}
	for key := range c.entries {
		c.discardLocked(key)
	}
	c.clearGCLocked()
	for key := range c.observers {
		c.notifyLocked(key)
	}
}

// Close cancels active work, closes subscriptions, and prevents new requests; it is idempotent.
func (c *Client) Close() {
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
		for observer := range observers {
			observer.close()
		}
	}
	clear(c.observers)
	clear(c.entries)
	c.recency.Init()
	clear(c.recencyEntries)
	c.clearGCLocked()
}

// startLocked publishes pending state before starting one asynchronous load.
func (c *Client) startLocked(ctx context.Context, key any, request queryRequest) *flight {
	c.stopGCLocked(key)
	owner := ctx
	ctx, cancel := context.WithCancel(ctx)
	if request.options.Timeout > 0 {
		var timeoutCancel context.CancelFunc
		ctx, timeoutCancel = context.WithTimeout(ctx, request.options.Timeout)
		previousCancel := cancel
		cancel = func() { timeoutCancel(); previousCancel() }
	}
	pending := &flight{
		done:      make(chan struct{}),
		cancel:    cancel,
		owner:     owner,
		startedAt: c.loadNow(),
		request:   request,
	}
	c.pending[key] = pending
	c.stats.Loads++
	c.notifyLocked(key)
	go c.execute(ctx, key, pending)
	return pending
}

// execute publishes a completed load only while it still owns the key's active request.
func (c *Client) execute(ctx context.Context, key any, pending *flight) {
	defer pending.cancel()
	value, expiresAt, err := c.run(ctx, pending.request)
	if ctx.Err() != nil {
		value, expiresAt, err = nil, time.Time{}, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.TotalLoadDuration += c.loadNow().Sub(pending.startedAt)
	switch {
	case err == nil:
		c.stats.LoadSuccesses++
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		c.stats.LoadCancellations++
	default:
		c.stats.LoadFailures++
	}
	if c.pending[key] == pending {
		delete(c.pending, key)
		if !c.closed && ctx.Err() == nil {
			c.expireDataLocked(key)
			cached := c.entries[key]
			if err == nil || c.options.Clock().Before(expiresAt) {
				cached.value, cached.hasData, cached.updatedAt = value, true, c.options.Clock()
				cached.ordinary = pending.request.ordinary
				if cached.ordinary {
					expiresAt = cached.updatedAt.Add(pending.request.options.StaleTime)
				}
				expiresAt = c.limitExpiry(cached.updatedAt, expiresAt)
			}
			if pending.invalidated {
				expiresAt = time.Time{}
			}
			cached.invalidated = pending.invalidated || err != nil
			cached.err, cached.expiresAt = err, expiresAt
			c.entries[key] = cached
			c.touchCapacityLocked(key)
		}
		c.touchGCLocked(key)
	}
	pending.value, pending.err = value, err
	c.notifyLocked(key)
	close(pending.done)
}

// run applies bounded retries and stops immediately for cancellation or deadline errors.
func (c *Client) run(ctx context.Context, request queryRequest) (any, time.Time, error) {
	options := request.options
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil, time.Time{}, ctx.Err()
		}
		if attempt > 0 {
			c.mu.Lock()
			c.stats.Retries++
			c.mu.Unlock()
		}
		value, expiresAt, err := request.load(ctx)
		stopped := ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
		shouldRetry := err != nil && attempt < options.Retry && !stopped
		if !shouldRetry {
			return value, expiresAt, err
		}
		if options.RetryIf != nil && !options.RetryIf(err) {
			return value, expiresAt, err
		}
		if err := ctx.Err(); err != nil {
			return value, expiresAt, err
		}
		timer := time.NewTimer(options.RetryDelay(attempt + 1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, time.Time{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// snapshotLocked removes data beyond MaxAge and derives state without scheduling a request.
func (c *Client) snapshotLocked(key any) Snapshot[any] {
	c.expireDataLocked(key)
	cached := c.entries[key]
	state := Snapshot[any]{
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

// notifyLocked publishes the latest state while the client lock prevents subscription closure races.
func (c *Client) notifyLocked(key any) {
	if len(c.observers[key]) == 0 {
		return
	}
	state := c.snapshotLocked(key)
	for observer := range c.observers[key] {
		observer.publish(c.withFreshnessLocked(key, state, observer.request.options))
	}
}

// cancelLocked detaches the active load so canceled or obsolete results cannot overwrite current data.
func (c *Client) cancelLocked(key any) {
	if pending := c.pending[key]; pending != nil {
		pending.detached = true
		pending.cancel()
		delete(c.pending, key)
	}
}

// checkLocked rejects closed clients, unsafe keys, and incompatible static result types without changing state.
func (c *Client) checkLocked(key any, typ reflect.Type) error {
	if c.closed {
		return ErrClosed
	}
	if key == nil || !reflect.ValueOf(key).Comparable() {
		return fmt.Errorf("%w: %T", ErrInvalidKey, key)
	}
	cached := c.entries[key]
	typeMismatch := typ != nil && cached.typ != nil && cached.typ != typ
	if typeMismatch {
		return fmt.Errorf(
			"%w: key %v contains %v, requested %v",
			ErrTypeMismatch,
			key,
			cached.typ,
			typ,
		)
	}
	return nil
}

// bindLocked installs a static type before a result or flight can become visible to other callers.
func (c *Client) bindLocked(key any, typ reflect.Type) error {
	if err := c.checkLocked(key, typ); err != nil {
		return err
	}
	cached := c.entries[key]
	cached.typ = typ
	c.entries[key] = cached
	return nil
}

// discardLocked preserves active type bindings while clearing completed data and capacity bookkeeping.
func (c *Client) discardLocked(key any) {
	c.forgetCapacityLocked(key)
	if len(c.observers[key]) > 0 || c.pending[key] != nil {
		c.entries[key] = entry{typ: c.entries[key].typ}
		return
	}
	delete(c.entries, key)
}

// observedRequestLocked selects an enabled handle deterministically by subscription order.
func (c *Client) observedRequestLocked(key any) queryRequest {
	var selected *subscription
	for observer := range c.observers[key] {
		if observer.enabled && (selected == nil || observer.order < selected.order) {
			selected = observer
		}
	}
	if selected != nil {
		return selected.request
	}
	return queryRequest{}
}

// withFreshnessLocked applies one consumer's freshness to ordinary, non-invalidated shared data.
func (c *Client) withFreshnessLocked(key any, state Snapshot[any], options Options) Snapshot[any] {
	cached := c.entries[key]
	if cached.ordinary && state.HasData && !cached.invalidated {
		state.ExpiresAt = c.limitExpiry(cached.updatedAt, cached.updatedAt.Add(options.StaleTime))
		state.Stale = !options.Clock().Before(state.ExpiresAt)
	}
	return state
}
