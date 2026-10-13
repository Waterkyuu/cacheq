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
	// eventSubscriptions indexes diagnostic streams by exact key; nil owns all-key subscriptions under mu.
	eventSubscriptions map[any]map[*EventSubscription]struct{}
	// eventSequence orders emitted diagnostic records without retaining history.
	eventSequence uint64
	// nextLoadID assigns operation identities even without active diagnostic subscriptions.
	nextLoadID uint64
	// loadNow measures elapsed load time independently of the freshness clock; tests may replace it.
	loadNow func() time.Time
	// nextRevision distinguishes writes even when a key is removed and recreated.
	nextRevision uint64
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
	// tags retains the union of labels registered for this key until its query state is released.
	tags []string
	// revision changes when data or its invalidation state is replaced.
	revision uint64
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
	// fetchedAt preserves source freshness when sharedExpiresAt identifies a shared result.
	fetchedAt time.Time
	// sharedExpiresAt caps every consumer's freshness for a shared result; zero means no shared cap.
	sharedExpiresAt time.Time
	// err records the most recent failure while earlier data can remain available.
	err error
	// staleReason records the latest explicit stale cause without keeping removed-key history.
	staleReason EventReason
}

// queryRequest owns an erased loader and the initiating consumer's copied request policy.
type queryRequest struct {
	// tags owns copied labels supplied by a definition; admission merges them into the shared entry.
	tags []string
	// load produces data and an optional explicit expiration outside the client lock.
	load Loader[any]
	// shared optionally performs typed shared-cache access within the existing loader attempt.
	shared *sharedQuery
	// options supplies the initiator's retry, timeout, and ordinary freshness settings.
	options Options
	// ordinary distinguishes fetchers from loaders with authoritative absolute deadlines.
	ordinary bool
	// trigger identifies the API decision initiating or joining this request.
	trigger EventTrigger
}

// flight shares one loader's completion with all waiting callers.
type flight struct {
	// id identifies this shared operation within its Client.
	id uint64
	// reason describes the initiating consumer's stale decision or forced refresh.
	reason EventReason
	// joined counts additional joins independently of current waiter count.
	joined uint64
	// attempts counts actual loader invocations under the Client lock.
	attempts int
	// discardReason identifies an explicit operation that detached this load.
	discardReason EventReason
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
	// refresh reports the outcome to callers waiting for this invalidation-triggered load.
	refresh *refreshCompletion
	// followup coalesces invalidations requiring a load after this one finishes.
	followup *refreshCompletion
	// followupLoad prevents a chain of automatic follow-ups from growing without bound.
	followupLoad bool
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

// Invalidate marks targets selected by an exact key, []any, func(any) bool, or QueryFilter stale.
// Completed data is retained while selected queries become stale.
// By default it refreshes enabled observed queries only when no load is active; the last option wins.
// RefetchAfterLoad optionally coalesces in-flight invalidations into one bounded follow-up.
// Batches are validated before changes; predicates run outside the lock against existing keys.
// Filter and predicate selection use existing keys only, exclude additions, and skip removed keys.
// Nil targets or direct predicates are invalid; a nil QueryFilter.Predicate imposes no restriction.
// Empty batches succeed; closed clients return ErrClosed without evaluating predicates.
func (c *Client) Invalidate(target any, options ...InvalidateOptions) error {
	_, err := c.invalidate(target, lastInvalidateOptions(options), nil)
	return err
}

// lastInvalidateOptions applies the existing last-option-wins convention.
func lastInvalidateOptions(options []InvalidateOptions) InvalidateOptions {
	if len(options) == 0 {
		return InvalidateOptions{}
	}
	return options[len(options)-1]
}

// invalidate applies shared target selection and records the scheduled refresh for each selected key.
func (c *Client) invalidate(
	target any,
	options InvalidateOptions,
	waitCtx context.Context,
) ([]*refreshCompletion, error) {
	refreshes := make([]*refreshCompletion, 0)
	err := c.withQueryTargets(target, func(key any) {
		refresh := c.invalidateLocked(key, options, waitCtx)
		if waitCtx != nil {
			refreshes = append(refreshes, refresh)
		}
	})
	if err != nil {
		return nil, err
	}
	return refreshes, nil
}

// invalidateLocked marks stale state and captures the exact refresh required by this invalidation.
func (c *Client) invalidateLocked(key any, options InvalidateOptions, waitCtx context.Context) *refreshCompletion {
	cached := c.entries[key]
	cached.expiresAt = time.Time{}
	cached.invalidated = true
	cached.staleReason = ReasonInvalidated
	c.advanceRevisionLocked(&cached)
	c.entries[key] = cached
	event := Event{Key: key, Kind: EventInvalidated, Trigger: TriggerInvalidate, Reason: ReasonInvalidated}
	pending := c.pending[key]
	if pending != nil {
		pending.invalidated = true
		event.LoadID = pending.id
	}
	c.emitEventLocked(event)
	refresh := c.scheduleRefreshLocked(key, pending, options, waitCtx)
	if refresh == nil && waitCtx != nil {
		refresh = newRefreshCompletion(key)
		refresh.finish(false, nil)
	}
	c.touchCapacityLocked(key)
	c.touchGCLocked(key)
	c.notifyLocked(key)
	return refresh
}

// Cancel stops selected active loads while retaining previously completed data and subscriptions.
// Targets accept an exact key, []any, func(any) bool, or QueryFilter with the same rules as Invalidate.
func (c *Client) Cancel(target any) error {
	return c.withQueryTargets(target, func(key any) {
		c.cancelLocked(key, ReasonCancel)
		c.touchGCLocked(key)
		c.notifyLocked(key)
	})
}

// Remove discards selected results and prevents their active loaders from restoring removed data.
// Targets accept an exact key, []any, func(any) bool, or QueryFilter; live subscriptions remain usable.
func (c *Client) Remove(target any) error {
	return c.withQueryTargets(target, func(key any) {
		c.cancelLocked(key, ReasonRemove)
		c.discardLocked(key, ReasonRemove)
		c.stopGCLocked(key)
		c.notifyLocked(key)
	})
}

// Clear removes all results and cancels active loads while retaining live subscriptions.
func (c *Client) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key := range c.pending {
		c.cancelLocked(key, ReasonClear)
	}
	for key := range c.entries {
		c.discardLocked(key, ReasonClear)
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
		c.cancelLocked(key, ReasonClientClosed)
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
	c.emitEventLocked(Event{Kind: EventClientClosed, Reason: ReasonClientClosed})
	for _, subscriptions := range c.eventSubscriptions {
		for subscription := range subscriptions {
			close(subscription.events)
		}
	}
	clear(c.eventSubscriptions)
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
	c.nextLoadID++
	pending := &flight{
		id:        c.nextLoadID,
		reason:    loadReason(c.entries[key]),
		done:      make(chan struct{}),
		cancel:    cancel,
		owner:     owner,
		startedAt: c.loadNow(),
		request:   request,
	}
	c.pending[key] = pending
	c.stats.Loads++
	c.emitEventLocked(pending.event(key, EventLoadStarted))
	c.notifyLocked(key)
	go c.execute(ctx, key, pending)
	return pending
}

// execute publishes a completed load only while it still owns the key's active request.
func (c *Client) execute(ctx context.Context, key any, pending *flight) {
	defer pending.cancel()
	value, expiresAt, err := c.run(ctx, key, pending)
	var source sharedResult
	if result, ok := value.(sharedResult); ok {
		source = result
		value = result.value
	}
	if ctx.Err() != nil {
		value, expiresAt, err = nil, time.Time{}, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	duration := c.loadNow().Sub(pending.startedAt)
	c.stats.TotalLoadDuration += duration
	switch {
	case err == nil:
		c.stats.LoadSuccesses++
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		c.stats.LoadCancellations++
	default:
		c.stats.LoadFailures++
	}
	var applied bool
	if c.pending[key] == pending {
		delete(c.pending, key)
		if !c.closed && ctx.Err() == nil {
			c.expireDataLocked(key)
			cached := c.entries[key]
			installsData := err == nil || c.options.Clock().Before(expiresAt)
			if installsData {
				cached.value, cached.hasData, cached.updatedAt = value, true, c.options.Clock()
				cached.fetchedAt, cached.sharedExpiresAt = source.fetchedAt, source.expiresAt
				cached.ordinary = pending.request.ordinary
				if cached.ordinary {
					expiresAt = c.ordinaryExpiry(cached, pending.request.options.StaleTime)
				}
				expiresAt = c.limitExpiry(cached.updatedAt, expiresAt)
			}
			if pending.invalidated {
				expiresAt = time.Time{}
			}
			cached.invalidated = pending.invalidated || err != nil
			cached.staleReason = ReasonNone
			if pending.invalidated {
				cached.staleReason = ReasonInvalidated
			} else if err != nil && !installsData {
				cached.staleReason = ReasonPreviousFailure
			}
			cached.err, cached.expiresAt = err, expiresAt
			c.advanceRevisionLocked(&cached)
			c.entries[key] = cached
			applied = true
			c.touchCapacityLocked(key)
		}
		c.touchGCLocked(key)
	}
	pending.value, pending.err = value, err
	event := pending.event(key, EventLoadFinished)
	event.Err, event.Duration, event.Applied = err, duration, applied
	c.emitEventLocked(event)
	if !applied {
		event.Kind, event.Reason = EventResultDiscarded, pending.discardReason
		if event.Reason == ReasonNone {
			event.Reason = ReasonCancel
		}
		c.emitEventLocked(event)
	}
	c.finishRefreshLocked(key, pending, applied)
	c.notifyLocked(key)
	close(pending.done)
}

// run applies bounded retries and stops for cancellation, deadline errors, closure, or overload.
func (c *Client) run(ctx context.Context, key any, pending *flight) (any, time.Time, error) {
	options := pending.request.options
	var previousErr error
	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return nil, time.Time{}, ctx.Err()
		}
		value, expiresAt, err := c.attempt(ctx, key, pending, previousErr)
		previousErr = err
		// Closed schedulers, overload, and recursive budgets cannot recover through retries.
		unavailable := errors.Is(err, ErrBatcherClosed) || errors.Is(err, ErrQueueFull)
		if unavailable || errors.Is(err, ErrExecutionReentry) {
			return value, expiresAt, err
		}
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

// attempt acquires execution capacity only around a loader invocation, never around retry backoff.
func (c *Client) attempt(
	ctx context.Context,
	key any,
	pending *flight,
	previousErr error,
) (any, time.Time, error) {
	// The initiating query owns budget selection; batch fetchers can skip this outer acquisition.
	ctx, release, err := pending.request.options.Executor.acquireContext(ctx)
	if err != nil {
		return nil, time.Time{}, err
	}
	defer release()
	c.mu.Lock()
	pending.attempts++
	if pending.attempts > 1 {
		c.stats.Retries++
		event := pending.event(key, EventRetryStarted)
		event.Err = previousErr
		c.emitEventLocked(event)
	}
	c.mu.Unlock()
	if pending.request.shared != nil {
		forced := pending.reason == ReasonInvalidated || pending.reason == ReasonRefetch
		// Failed refreshes remain invalidated; reusing their old shared record would undo that invalidation.
		bypass := forced || pending.reason == ReasonPreviousFailure
		returnValue, sharedErr := pending.request.shared.load(ctx, bypass)
		if sharedErr != nil {
			return nil, time.Time{}, sharedErr
		}
		return returnValue, time.Time{}, nil
	}
	return pending.request.load(ctx)
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
func (c *Client) cancelLocked(key any, reason EventReason) {
	if pending := c.pending[key]; pending != nil {
		pending.detached = true
		pending.discardReason = reason
		pending.cancel()
		c.cancelRefreshLocked(pending, reason)
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

// discardLocked clears retained state, preserves active type bindings, and records an explicit removal cause.
// ReasonNone suppresses events for internal cleanup of empty, unowned type metadata.
func (c *Client) discardLocked(key any, reason EventReason) {
	cached := c.entries[key]
	c.forgetCapacityLocked(key)
	if len(c.observers[key]) > 0 || c.pending[key] != nil {
		c.entries[key] = entry{typ: cached.typ, tags: cached.tags}
	} else {
		delete(c.entries, key)
	}
	// Live handles keep empty type bindings after removal. Repeated Remove or Clear
	// must not report another deletion when neither data nor an error was retained.
	if (cached.hasData || cached.err != nil) && reason != ReasonNone {
		c.emitEventLocked(Event{Key: key, Kind: EventCacheRemoved, Reason: reason})
	}
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

// entryFreshnessLocked derives a consumer's deadline and cache reuse decision from an already-read entry.
// Callers hold mu and remove over-age data first; explicit deadlines and invalidation remain authoritative.
func (c *Client) entryFreshnessLocked(cached entry, staleTime time.Duration) (time.Time, bool) {
	expiresAt := cached.expiresAt
	if !cached.hasData {
		return expiresAt, false
	}
	if cached.ordinary && !cached.invalidated {
		expiresAt = c.ordinaryExpiry(cached, staleTime)
	}
	return expiresAt, c.options.Clock().Before(expiresAt)
}

// ordinaryExpiry applies consumer freshness to source time and caps it by shared and local age limits.
func (c *Client) ordinaryExpiry(cached entry, staleTime time.Duration) time.Time {
	start := cached.updatedAt
	if !cached.sharedExpiresAt.IsZero() {
		start = cached.fetchedAt
	}
	expiresAt := start.Add(staleTime)
	if !cached.sharedExpiresAt.IsZero() && expiresAt.After(cached.sharedExpiresAt) {
		expiresAt = cached.sharedExpiresAt
	}
	return c.limitExpiry(cached.updatedAt, expiresAt)
}

// withFreshnessLocked applies one consumer's freshness to ordinary, non-invalidated shared data.
func (c *Client) withFreshnessLocked(key any, state Snapshot[any], options Options) Snapshot[any] {
	cached := c.entries[key]
	applyPolicy := cached.ordinary && state.HasData && !cached.invalidated
	if applyPolicy {
		expiresAt, fresh := c.entryFreshnessLocked(cached, options.StaleTime)
		state.ExpiresAt, state.Stale = expiresAt, !fresh
	}
	return state
}
