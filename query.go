package cacheq

import (
	"context"
	"reflect"
	"time"
)

// subscription connects a typed handle to shared state; all fields are protected by Client.mu.
type subscription struct {
	// enabled permits this handle to initiate automatic requests.
	enabled bool
	// request retains this consumer's loader and copied policy without exposing untyped results to callers.
	request queryRequest
	// order identifies this consumer's place in automatic refresh selection.
	order uint64
	// publish installs a typed snapshot in the handle's bounded updates channel.
	publish func(Snapshot[any])
	// close ends the handle's channel after its subscription has been detached.
	close func()
}

// QueryHandle owns a typed subscription, its loader, and its automatic loading permission.
// Handles for one key share data and in-flight work; call Close when a consumer leaves.
type QueryHandle[V any] struct {
	// client owns heterogeneous cached data and serializes all handle operations.
	client *Client
	// key identifies the shared result independently of the handle's result type.
	key any
	// observer is registered only when construction succeeds.
	observer *subscription
	// updates carries the initial and latest typed states without blocking a loader.
	updates chan Snapshot[V]
	// err records a construction failure without admitting an incompatible subscription.
	err error
}

// Query creates a typed handle and loads missing or stale data in the background.
// Automatic loading defaults to enabled; omitted options use Client defaults.
// Nil policy fields inherit Client defaults; each handle evaluates its own ordinary freshness.
// Concurrent loads share the initiator's policy; automatic invalidation uses the oldest enabled handle.
// Invalid keys, incompatible result types, and nil fetchers are reported through Snapshot and Updates.
// Extra configurations produce ErrTooManyOptions through the same failed-handle contract.
// Disabled handles still receive shared updates and may explicitly Refetch. Close releases the subscription.
func Query[V any](client *Client, key any, fetch Fetcher[V], options ...QueryOptions) *QueryHandle[V] {
	definition, err := QueryDefinition(key, fetch, options...)
	return query(client, definition, err)
}

// query registers a typed subscription and its definition's labels before starting automatic work.
// A constructor error produces a failed handle without registering the zero definition.
func query[V any](client *Client, definition Definition[V], constructorErr error) *QueryHandle[V] {
	key := definition.Key
	enabled := !definition.Options.Disable
	var request queryRequest
	if constructorErr == nil {
		request = definition.request(client.options, TriggerQuery)
	}
	policy := request.options
	handle := &QueryHandle[V]{client: client, key: key, updates: make(chan Snapshot[V], 1)}
	client.mu.Lock()
	defer client.mu.Unlock()
	handle.err = constructorErr
	if handle.err == nil {
		handle.err = client.checkLocked(key, reflect.TypeFor[V]())
	}
	if handle.err == nil && definition.Fetcher == nil {
		handle.err = ErrNoFetcher
	}
	if handle.err != nil {
		handle.updates <- Snapshot[V]{Status: Error, Stale: true, Err: handle.err}
		close(handle.updates)
		return handle
	}
	// Bind before registering a typed publisher so no concurrent writer can install another type.
	_ = client.bindLocked(key, reflect.TypeFor[V]())
	client.registerTagsLocked(key, request.tags)
	client.registerSharedLocked(key, request)
	observer := &subscription{
		enabled: enabled,
		request: request,
		order:   client.nextObserver,
		publish: func(state Snapshot[any]) { publishLatest(handle.updates, typedSnapshot[V](state)) },
		close:   func() { close(handle.updates) },
	}
	client.nextObserver++
	handle.observer = observer
	if client.observers[key] == nil {
		client.observers[key] = make(map[*subscription]struct{})
	}
	client.observers[key][observer] = struct{}{}
	client.stopGCLocked(key)
	if client.expireDataLocked(key) {
		client.notifyLocked(key)
	}
	client.touchCapacityLocked(key)
	if enabled {
		client.loadObservedLocked(key, observer.request)
	}
	observer.publish(client.withFreshnessLocked(key, client.snapshotLocked(key), policy))
	return handle
}

// Snapshot reads this handle's current data without starting a request.
// A released handle may still read its shared cache; a closed client returns ErrClosed.
func (h *QueryHandle[V]) Snapshot() Snapshot[V] {
	if h.err != nil {
		return Snapshot[V]{Status: Error, Stale: true, Err: h.err}
	}
	return typedSnapshot[V](h.client.getSnapshot(h.key, reflect.TypeFor[V](), h.observer.request.options))
}

// Updates returns the initial and latest states; slow readers may skip intermediate transitions.
func (h *QueryHandle[V]) Updates() <-chan Snapshot[V] { return h.updates }

// SetEnabled changes this consumer's permission to load automatically.
// Enabling loads missing or stale data; disabling neither cancels work nor disables other consumers.
func (h *QueryHandle[V]) SetEnabled(enabled bool) error {
	c := h.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := h.activeLocked(); err != nil {
		return err
	}
	if h.observer.enabled == enabled {
		return nil
	}
	h.observer.enabled = enabled
	if enabled {
		request := h.observer.request
		request.trigger = TriggerEnable
		c.loadObservedLocked(h.key, request)
	}
	return nil
}

// Refetch waits for a new result even when data is fresh or this handle is disabled.
// An existing same-key load is shared; released handles return ErrQueryClosed without starting work.
func (h *QueryHandle[V]) Refetch(ctx context.Context) (V, error) {
	if h.err != nil {
		var zero V
		return zero, h.err
	}
	request := h.observer.request
	request.trigger = TriggerRefetch
	value, err := h.client.fetch(
		ctx,
		h.key,
		reflect.TypeFor[V](),
		request,
		true,
		h.observer,
	)
	return typedValue[V](value), err
}

// Close releases this consumer once and closes Updates without canceling shared requests.
func (h *QueryHandle[V]) Close() {
	if h.err != nil {
		return
	}
	c := h.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.observers[h.key][h.observer]; !exists {
		return
	}
	delete(c.observers[h.key], h.observer)
	if len(c.observers[h.key]) == 0 {
		delete(c.observers, h.key)
	}
	h.observer.close()
	c.touchGCLocked(h.key)
}

// activeLocked verifies handle ownership while the client's lock is held.
func (h *QueryHandle[V]) activeLocked() error {
	if h.err != nil {
		return h.err
	}
	if h.client.closed {
		return ErrClosed
	}
	if _, exists := h.client.observers[h.key][h.observer]; !exists {
		return ErrQueryClosed
	}
	return nil
}

// Fetch waits for fresh data without owning a long-lived subscription.
// The result type is inferred from fetch, allowing one client to serve unrelated data types.
// An omitted configuration uses Client defaults; extra values return ErrTooManyOptions without loading.
// Disable controls automatic subscriptions only and does not suppress this explicit read.
// Ordinary freshness is evaluated independently; explicit loader deadlines stay authoritative.
// Joining in-flight work preserves its initiator's loader, retries, and timeout.
// Canceling ctx stops this caller's wait; a configured timeout applies only to loads it starts.
func Fetch[V any](
	ctx context.Context,
	client *Client,
	key any,
	fetch Fetcher[V],
	options ...QueryOptions,
) (V, error) {
	definition, err := QueryDefinition(key, fetch, options...)
	if err != nil {
		var zero V
		return zero, err
	}
	return definition.Fetch(ctx, client)
}

// FetchWithExpiry loads data with an absolute freshness deadline, preserving the age of restored data.
// A future expiration on an error explicitly caches the fallback data and error until that deadline.
func FetchWithExpiry[V any](ctx context.Context, client *Client, key any, load Loader[V]) (V, error) {
	var erased Loader[any]
	if load != nil {
		erased = func(ctx context.Context) (any, time.Time, error) {
			value, expiry, err := load(ctx)
			return value, expiry, err
		}
	}
	value, err := client.fetch(
		ctx,
		key,
		reflect.TypeFor[V](),
		queryRequest{load: erased, options: client.options, trigger: TriggerFetch},
		false,
		nil,
	)
	return typedValue[V](value), err
}

// Prefetch warms a typed key without returning its data; unlike browser prefetch APIs it returns errors.
// It uses the supplied loading policy like Fetch and ignores Disable.
// Omitted options use Client defaults; extra values return ErrTooManyOptions without loading.
func Prefetch[V any](
	ctx context.Context,
	client *Client,
	key any,
	fetch Fetcher[V],
	options ...QueryOptions,
) error {
	definition, err := QueryDefinition(key, fetch, options...)
	if err != nil {
		return err
	}
	return definition.Prefetch(ctx, client)
}

// Get reads cached state without requiring a result type, starting a request, or creating a subscription.
// Data retains its dynamic type; use HasData to distinguish missing data from a cached nil or zero value.
// Reads use Client freshness defaults and restart inactive retention without binding an unused key.
// Invalid keys and closed clients return errors; existing result type bindings remain unchanged.
func (c *Client) Get(key any) Snapshot[any] {
	return c.getSnapshot(key, nil, c.options)
}

// Get reads typed cached state without starting a request or binding a previously unused key.
// Incompatible result types return ErrTypeMismatch instead of panicking or exposing another query's data.
// Use Client.Get to inspect cached state without specifying or checking a result type.
func Get[V any](client *Client, key any) Snapshot[V] {
	return typedSnapshot[V](client.getSnapshot(key, reflect.TypeFor[V](), client.options))
}

// getSnapshot reads shared state using one consumer's freshness without changing another consumer's policy.
// A nil result type skips type matching while preserving key validation and cache lifecycle behavior.
func (c *Client) getSnapshot(key any, typ reflect.Type, options Options) Snapshot[any] {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.checkLocked(key, typ); err != nil {
		return Snapshot[any]{Status: Error, Stale: true, Err: err}
	}
	if c.expireDataLocked(key) {
		c.notifyLocked(key)
	}
	c.touchCapacityLocked(key)
	c.touchGCLocked(key)
	return c.withFreshnessLocked(key, c.snapshotLocked(key), options)
}

// Set installs a typed local result, cancels older loads, and notifies all compatible handles.
// Type conflicts are checked before cancellation so a rejected update cannot damage an existing query.
func Set[V any](client *Client, key any, value V) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.bindLocked(key, reflect.TypeFor[V]()); err != nil {
		return err
	}
	client.setLocked(key, value, reflect.TypeFor[V]())
	return nil
}

// setLocked installs already-validated local data while serializing cancellation and notifications.
func (c *Client) setLocked(key, value any, typ reflect.Type) {
	c.cancelLocked(key, ReasonSet)
	now := c.options.Clock()
	cached := entry{
		typ: typ, value: value, hasData: true, updatedAt: now, tags: c.entries[key].tags,
		shared:    c.entries[key].shared,
		expiresAt: c.limitExpiry(now, now.Add(c.options.StaleTime)), ordinary: true,
	}
	c.advanceRevisionLocked(&cached)
	c.entries[key] = cached
	c.emitEventLocked(Event{Key: key, Kind: EventLocalWrite, Reason: ReasonSet})
	c.touchCapacityLocked(key)
	c.touchGCLocked(key)
	c.notifyLocked(key)
}

// fetch shares one erased loader while keeping cancellation and type binding atomic with cache access.
func (c *Client) fetch(
	ctx context.Context,
	key any,
	typ reflect.Type,
	request queryRequest,
	force bool,
	observer *subscription,
) (any, error) {
	useCache := !force
	var countedLookup, countedMerge bool
	var joinedFlight *flight
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrClosed
		}
		if observer != nil {
			if _, exists := c.observers[key][observer]; !exists {
				c.mu.Unlock()
				return nil, ErrQueryClosed
			}
		}
		if err := c.checkLocked(key, typ); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if c.expireDataLocked(key) {
			c.notifyLocked(key)
		}
		cached := c.entries[key]
		// A cache decision only needs freshness; building a snapshot repeats age checks and clock reads under mu.
		_, fresh := c.entryFreshnessLocked(cached, request.options.StaleTime)
		if useCache && !countedLookup {
			c.recordLookupLocked(fresh)
			countedLookup = true
		}
		if !force && fresh {
			c.registerTagsLocked(key, request.tags)
			c.registerSharedLocked(key, request)
			c.emitEventLocked(
				Event{Key: key, Kind: EventCacheHit, Trigger: request.trigger, Reason: ReasonFresh, Err: cached.err},
			)
			c.touchCapacityLocked(key)
			c.touchGCLocked(key)
			c.mu.Unlock()
			return cached.value, cached.err
		}
		if request.load == nil {
			c.mu.Unlock()
			return nil, ErrNoFetcher
		}
		if err := c.bindLocked(key, typ); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		c.registerTagsLocked(key, request.tags)
		c.registerSharedLocked(key, request)
		if force {
			cached = c.entries[key]
			cached.expiresAt = time.Time{}
			cached.invalidated = true
			cached.staleReason = ReasonRefetch
			c.advanceRevisionLocked(&cached)
			c.entries[key] = cached
			c.notifyLocked(key)
			force = false
		}
		pending := c.pending[key]
		if pending == nil {
			pending = c.startLocked(ctx, key, request)
		} else {
			if !countedMerge {
				// Owner cancellation can repeat this loop, but one caller still represents one shared request.
				c.stats.MergedRequests++
				countedMerge = true
			}
			if joinedFlight != pending {
				c.joinLoadLocked(key, pending, request)
				joinedFlight = pending
			}
		}
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pending.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			// Only caller-owned cancellation permits a remaining waiter to start a replacement load.
			if pending.owner.Err() != nil && !pending.detached {
				continue
			}
			return pending.value, pending.err
		}
	}
}

// loadObservedLocked records an enabled consumer's cache decision and starts or joins a stale load.
func (c *Client) loadObservedLocked(key any, request queryRequest) {
	// Enabling a consumer must still discard over-age data before checking its freshness policy.
	c.expireDataLocked(key)
	_, fresh := c.entryFreshnessLocked(c.entries[key], request.options.StaleTime)
	c.recordLookupLocked(fresh)
	if fresh {
		c.emitEventLocked(Event{Key: key, Kind: EventCacheHit, Trigger: request.trigger, Reason: ReasonFresh,
			Err: c.entries[key].err})
		return
	}
	if pending := c.pending[key]; pending != nil {
		c.stats.MergedRequests++
		c.joinLoadLocked(key, pending, request)
		return
	}
	c.startLocked(c.ctx, key, request)
}

// eraseFetcher copies an ordinary fetcher's policy and retains static typing at the API boundary.
func eraseFetcher[V any](fetch Fetcher[V], options Options) queryRequest {
	request := queryRequest{options: options, ordinary: true, trigger: TriggerFetch}
	if fetch == nil {
		return request
	}
	request.load = func(ctx context.Context) (any, time.Time, error) {
		value, err := fetch(ctx)
		return value, time.Time{}, err
	}
	return request
}

// typedValue restores a value only after its static type has been checked under the client lock.
func typedValue[V any](value any) V {
	if value == nil {
		var zero V
		return zero
	}
	return value.(V)
}

// typedSnapshot restores typed data while preserving all shared request metadata.
func typedSnapshot[V any](state Snapshot[any]) Snapshot[V] {
	return Snapshot[V]{
		Data: typedValue[V](state.Data), HasData: state.HasData, Status: state.Status, Err: state.Err,
		Stale: state.Stale, Fetching: state.Fetching, UpdatedAt: state.UpdatedAt, ExpiresAt: state.ExpiresAt,
	}
}

// publishLatest replaces an unread snapshot so slow consumers cannot block shared query progress.
func publishLatest[V any](updates chan Snapshot[V], state Snapshot[V]) {
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
