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
	// load retains this handle's erased loader without exposing untyped values to callers.
	load Loader[any]
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
// Automatic loading defaults to enabled; if options are supplied, the last option wins.
// Invalid keys, incompatible result types, and nil fetchers are reported through Snapshot and Updates.
// Disabled handles still receive shared updates and may explicitly Refetch. Close releases the subscription.
func Query[V any](client *Client, key any, fetch Fetcher[V], options ...QueryOptions) *QueryHandle[V] {
	enabled := true
	for _, option := range options {
		enabled = option.Enabled
	}
	handle := &QueryHandle[V]{client: client, key: key, updates: make(chan Snapshot[V], 1)}
	client.mu.Lock()
	defer client.mu.Unlock()
	handle.err = client.checkLocked(key, reflect.TypeFor[V]())
	if handle.err == nil && fetch == nil {
		handle.err = ErrNoFetcher
	}
	if handle.err != nil {
		handle.updates <- Snapshot[V]{Status: Error, Stale: true, Err: handle.err}
		close(handle.updates)
		return handle
	}
	// Bind before registering a typed publisher so no concurrent writer can install another type.
	_ = client.bindLocked(key, reflect.TypeFor[V]())
	observer := &subscription{
		enabled: enabled,
		load:    eraseLoader(client, fetch),
		publish: func(state Snapshot[any]) { publishLatest(handle.updates, typedSnapshot[V](state)) },
		close:   func() { close(handle.updates) },
	}
	handle.observer = observer
	if client.observers[key] == nil {
		client.observers[key] = make(map[*subscription]struct{})
	}
	client.observers[key][observer] = struct{}{}
	client.stopGCLocked(key)
	needsLoad := enabled && client.snapshotLocked(key).Stale && client.pending[key] == nil
	if needsLoad {
		client.startLocked(client.ctx, key, observer.load)
	}
	observer.publish(client.snapshotLocked(key))
	return handle
}

// Snapshot reads this handle's current data without starting a request.
// A released handle may still read its shared cache; a closed client returns ErrClosed.
func (h *QueryHandle[V]) Snapshot() Snapshot[V] {
	if h.err != nil {
		return Snapshot[V]{Status: Error, Stale: true, Err: h.err}
	}
	return Get[V](h.client, h.key)
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
	needsLoad := enabled && c.snapshotLocked(h.key).Stale && c.pending[h.key] == nil
	if needsLoad {
		c.startLocked(c.ctx, h.key, h.observer.load)
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
	value, err := h.client.fetch(
		ctx,
		h.key,
		reflect.TypeFor[V](),
		h.observer.load,
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
func Fetch[V any](ctx context.Context, client *Client, key any, fetch Fetcher[V]) (V, error) {
	value, err := client.fetch(
		ctx,
		key,
		reflect.TypeFor[V](),
		eraseLoader(client, fetch),
		false,
		nil,
	)
	return typedValue[V](value), err
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
		erased,
		false,
		nil,
	)
	return typedValue[V](value), err
}

// Prefetch warms a typed key without returning its data; unlike browser prefetch APIs it returns errors.
func Prefetch[V any](ctx context.Context, client *Client, key any, fetch Fetcher[V]) error {
	_, err := Fetch(
		ctx,
		client,
		key,
		fetch,
	)
	return err
}

// Get reads typed cached state without starting a request or binding a previously unused key.
// Incompatible result types return ErrTypeMismatch instead of panicking or exposing another query's data.
func Get[V any](client *Client, key any) Snapshot[V] {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.checkLocked(key, reflect.TypeFor[V]()); err != nil {
		return Snapshot[V]{Status: Error, Stale: true, Err: err}
	}
	client.touchGCLocked(key)
	return typedSnapshot[V](client.snapshotLocked(key))
}

// Set installs a typed local result, cancels older loads, and notifies all compatible handles.
// Type conflicts are checked before cancellation so a rejected update cannot damage an existing query.
func Set[V any](client *Client, key any, value V) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if err := client.bindLocked(key, reflect.TypeFor[V]()); err != nil {
		return err
	}
	client.cancelLocked(key)
	now := client.options.Clock()
	client.entries[key] = entry{
		typ: reflect.TypeFor[V](), value: value, hasData: true, updatedAt: now,
		expiresAt: now.Add(client.options.StaleTime),
	}
	client.touchGCLocked(key)
	client.notifyLocked(key)
	return nil
}

// fetch shares one erased loader while keeping cancellation and type binding atomic with cache access.
func (c *Client) fetch(
	ctx context.Context,
	key any,
	typ reflect.Type,
	load Loader[any],
	force bool,
	observer *subscription,
) (any, error) {
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
		cached := c.entries[key]
		fresh := cached.hasData && c.options.Clock().Before(cached.expiresAt)
		if !force && fresh {
			c.touchGCLocked(key)
			c.mu.Unlock()
			return cached.value, cached.err
		}
		if load == nil {
			c.mu.Unlock()
			return nil, ErrNoFetcher
		}
		if err := c.bindLocked(key, typ); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if force {
			cached = c.entries[key]
			cached.expiresAt = time.Time{}
			c.entries[key] = cached
			c.notifyLocked(key)
			force = false
		}
		pending := c.pending[key]
		if pending == nil {
			pending = c.startLocked(ctx, key, load)
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

// eraseLoader retains static typing at the API boundary while sharing a single heterogeneous cache.
func eraseLoader[V any](client *Client, fetch Fetcher[V]) Loader[any] {
	if fetch == nil {
		return nil
	}
	return func(ctx context.Context) (any, time.Time, error) {
		value, err := fetch(ctx)
		if err != nil {
			return value, time.Time{}, err
		}
		return value, client.options.Clock().Add(client.options.StaleTime), nil
	}
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
