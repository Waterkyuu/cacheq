package query

// Observer owns one query subscription and its permission to start automatic loads.
// Observers of the same key share data and in-flight work but keep independent enabled states.
type Observer[K comparable, V any] struct {
	// client owns the shared query state and protects the observer's mutable fields.
	client *Client[K, V]
	// key identifies the shared result watched by this observer.
	key K
	// updates delivers initial and latest shared states and closes when the observer is released.
	updates chan Snapshot[V]
	// load is this observer's background loader; legacy Subscribe observers use the cached loader.
	load Loader[V]
	// enabled permits this observer to initiate automatic loads; client.mu protects changes.
	enabled bool
}

// Observe subscribes to shared state and loads stale or missing data only when options.Enabled is true.
// Zero-value options are disabled. Changing application conditions requires calling SetEnabled.
// Close the returned observer when it is no longer needed; disabled observers still receive shared updates.
func (c *Client[K, V]) Observe(key K, fetch Fetcher[V], options ObserveOptions) *Observer[K, V] {
	c.mu.Lock()
	defer c.mu.Unlock()
	observer := c.newObserverLocked(key, c.loader(fetch), options.Enabled)
	if !c.closed && observer.enabled {
		observer.loadIfStaleLocked()
	}
	return observer
}

// Updates returns this observer's state channel; slow readers may skip intermediate transitions.
func (o *Observer[K, V]) Updates() <-chan Snapshot[V] {
	return o.updates
}

// Snapshot reads current shared state without starting a request, even while this observer is disabled.
func (o *Observer[K, V]) Snapshot() Snapshot[V] {
	return o.client.Snapshot(o.key)
}

// SetEnabled changes automatic loading permission and loads stale or missing data when enabling.
// Disabling does not cancel existing loads or prevent other observers from requesting shared data.
// Repeating the current value, or updating a closed observer or client, has no effect.
func (o *Observer[K, V]) SetEnabled(enabled bool) {
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.observers[o.key][o.updates] != o {
		return
	}
	if o.enabled == enabled {
		return
	}
	o.enabled = enabled
	if enabled {
		o.loadIfStaleLocked()
	}
}

// Close releases this observer and closes its updates channel without canceling shared work.
// Repeated calls and calls after Client.Close have no effect.
func (o *Observer[K, V]) Close() {
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.observers[o.key][o.updates] != o {
		return
	}
	delete(c.observers[o.key], o.updates)
	if len(c.observers[o.key]) == 0 {
		delete(c.observers, o.key)
	}
	close(o.updates)
	c.touchGCLocked(o.key)
}

// loadIfStaleLocked starts this observer's load only when shared data needs refreshing and no load exists.
func (o *Observer[K, V]) loadIfStaleLocked() {
	c := o.client
	if c.snapshotLocked(o.key).Stale && c.pending[o.key] == nil {
		c.startLocked(c.ctx, o.key, o.load)
	}
}

// newObserverLocked registers a subscriber before any load can publish a state change.
func (c *Client[K, V]) newObserverLocked(key K, load Loader[V], enabled bool) *Observer[K, V] {
	observer := &Observer[K, V]{
		client: c, key: key, load: load, enabled: enabled,
		updates: make(chan Snapshot[V], 1),
	}
	observer.updates <- c.snapshotLocked(key)
	if c.closed {
		close(observer.updates)
		return observer
	}
	if c.observers[key] == nil {
		c.observers[key] = make(map[chan Snapshot[V]]*Observer[K, V])
	}
	c.observers[key][observer.updates] = observer
	c.stopGCLocked(key)
	return observer
}

// observedLoaderLocked selects an enabled observer's loader or the cached loader for legacy subscribers.
func (c *Client[K, V]) observedLoaderLocked(key K) Loader[V] {
	legacyEnabled := false
	for _, observer := range c.observers[key] {
		if !observer.enabled {
			continue
		}
		if observer.load != nil {
			return observer.load
		}
		legacyEnabled = true
	}
	if legacyEnabled {
		return c.entries[key].load
	}
	return nil
}
