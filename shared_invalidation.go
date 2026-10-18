package cacheq

import (
	"context"
	"errors"
	"slices"
	"sync"
)

// ErrSharedCapability indicates that a store lacks the requested invalidation or tag capability.
var ErrSharedCapability = errors.New("shared store capability is unavailable")

// SharedInvalidation identifies one committed remote change or a notification gap requiring resynchronization.
type SharedInvalidation struct {
	// ID uniquely identifies a normal operation for local duplicate suppression; reset events may omit it.
	ID string
	// Key selects one logical shared key; it is empty for tag or reset events.
	Key string
	// Tags selects related records with OR matching; it is empty for key or reset events.
	Tags []string
	// RegisteredTags adds remote membership to Key without invalidating its data; it is empty for invalidations.
	RegisteredTags []string
	// Reset marks every query bound to the source stale after startup or a connection gap.
	Reset bool
	// Err describes a transport or decoding failure that caused a reset; it is never sent over the wire.
	Err error `json:"-"`
}

// SharedInvalidationSubscription owns a notification stream, independently of the borrowed store connection.
type SharedInvalidationSubscription interface {
	// Events delivers invalidations and reset markers; implementations must never silently discard events.
	Events() <-chan SharedInvalidation
	// Close releases subscription resources and must be safe and idempotent during cancellation.
	Close() error
}

// SharedInvalidationStore optionally extends SharedStore with operation receipts and notifications.
// When subscriptions are enabled, invalidation and event publication must be atomic; receipt IDs must never be reused.
type SharedInvalidationStore interface {
	// SharedStore supplies the existing read and conditional-write contract.
	SharedStore
	// InvalidationScope returns a nonempty, immutable process-local store identity without I/O.
	// Reuse the same store instance for query definitions and their watcher to avoid mixing unrelated servers.
	InvalidationScope() string
	// SubscribeInvalidations returns after subscription admission and reports connection gaps with Reset.
	SubscribeInvalidations(context.Context) (SharedInvalidationSubscription, error)
	// InvalidateKey commits a key invalidation, broadcasts it, and returns the same operation receipt.
	InvalidateKey(context.Context, string) (SharedInvalidation, error)
}

// SharedTagStore optionally registers tags and fences writes against both key and tag generations.
type SharedTagStore interface {
	// SharedInvalidationStore supplies key invalidation receipts and notifications.
	SharedInvalidationStore
	// TagsEnabled reports whether the store was explicitly configured for atomic tag generations.
	TagsEnabled() bool
	// ReadTagged atomically registers requested tags and returns their opaque generation snapshot.
	// Version includes all registered tags; Write must reject a changed key or tag generation.
	// Tag membership is additive, including on hits; Read also checks tags registered by other readers.
	// Version remains stable while generations and membership are unchanged.
	// With notifications enabled, new membership is broadcast as RegisteredTags.
	ReadTagged(context.Context, string, []string) (SharedRecord, error)
	// InvalidateTags atomically changes all requested tag generations and publishes one OR-matching receipt.
	// Empty input succeeds without changing remote state or publishing an event.
	InvalidateTags(context.Context, []string) (SharedInvalidation, error)
}

// sharedBinding connects retained local query state to its notification source and remote identity.
type sharedBinding struct {
	// scope identifies the process-local store instance owning this query's remote records.
	scope string
	// key is the stable logical shared record identity.
	key string
	// tags contains copied remote membership plus labels admitted by tagged query definitions.
	tags []string
	// lastID suppresses duplicate delivery of the latest remote operation receipt.
	lastID string
}

// SharedWatchStats describes processed notifications and recovery failures for one watcher.
type SharedWatchStats struct {
	// Invalidations counts matching local queries changed by normal remote operations.
	Invalidations uint64
	// Resyncs counts startup and transport-gap resets, including resets without retained queries.
	Resyncs uint64
	// Errors counts reported transport, decoding, or unexpected stream-closure failures.
	Errors uint64
	// LastError retains the most recently reported failure; later recovery does not erase diagnostic evidence.
	LastError error
}

// SharedWatcher owns one client's notification processing lifetime; Close waits for subscription release.
type SharedWatcher struct {
	// cancel stops event processing without closing the application-owned store.
	cancel context.CancelFunc
	// done closes after all watcher-owned subscription resources are released.
	done chan struct{}
	// mu protects cumulative statistics while the event loop updates them.
	mu sync.Mutex
	// stats records notification delivery and recovery independently of query load counters.
	stats SharedWatchStats
}

// WatchShared subscribes one Client to the supplied store and repairs any pre-subscription local gap.
// The zero-value policy refreshes enabled observers; RefetchAfterLoad opts into one bounded follow-up.
// Exactly one policy is required. Disabled observers receive stale snapshots without loading.
// Caller cancellation, Client.Close, or watcher.Close releases the subscription; the borrowed store stays open.
func (c *Client) WatchShared(
	ctx context.Context,
	store SharedInvalidationStore,
	options InvalidateOptions,
) (*SharedWatcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil || store.InvalidationScope() == "" {
		return nil, ErrSharedCapability
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}
	lifetime, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	stream, err := store.SubscribeInvalidations(lifetime)
	if err != nil {
		stop()
		cancel()
		return nil, err
	}
	if stream == nil {
		stop()
		cancel()
		return nil, ErrSharedCapability
	}
	if stream.Events() == nil {
		stop()
		cancel()
		_ = stream.Close()
		return nil, ErrSharedCapability
	}
	watch := &SharedWatcher{cancel: cancel, done: make(chan struct{})}
	_, err = c.applySharedInvalidation(store.InvalidationScope(), SharedInvalidation{Reset: true}, options)
	if err != nil {
		stop()
		cancel()
		_ = stream.Close()
		return nil, err
	}
	watch.stats.Resyncs = 1
	go watch.run(lifetime, stop, c, store.InvalidationScope(), stream, options)
	return watch, nil
}

// Close cancels the watcher and waits for its own resources, without closing Client or Store.
func (w *SharedWatcher) Close() { w.cancel(); <-w.done }

// Done signals that the stream is no longer being processed and its resources have been released.
func (w *SharedWatcher) Done() <-chan struct{} { return w.done }

// Stats returns a concurrency-safe snapshot of notification delivery and recovery outcomes.
func (w *SharedWatcher) Stats() SharedWatchStats { w.mu.Lock(); defer w.mu.Unlock(); return w.stats }

// run applies notifications through normal Client invalidation while owning only the subscription lifetime.
func (w *SharedWatcher) run(
	ctx context.Context,
	stop func() bool,
	client *Client,
	scope string,
	stream SharedInvalidationSubscription,
	policy InvalidateOptions,
) {
	defer close(w.done)
	defer stop()
	defer w.cancel()
	defer func() { _ = stream.Close() }()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-stream.Events():
			if ctx.Err() != nil {
				return
			}
			if !ok {
				event = SharedInvalidation{Reset: true, Err: errors.New("shared invalidation stream closed")}
			}
			if !event.Reset && event.ID == "" {
				event = SharedInvalidation{Reset: true, Err: errors.New("shared invalidation omitted its receipt ID")}
			}
			count, err := client.applySharedInvalidation(scope, event, policy)
			if errors.Is(err, ErrClosed) {
				return
			}
			w.mu.Lock()
			if event.Reset {
				w.stats.Resyncs++
			} else {
				w.stats.Invalidations += count
			}
			if event.Err != nil {
				w.stats.Errors++
				w.stats.LastError = event.Err
			}
			w.mu.Unlock()
			if !ok {
				return
			}
		}
	}
}

// registerSharedLocked preserves remote identity and tagged relationships while admitting a query request.
func (c *Client) registerSharedLocked(key any, request queryRequest) {
	if request.shared == nil {
		return
	}
	cached := c.entries[key]
	cached.shared.scope, cached.shared.key = request.shared.scope, request.shared.key
	for _, tag := range request.shared.tags {
		if !slices.Contains(cached.shared.tags, tag) {
			cached.shared.tags = append(cached.shared.tags, tag)
		}
	}
	c.entries[key] = cached
}

// applySharedInvalidation selects only retained queries bound to this store, without application predicates.
func (c *Client) applySharedInvalidation(
	scope string,
	event SharedInvalidation,
	policy InvalidateOptions,
) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, ErrClosed
	}
	var count uint64
	for key, cached := range c.entries {
		if cached.shared.scope != scope {
			continue
		}
		if len(event.RegisteredTags) > 0 {
			if cached.shared.key == event.Key {
				for _, tag := range event.RegisteredTags {
					if !slices.Contains(cached.shared.tags, tag) {
						cached.shared.tags = append(cached.shared.tags, tag)
					}
				}
				c.entries[key] = cached
			}
			continue
		}
		matches := event.Reset || cached.shared.key == event.Key || matchesQueryTags(cached.shared.tags, event.Tags)
		duplicate := event.ID != "" && cached.shared.lastID == event.ID
		if !matches || duplicate {
			continue
		}
		cached.shared.lastID = event.ID
		c.entries[key] = cached
		c.invalidateLocked(key, policy, nil)
		count++
	}
	return count, nil
}

// applySharedKey preserves the existing exact-local-key helper while suppressing its broadcast receipt.
func (c *Client) applySharedKey(key any, event SharedInvalidation, policy InvalidateOptions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrClosed
	}
	cached, exists := c.entries[key]
	if !exists || (event.ID != "" && cached.shared.lastID == event.ID) {
		return nil
	}
	cached.shared.lastID = event.ID
	c.entries[key] = cached
	c.invalidateLocked(key, policy, nil)
	return nil
}

// InvalidateSharedTags invalidates remote tags and then the matching queries bound to this store in one Client.
// Tag matching uses OR and exact strings. Remote failure leaves local state unchanged.
// Other Clients receive notifications only when watching the same remote namespace.
func (c *Client) InvalidateSharedTags(
	ctx context.Context,
	store SharedTagStore,
	tags []string,
	options InvalidateOptions,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store == nil {
		return ErrSharedCapability
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if len(tags) == 0 {
		return nil
	}
	event, err := store.InvalidateTags(ctx, slices.Clone(tags))
	if err != nil {
		return err
	}
	_, err = c.applySharedInvalidation(store.InvalidationScope(), event, options)
	return err
}
