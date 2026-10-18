package cacheq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// sharedNotificationStore injects reliable notifications without network or scheduling assumptions.
type sharedNotificationStore struct {
	// SharedStore owns the P0 generation and value contract.
	SharedStore
	// mu protects subscription registration and event numbering.
	mu sync.Mutex
	// streams contains independently owned subscribers.
	streams []*sharedNotificationStream
	// sequence produces deterministic operation receipts.
	sequence int
	// enableTags controls the explicit optional tag capability in these tests.
	enableTags bool
	// tagErr injects remote failure before local invalidation.
	tagErr error
	// taggedRead controls snapshot/notification ordering for the dependency-registration race test.
	taggedRead func(context.Context, string, []string) (SharedRecord, error)
}

// sharedNotificationStream owns one buffered test notification channel.
type sharedNotificationStream struct {
	// events delivers controlled events to the Client watcher.
	events chan SharedInvalidation
	// closed records resource release without closing a channel concurrently with a test publisher.
	closed chan struct{}
	// once makes release idempotent.
	once sync.Once
}

// Events exposes the stream consumed by a watcher.
func (s *sharedNotificationStream) Events() <-chan SharedInvalidation { return s.events }

// Close releases the stream once without affecting the borrowed store.
func (s *sharedNotificationStream) Close() error { s.once.Do(func() { close(s.closed) }); return nil }

// InvalidationScope identifies this particular test store independently of remote key strings.
func (s *sharedNotificationStore) InvalidationScope() string { return "notification-test" }

// SubscribeInvalidations registers a stream before the caller starts issuing remote operations.
func (s *sharedNotificationStore) SubscribeInvalidations(ctx context.Context) (SharedInvalidationSubscription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stream := &sharedNotificationStream{events: make(chan SharedInvalidation, 8), closed: make(chan struct{})}
	s.mu.Lock()
	s.streams = append(s.streams, stream)
	s.mu.Unlock()
	return stream, nil
}

// InvalidateKey returns and broadcasts the same receipt to make duplicate delivery observable.
func (s *sharedNotificationStore) InvalidateKey(ctx context.Context, key string) (SharedInvalidation, error) {
	if err := s.Invalidate(ctx, key); err != nil {
		return SharedInvalidation{}, err
	}
	s.mu.Lock()
	s.sequence++
	event := SharedInvalidation{ID: fmt.Sprintf("event:%d", s.sequence), Key: key}
	s.mu.Unlock()
	s.emit(event)
	return event, nil
}

// emit publishes controlled input after stream registration.
func (s *sharedNotificationStore) emit(event SharedInvalidation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stream := range s.streams {
		stream.events <- event
	}
}

// waitSharedEvent uses diagnostic invalidation publication as a deterministic state-transition barrier.
func waitSharedEvent(t *testing.T, events <-chan Event, key any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		select {
		case event := <-events:
			if event.Kind == EventInvalidated && event.Key == key {
				return
			}
		case <-ctx.Done():
			t.Fatal("shared event was not applied")
		}
	}
}

// TestSharedWatcherKeyPropagation applies external changes to disabled subscriptions without forcing a load.
func TestSharedWatcherKeyPropagation(t *testing.T) {
	store := &sharedNotificationStore{SharedStore: newSharedTestStore()}
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 7, nil })
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	watch, err := c.WatchShared(context.Background(), store, InvalidateOptions{Refetch: RefetchNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	if _, err := d.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	d.Options.Disable = true
	h := d.Query(c)
	t.Cleanup(h.Close)
	diagnostic, err := c.SubscribeEvents(EventOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(diagnostic.Close)
	if _, err := store.InvalidateKey(context.Background(), d.Shared.options.Key); err != nil {
		t.Fatal(err)
	}
	waitSharedEvent(t, diagnostic.Events(), d.Key)
	if state := h.Snapshot(); !state.Stale || state.Data != 7 || state.Fetching {
		t.Fatalf("notification = %+v", state)
	}
	// Local writes retain the binding so a later remote event still reaches this query.
	if err := Set(c, d.Key, 9); err != nil {
		t.Fatal(err)
	}
	store.emit(SharedInvalidation{ID: "next-event", Key: d.Shared.options.Key})
	waitSharedEvent(t, diagnostic.Events(), d.Key)
	if !h.Snapshot().Stale {
		t.Fatal("Set lost its remote notification binding")
	}
}

// TestSharedWatcherResync marks all bound values stale after a connection gap, leaving unrelated data fresh.
func TestSharedWatcherResync(t *testing.T) {
	store := &sharedNotificationStore{SharedStore: newSharedTestStore()}
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 7, nil })
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	if _, err := d.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if err := Set(c, "unrelated", 9); err != nil {
		t.Fatal(err)
	}
	watch, err := c.WatchShared(context.Background(), store, InvalidateOptions{Refetch: RefetchNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	if !Get[int](c, d.Key).Stale {
		t.Fatal("startup did not repair the pre-subscription gap")
	}
	if _, err := d.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	diagnostic, err := c.SubscribeEvents(EventOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(diagnostic.Close)
	store.emit(SharedInvalidation{Reset: true, Err: errors.New("connection lost")})
	waitSharedEvent(t, diagnostic.Events(), d.Key)
	if !Get[int](c, d.Key).Stale || Get[int](c, "unrelated").Stale {
		t.Fatal("resync crossed its source scope")
	}
}

// TestSharedWatcherClose releases resources when either the handle, caller, or Client ends its lifetime.
func TestSharedWatcherClose(t *testing.T) {
	for _, closer := range []string{"watch", "context", "client"} {
		t.Run(closer, func(t *testing.T) {
			store := &sharedNotificationStore{SharedStore: newSharedTestStore()}
			c := NewClient(Options{})
			t.Cleanup(c.Close)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			watch, err := c.WatchShared(ctx, store, InvalidateOptions{RefetchAfterLoad: true})
			if err != nil {
				t.Fatal(err)
			}
			switch closer {
			case "watch":
				watch.Close()
			case "context":
				cancel()
			case "client":
				c.Close()
			}
			select {
			case <-store.streams[0].closed:
			case <-time.After(3 * time.Second):
				t.Fatal("subscription leaked")
			}
			watch.Close()
		})
	}
}

// TestSharedWatcherDuplicateReceipt applies one operation once even when the watcher and caller both receive it.
func TestSharedWatcherDuplicateReceipt(t *testing.T) {
	store := &sharedNotificationStore{SharedStore: newSharedTestStore()}
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { return 7, nil })
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	if _, err := d.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	diagnostic, err := c.SubscribeEvents(EventOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(diagnostic.Close)
	if err := d.InvalidateShared(context.Background(), c, InvalidateOptions{Refetch: RefetchNone}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.applySharedInvalidation(
		store.InvalidationScope(),
		SharedInvalidation{ID: "event:1", Key: d.Shared.options.Key},
		InvalidateOptions{Refetch: RefetchNone},
	); err != nil {
		t.Fatal(err)
	}
	count := 0
	for len(diagnostic.Events()) > 0 {
		if event := <-diagnostic.Events(); event.Kind == EventInvalidated {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("receipt applied %d times", count)
	}
}

// TagsEnabled identifies the explicit capability selected by a test case.
func (s *sharedNotificationStore) TagsEnabled() bool { return s.enableTags }

// ReadTagged supplies copied relationships to isolate root tag binding tests from the Redis protocol.
func (s *sharedNotificationStore) ReadTagged(ctx context.Context, key string, tags []string) (SharedRecord, error) {
	if s.taggedRead != nil {
		return s.taggedRead(ctx, key, tags)
	}
	record, err := s.Read(ctx, key)
	record.Tags = slices.Clone(tags)
	return record, err
}

// TestSharedUnknownTagNotificationConfirmsSnapshot closes the interval before remote relationships become known locally.
func TestSharedUnknownTagNotificationConfirmsSnapshot(t *testing.T) {
	backend := newSharedTestStore()
	store := &sharedNotificationStore{SharedStore: backend, enableTags: true}
	shared, err := NewSharedCache[int](
		store,
		SharedCacheOptions[int]{Key: "detail", Format: "int.v1", TTL: time.Hour, ShareTags: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	d, err := QueryDefinition("detail", func(context.Context) (int, error) { return 7, nil })
	if err != nil {
		t.Fatal(err)
	}
	d.Shared = shared
	d.Tags = []string{"declared"}
	bootstrap := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(bootstrap.Close)
	if _, err := d.Fetch(context.Background(), bootstrap); err != nil {
		t.Fatal(err)
	}
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	watch, err := c.WatchShared(context.Background(), store, InvalidateOptions{Refetch: RefetchNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	diagnostic, err := c.SubscribeEvents(EventOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(diagnostic.Close)
	// This already-known tagged query provides a deterministic barrier after the unknown-tag event is processed.
	sentinelShared, err := NewSharedCache[int](
		store,
		SharedCacheOptions[int]{Key: "sentinel", Format: "int.v1", TTL: time.Hour, ShareTags: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	sentinel, err := QueryDefinition("sentinel", func(context.Context) (int, error) { return 1, nil })
	if err != nil {
		t.Fatal(err)
	}
	sentinel.Shared = sentinelShared
	sentinel.Tags = []string{"remote-only"}
	if _, err := sentinel.Fetch(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int32
	store.taggedRead = func(ctx context.Context, key string, _ []string) (SharedRecord, error) {
		record, err := backend.Read(ctx, key)
		record.Tags = []string{"declared", "remote-only"}
		if reads.Add(1) == 1 {
			if err := backend.Invalidate(ctx, key); err != nil {
				return SharedRecord{}, err
			}
			store.emit(SharedInvalidation{ID: "hidden-tag-change", Tags: []string{"remote-only"}})
			barrier, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			for {
				select {
				case event := <-diagnostic.Events():
					if event.Kind == EventInvalidated && event.Key == sentinel.Key {
						return record, err
					}
				case <-barrier.Done():
					return SharedRecord{}, barrier.Err()
				}
			}
		}
		return record, err
	}
	d.Fetcher = func(context.Context) (int, error) { return 9, nil }
	if value, err := d.Fetch(context.Background(), c); err != nil || value != 9 {
		t.Fatalf("obsolete snapshot reused: %d, %v", value, err)
	}
}

// InvalidateTags models an operation receipt or failure; atomic generation fencing has adapter tests.
func (s *sharedNotificationStore) InvalidateTags(ctx context.Context, tags []string) (SharedInvalidation, error) {
	if err := ctx.Err(); err != nil {
		return SharedInvalidation{}, err
	}
	if !s.enableTags {
		return SharedInvalidation{}, ErrSharedCapability
	}
	if s.tagErr != nil {
		return SharedInvalidation{}, s.tagErr
	}
	s.mu.Lock()
	s.sequence++
	event := SharedInvalidation{ID: fmt.Sprintf("event:%d", s.sequence), Tags: slices.Clone(tags)}
	s.mu.Unlock()
	s.emit(event)
	return event, nil
}

// TestSharedTagsLocalSelection covers OR matching, tenant boundaries, empty input, failure, and cancellation.
func TestSharedTagsLocalSelection(t *testing.T) {
	store := &sharedNotificationStore{SharedStore: newSharedTestStore(), enableTags: true}
	shared, err := NewSharedCache[int](
		store,
		SharedCacheOptions[int]{Key: "detail", Format: "int.v1", TTL: time.Hour, ShareTags: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	d, err := QueryDefinition("detail", func(context.Context) (int, error) { return 7, nil })
	if err != nil {
		t.Fatal(err)
	}
	d.Shared = shared
	d.Tags = []string{"tenant:42:users"}
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	ctx := context.Background()
	if _, err := d.Fetch(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := Set(c, "tenant:99", 9); err != nil {
		t.Fatal(err)
	}
	if err := c.InvalidateSharedTags(ctx, store, []string{}); err != nil || store.sequence != 0 {
		t.Fatalf("empty = %v", err)
	}
	failure := errors.New("remote tag operation failed")
	store.tagErr = failure
	if err := c.InvalidateSharedTags(
		ctx,
		store,
		[]string{"tenant:42:users"},
	); !errors.Is(err, failure) ||
		Get[int](c, d.Key).Stale {
		t.Fatalf("failure = %v", err)
	}
	store.tagErr = nil
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := c.InvalidateSharedTags(canceled, store, d.Tags); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := c.InvalidateSharedTags(
		ctx,
		store,
		[]string{"unknown", "tenant:42:users"},
	); err != nil {
		t.Fatal(err)
	}
	if !Get[int](c, d.Key).Stale || Get[int](c, "tenant:99").Stale {
		t.Fatal("tag selection crossed scope or used AND")
	}
}

// TestSharedWatcherAutomaticRefresh uses enabled observers to refresh a notified key exactly once.
func TestSharedWatcherAutomaticRefresh(t *testing.T) {
	store := &sharedNotificationStore{SharedStore: newSharedTestStore()}
	var source, calls atomic.Int32
	source.Store(1)
	d := newSharedDefinition(
		t,
		store,
		func(context.Context) (int, error) { calls.Add(1); return int(source.Load()), nil },
	)
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	watch, err := c.WatchShared(ctx, store, InvalidateOptions{RefetchAfterLoad: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	if _, err := d.Fetch(ctx, c); err != nil {
		t.Fatal(err)
	}
	h := d.Query(c)
	t.Cleanup(h.Close)
	source.Store(2)
	if _, err := store.InvalidateKey(ctx, d.Shared.options.Key); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case state := <-h.Updates():
			if state.Data == 2 && !state.Stale && !state.Fetching {
				if calls.Load() != 2 {
					t.Fatalf("source calls = %d", calls.Load())
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("enabled query did not refresh")
		}
	}
}

// TestSharedRemoteFenceKeepsLocalResultStale rejects freshness after a remote change even without notification delivery.
func TestSharedRemoteFenceKeepsLocalResultStale(t *testing.T) {
	store := newSharedTestStore()
	started, release := make(chan struct{}), make(chan struct{})
	d := newSharedDefinition(t, store, func(context.Context) (int, error) { close(started); <-release; return 7, nil })
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	done := make(chan error, 1)
	go func() { _, err := d.Fetch(context.Background(), c); done <- err }()
	<-started
	if err := store.Invalidate(context.Background(), d.Shared.options.Key); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if state := Get[int](c, d.Key); !state.HasData || !state.Stale || state.Data != 7 {
		t.Fatalf("remote conflict = %+v", state)
	}
}
