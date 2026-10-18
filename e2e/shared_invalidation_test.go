package e2e_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Waterkyuu/soroi"
)

// httpInvalidationStore provides independent subscription streams for two HTTP query clients.
type httpInvalidationStore struct {
	// httpSharedStore owns encoded values and conditional writes.
	httpSharedStore
	// streams owns two controlled notification streams.
	streams []*httpInvalidationStream
	// guard protects stream registration.
	guard sync.Mutex
}

// TagsEnabled enables the controlled tag capability for this integration transport.
func (s *httpInvalidationStore) TagsEnabled() bool { return true }

// ReadTagged returns declared relationships while delegating byte storage to the controlled cache.
func (s *httpInvalidationStore) ReadTagged(
	ctx context.Context,
	key string,
	tags []string,
) (soroi.SharedRecord, error) {
	record, err := s.Read(ctx, key)
	record.Tags = slices.Clone(tags)
	return record, err
}

// InvalidateTags broadcasts an OR-matching receipt; real Redis tests verify remote tag generation fencing.
func (s *httpInvalidationStore) InvalidateTags(_ context.Context, tags []string) (soroi.SharedInvalidation, error) {
	event := soroi.SharedInvalidation{ID: "http-tag-change", Tags: slices.Clone(tags)}
	s.guard.Lock()
	defer s.guard.Unlock()
	for _, stream := range s.streams {
		stream.events <- event
	}
	return event, nil
}

// TestHTTPSharedTagInvalidation refreshes a second client's data after a committed HTTP mutation.
func TestHTTPSharedTagInvalidation(t *testing.T) {
	api := newUserAPI(t)
	store := &httpInvalidationStore{}
	shared, err := soroi.NewSharedCache[user](
		store,
		soroi.SharedCacheOptions[user]{Key: "user:42", Format: "user.v1", TTL: time.Minute, ShareTags: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	d, err := soroi.QueryDefinition(
		"user",
		readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	d.Shared = shared
	d.Tags = []string{"tenant:42:users"}
	a, b := soroi.NewClient(
		soroi.Options{StaleTime: time.Minute},
	), soroi.NewClient(
		soroi.Options{StaleTime: time.Minute},
	)
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	watch, err := b.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	if _, err := d.Fetch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Fetch(ctx, b); err != nil {
		t.Fatal(err)
	}
	h := d.Query(b)
	t.Cleanup(h.Close)
	api.update(t, user{Name: "Bob"})
	if err := a.InvalidateSharedTags(ctx, store, d.Tags); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case state := <-h.Updates():
			if state.Stale {
				value, err := d.Fetch(ctx, b)
				if err != nil || value.Name != "Bob" || api.count("/users/42") != 2 {
					t.Fatalf("tag refresh = %+v, %v", value, err)
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("tag notification did not reach second client")
		}
	}
}

// httpInvalidationStream delivers remote notifications without real network dependencies.
type httpInvalidationStream struct {
	// events contains future invalidations.
	events chan soroi.SharedInvalidation
}

// Events exposes controlled delivery to the root watcher.
func (s *httpInvalidationStream) Events() <-chan soroi.SharedInvalidation { return s.events }

// Close requires no external cleanup in the controlled test transport.
func (s *httpInvalidationStream) Close() error { return nil }

// InvalidationScope distinguishes this store from unrelated query configurations.
func (s *httpInvalidationStore) InvalidationScope() string { return "http-invalidation" }

// SubscribeInvalidations registers before issuing writes, preventing a setup race.
func (s *httpInvalidationStore) SubscribeInvalidations(context.Context) (soroi.SharedInvalidationSubscription, error) {
	s.guard.Lock()
	defer s.guard.Unlock()
	stream := &httpInvalidationStream{events: make(chan soroi.SharedInvalidation, 8)}
	s.streams = append(s.streams, stream)
	return stream, nil
}

// InvalidateKey advances storage state and notifies every independently owned Client.
func (s *httpInvalidationStore) InvalidateKey(ctx context.Context, key string) (soroi.SharedInvalidation, error) {
	if err := s.Invalidate(ctx, key); err != nil {
		return soroi.SharedInvalidation{}, err
	}
	event := soroi.SharedInvalidation{ID: "http-key-change", Key: key}
	s.guard.Lock()
	defer s.guard.Unlock()
	for _, stream := range s.streams {
		stream.events <- event
	}
	return event, nil
}

// TestHTTPSharedInvalidation invalidates a second client's fresh subscribed result after a source mutation.
func TestHTTPSharedInvalidation(t *testing.T) {
	api := newUserAPI(t)
	store := &httpInvalidationStore{}
	shared, err := soroi.NewSharedCache[user](
		store,
		soroi.SharedCacheOptions[user]{Key: "user:42", Format: "user.v1", TTL: time.Minute},
	)
	if err != nil {
		t.Fatal(err)
	}
	d, err := soroi.QueryDefinition(
		"user",
		readJSON[user](api.server.Client(), api.server.URL+"/users/42"),
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	d.Shared = shared
	a, b := soroi.NewClient(
		soroi.Options{StaleTime: time.Minute},
	), soroi.NewClient(
		soroi.Options{StaleTime: time.Minute},
	)
	t.Cleanup(a.Close)
	t.Cleanup(b.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	watch, err := b.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(watch.Close)
	if _, err := d.Fetch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Fetch(ctx, b); err != nil {
		t.Fatal(err)
	}
	h := d.Query(b)
	t.Cleanup(h.Close)
	if err := d.InvalidateShared(ctx, a, soroi.InvalidateOptions{Refetch: soroi.RefetchNone}); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case state := <-h.Updates():
			if state.Stale {
				if !state.HasData || state.Data.Name != "Alice" || state.Fetching {
					t.Fatalf("notified = %+v", state)
				}
				if _, err := d.Fetch(ctx, b); err != nil {
					t.Fatal(err)
				}
				if api.count("/users/42") != 2 {
					t.Fatal("remote notification did not force the next source read")
				}
				return
			}
		case <-ctx.Done():
			t.Fatal("notification did not reach second client")
		}
	}
}
