package cacheq

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestCapacityLRU evicts the least recently used result after reads, writes, or handle creation.
func TestCapacityLRU(t *testing.T) {
	for _, use := range []string{"get", "fetch", "query", "set", "invalidate"} {
		t.Run(use, func(t *testing.T) {
			c := NewClient(Options{StaleTime: time.Hour, MaxEntries: 2})
			t.Cleanup(c.Close)
			Set(c, "a", 1)
			Set(c, "b", 2)
			switch use {
			case "get":
				Get[int](c, "a")
			case "fetch":
				if _, err := Fetch[int](context.Background(), c, "a", nil); err != nil {
					t.Fatal(err)
				}
			case "query":
				query := Query(c, "a", func(context.Context) (int, error) { return 4, nil })
				t.Cleanup(query.Close)
			case "set":
				Set(c, "a", 4)
			case "invalidate":
				c.Invalidate("a", InvalidateOptions{Refetch: RefetchNone})
			}
			Set(c, "c", 3)
			if Get[int](c, "b").HasData || !Get[int](c, "a").HasData || !Get[int](c, "c").HasData {
				t.Fatal("capacity did not evict only the least recently used result")
			}
		})
	}
}

// TestCapacityDisabled preserves all cached results for zero and negative limits.
func TestCapacityDisabled(t *testing.T) {
	for _, limit := range []int{0, -1} {
		c := NewClient(Options{MaxEntries: limit})
		for i := 0; i < 3; i++ {
			Set(c, i, i)
		}
		for i := 0; i < 3; i++ {
			if state := Get[int](c, i); !state.HasData || state.Data != i {
				t.Fatalf("limit %d, key %d = %+v", limit, i, state)
			}
		}
		c.Close()
	}
}

// TestCapacitySubscription notifies evicted observers and preserves their static result contract.
func TestCapacitySubscription(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour, MaxEntries: 1})
	t.Cleanup(c.Close)
	Set(c, "a", 1)
	query := Query(c, "a", func(context.Context) (int, error) { return 3, nil })
	t.Cleanup(query.Close)
	<-query.Updates()
	Set(c, "b", "second")
	state := <-query.Updates()
	if state.HasData || state.Data != 0 || state.Fetching || state.Status != Idle {
		t.Fatalf("evicted subscription = %+v", state)
	}
	if err := Set(c, "a", "wrong type"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("evicted binding = %v", err)
	}
	if value, err := query.Refetch(context.Background()); value != 3 || err != nil {
		t.Fatalf("refetch after eviction = %d, %v", value, err)
	}
	if Get[string](c, "b").HasData {
		t.Fatal("refetch exceeded capacity")
	}
}

// TestCapacityActiveLoad evicts old data without canceling a refresh or accepting a conflicting result type.
func TestCapacityActiveLoad(t *testing.T) {
	c := NewClient(Options{MaxEntries: 1})
	t.Cleanup(c.Close)
	Set(c, "a", 1)
	started, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := Fetch(context.Background(), c, "a", func(ctx context.Context) (int, error) {
			close(started)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-release:
				return 2, nil
			}
		})
		finished <- err
	}()
	<-started
	Set(c, "b", "second")
	if state := Get[int](c, "a"); state.HasData || !state.Fetching || state.Status != Pending {
		t.Fatalf("evicted refresh = %+v", state)
	}
	if err := Set(c, "a", "wrong type"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("active binding = %v", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if state := Get[int](c, "a"); !state.HasData || state.Data != 2 || Get[string](c, "b").HasData {
		t.Fatalf("completed refresh = %+v", state)
	}
}

// TestCapacityErrors counts retained failures and releases the evicted key for another result type.
func TestCapacityErrors(t *testing.T) {
	c := NewClient(Options{MaxEntries: 1})
	t.Cleanup(c.Close)
	Set(c, "a", 1)
	failure := errors.New("offline")
	_, err := Fetch(context.Background(), c, "b", func(context.Context) (string, error) { return "", failure })
	if !errors.Is(err, failure) || Get[int](c, "a").HasData {
		t.Fatalf("cached failure did not consume capacity: %v", err)
	}
	Set(c, "c", true)
	if state := Get[string](c, "b"); state.Err != nil || state.Status != Idle {
		t.Fatalf("evicted failure = %+v", state)
	}
	if err := Set(c, "b", 2); err != nil {
		t.Fatalf("evicted key retained an unused type binding: %v", err)
	}
}

// TestCapacityRemoval releases capacity records so discarded keys cannot evict later writes.
func TestCapacityRemoval(t *testing.T) {
	for _, action := range []string{"remove", "clear", "gc"} {
		t.Run(action, func(t *testing.T) {
			c, scheduler := newGCClient(t, time.Minute)
			c.options.MaxEntries = 1
			Set(c, "a", 1)
			old := scheduler.latest(t)
			switch action {
			case "remove":
				c.Remove("a")
			case "clear":
				c.Clear()
			case "gc":
				scheduler.now.Add(int64(time.Minute))
				old.callback()
			}
			Set(c, "b", 2)
			evicted := scheduler.latest(t)
			Set(c, "c", 3)
			if !evicted.stopped.Load() {
				t.Fatal("capacity eviction retained a cleanup timer")
			}
			old.callback()
			if Get[int](c, "a").HasData || Get[int](c, "b").HasData || !Get[int](c, "c").HasData {
				t.Fatal("discarded capacity records affected later cache state")
			}
		})
	}
}

// TestCapacityHandleClose releases a consumer without making its unread data more recently used.
func TestCapacityHandleClose(t *testing.T) {
	c := NewClient(Options{MaxEntries: 2, StaleTime: time.Hour})
	t.Cleanup(c.Close)
	Set(c, "a", 1)
	query := Query(c, "a", func(context.Context) (int, error) { return 4, nil })
	Set(c, "b", 2)
	query.Close()
	Set(c, "c", 3)
	if Get[int](c, "a").HasData || !Get[int](c, "b").HasData || !Get[int](c, "c").HasData {
		t.Fatal("closing a handle changed recency without reading its data")
	}
}

// TestCapacityEvictedHandleRelease removes an unused type binding after the final evicted consumer leaves.
func TestCapacityEvictedHandleRelease(t *testing.T) {
	c := NewClient(Options{MaxEntries: 1, StaleTime: time.Hour})
	t.Cleanup(c.Close)
	Set(c, "a", 1)
	query := Query(c, "a", func(context.Context) (int, error) { return 3, nil })
	Set(c, "b", 2)
	query.Close()
	if err := Set(c, "a", "new type"); err != nil {
		t.Fatalf("released evicted consumer retained metadata: %v", err)
	}
}

// TestCapacityCanceledBinding releases metadata from a canceled initial load without requiring timed GC.
func TestCapacityCanceledBinding(t *testing.T) {
	c := NewClient(Options{MaxEntries: 1})
	t.Cleanup(c.Close)
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := Fetch(context.Background(), c, "a", func(ctx context.Context) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		})
		finished <- err
	}()
	<-started
	c.Cancel("a")
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled initial load = %v", err)
	}
	if err := Set(c, "a", "new type"); err != nil {
		t.Fatalf("canceled load retained unowned metadata: %v", err)
	}
}
