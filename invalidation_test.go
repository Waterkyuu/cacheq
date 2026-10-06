package query

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newInvalidationClient freezes freshness checks while tests explicitly drive query completions.
func newInvalidationClient(t *testing.T) *Client[string, int] {
	t.Helper()
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	client := NewClient[string, int](Options{StaleTime: time.Hour, Clock: func() time.Time { return now }})
	t.Cleanup(client.Close)
	return client
}

// TestInvalidateMany selects related keys, deduplicates them, and controls observed background refreshes.
func TestInvalidateMany(t *testing.T) {
	for name, mode := range map[string]RefetchMode{"observed": RefetchObserved, "none": RefetchNone} {
		t.Run(name, func(t *testing.T) {
			client := newInvalidationClient(t)
			calls := make(map[string]*atomic.Int32)
			fetchers := make(map[string]Fetcher[int])
			updates := make(map[string]<-chan Snapshot[int])
			for _, key := range []string{"user:42", "users", "unrelated", "inactive"} {
				calls[key] = &atomic.Int32{}
				fetchers[key] = func(context.Context) (int, error) { return int(calls[key].Add(1)), nil }
				if _, err := client.Fetch(context.Background(), key, fetchers[key]); err != nil {
					t.Fatal(err)
				}
				if key != "inactive" {
					var unsubscribe func()
					updates[key], unsubscribe = client.Subscribe(key)
					t.Cleanup(unsubscribe)
					<-updates[key]
				}
			}
			client.InvalidateMany([]string{"user:42", "users", "inactive", "user:42", "users"}, InvalidateOptions{
				Refetch: mode,
			})
			for _, key := range []string{"user:42", "users"} {
				if mode == RefetchObserved {
					state := awaitState(
						t,
						updates[key],
						func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching },
					)
					if state.Stale || state.Err != nil || calls[key].Load() != 2 {
						t.Fatalf("refreshed %s = %+v; calls = %d", key, state, calls[key].Load())
					}
					continue
				}
				state := awaitState(t, updates[key], func(s Snapshot[int]) bool { return s.Stale })
				if !state.HasData || state.Data != 1 || state.Fetching || calls[key].Load() != 1 {
					t.Fatalf("deferred %s = %+v; calls = %d", key, state, calls[key].Load())
				}
			}
			if state := client.Snapshot("inactive"); !state.Stale || state.Fetching || state.Data != 1 {
				t.Fatalf("inactive query = %+v", state)
			}
			if calls["inactive"].Load() != 1 {
				t.Fatal("invalidation refreshed an unobserved key")
			}
			if state := client.Snapshot("unrelated"); state.Stale || state.Fetching || state.Data != 1 {
				t.Fatalf("unrelated query = %+v", state)
			}
			if calls["unrelated"].Load() != 1 {
				t.Fatal("invalidation refreshed an unrelated key")
			}
			if mode == RefetchNone {
				state := client.Query("user:42", fetchers["user:42"])
				if state.Data != 1 || !state.Fetching || !state.Stale {
					t.Fatalf("deferred background read = %+v", state)
				}
				state = awaitState(
					t,
					updates["user:42"],
					func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching },
				)
				if state.Stale {
					t.Fatalf("deferred background result = %+v", state)
				}
				if value, err := client.Fetch(
					context.Background(),
					"users",
					fetchers["users"],
				); value != 2 ||
					err != nil {
					t.Fatalf("deferred blocking read = %d, %v", value, err)
				}
			}
		})
	}
}

// TestInvalidateManyDuringLoad retains in-flight invalidation without canceling or duplicating work.
func TestInvalidateManyDuringLoad(t *testing.T) {
	for name, mode := range map[string]RefetchMode{"observed": RefetchObserved, "none": RefetchNone} {
		t.Run(name, func(t *testing.T) {
			client := newInvalidationClient(t)
			var calls atomic.Int32
			started, release := make(chan struct{}), make(chan struct{})
			fetch := func(ctx context.Context) (int, error) {
				call := calls.Add(1)
				if call == 2 {
					close(started)
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-release:
					}
				}
				return int(call), nil
			}
			if _, err := client.Fetch(context.Background(), "users", fetch); err != nil {
				t.Fatal(err)
			}
			_, unsubscribe := client.Subscribe("users")
			t.Cleanup(unsubscribe)
			finished := make(chan error, 1)
			go func() {
				_, err := client.Refetch(context.Background(), "users", fetch)
				finished <- err
			}()
			<-started
			client.InvalidateMany([]string{"users", "users"}, InvalidateOptions{Refetch: mode})
			if state := client.Snapshot("users"); !state.Fetching || state.Data != 1 || !state.Stale {
				t.Fatalf("in-flight invalidation = %+v", state)
			}
			close(release)
			if err := <-finished; err != nil {
				t.Fatal(err)
			}
			if state := client.Snapshot("users"); state.Fetching || state.Data != 2 || !state.Stale {
				t.Fatalf("invalidated completion = %+v", state)
			}
			if value, err := client.Fetch(context.Background(), "users", fetch); value != 3 || err != nil {
				t.Fatalf("read after invalidated completion = %d, %v", value, err)
			}
			if state := client.Snapshot("users"); state.Stale || calls.Load() != 3 {
				t.Fatalf("fresh completion = %+v; calls = %d", state, calls.Load())
			}
		})
	}
}

// TestInvalidateManyFailure isolates a failed refresh while retaining old data and refreshing other keys.
func TestInvalidateManyFailure(t *testing.T) {
	client := newInvalidationClient(t)
	failure := errors.New("offline")
	updates := make(map[string]<-chan Snapshot[int])
	for _, key := range []string{"user:42", "users"} {
		var calls atomic.Int32
		fetch := func(context.Context) (int, error) {
			call := calls.Add(1)
			if key == "user:42" && call > 1 {
				return 0, failure
			}
			return int(call), nil
		}
		if _, err := client.Fetch(context.Background(), key, fetch); err != nil {
			t.Fatal(err)
		}
		var unsubscribe func()
		updates[key], unsubscribe = client.Subscribe(key)
		t.Cleanup(unsubscribe)
		<-updates[key]
	}
	client.InvalidateMany([]string{"user:42", "users"}, InvalidateOptions{})
	state := awaitState(t, updates["user:42"], func(s Snapshot[int]) bool { return s.Err != nil && !s.Fetching })
	if !state.HasData || state.Data != 1 || !state.Stale || !errors.Is(state.Err, failure) {
		t.Fatalf("failed related query = %+v", state)
	}
	state = awaitState(t, updates["users"], func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	if state.Stale || state.Err != nil {
		t.Fatalf("successful related query = %+v", state)
	}
}

// TestInvalidateManyBoundaries covers empty batches, valid zero keys, unknown keys, and a closed client.
func TestInvalidateManyBoundaries(t *testing.T) {
	client := newInvalidationClient(t)
	client.Set("", 42)
	for _, keys := range [][]string{nil, {}} {
		client.InvalidateMany(keys, InvalidateOptions{})
		if state := client.Snapshot(""); state.Stale || state.Data != 42 {
			t.Fatalf("empty batch changed cached data: %+v", state)
		}
	}
	client.InvalidateMany([]string{"", "missing"}, InvalidateOptions{Refetch: RefetchNone})
	if state := client.Snapshot(""); !state.Stale || state.Data != 42 || state.Fetching {
		t.Fatalf("zero key invalidation = %+v", state)
	}
	if state := client.Snapshot("missing"); state.HasData || state.Fetching || state.Status != Idle {
		t.Fatalf("unknown key invalidation = %+v", state)
	}
	client.Close()
	client.InvalidateMany([]string{"", "missing"}, InvalidateOptions{})
	if state := client.Snapshot(""); state.HasData || state.Fetching || !errors.Is(state.Err, ErrClosed) {
		t.Fatalf("closed client invalidation = %+v", state)
	}
}

// TestInvalidateWhere applies refresh policy only to matching keys with enabled observers.
func TestInvalidateWhere(t *testing.T) {
	for name, mode := range map[string]RefetchMode{"observed": RefetchObserved, "none": RefetchNone} {
		t.Run(name, func(t *testing.T) {
			client := newInvalidationClient(t)
			calls := make(map[string]*atomic.Int32)
			observers := make(map[string]*Observer[string, int])
			for _, key := range []string{"users:42", "users:list", "users:unobserved", "projects:list"} {
				calls[key] = &atomic.Int32{}
				fetch := func(context.Context) (int, error) { return int(calls[key].Add(1)), nil }
				if _, err := client.Fetch(context.Background(), key, fetch); err != nil {
					t.Fatal(err)
				}
				if key != "users:unobserved" {
					observer := client.Observe(key, fetch, ObserveOptions{Enabled: key != "users:list"})
					t.Cleanup(observer.Close)
					observers[key] = observer
					<-observer.Updates()
				}
			}
			client.InvalidateWhere(func(key string) bool {
				return strings.HasPrefix(key, "users:")
			}, InvalidateOptions{Refetch: mode})
			active := observers["users:42"]
			if mode == RefetchObserved {
				state := awaitState(
					t,
					active.Updates(),
					func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching },
				)
				if state.Stale || state.Err != nil || calls["users:42"].Load() != 2 {
					t.Fatalf("matching active query = %+v", state)
				}
			} else {
				if state := active.Snapshot(); !state.Stale || state.Fetching || state.Data != 1 {
					t.Fatalf("deferred matching query = %+v", state)
				}
				active.SetEnabled(false)
				active.SetEnabled(true)
				state := awaitState(
					t,
					active.Updates(),
					func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching },
				)
				if state.Stale {
					t.Fatalf("re-enabled matching query = %+v", state)
				}
			}
			for _, key := range []string{"users:list", "users:unobserved"} {
				if state := client.Snapshot(
					key,
				); !state.Stale || state.Fetching || state.Data != 1 ||
					calls[key].Load() != 1 {
					t.Fatalf("matching passive query %s = %+v", key, state)
				}
			}
			if state := observers["projects:list"].Snapshot(); state.Stale || state.Data != 1 ||
				calls["projects:list"].Load() != 1 {
				t.Fatalf("nonmatching query = %+v", state)
			}
		})
	}
}

// TestInvalidateWherePending includes loading-only keys and evaluates cached/loading keys just once.
func TestInvalidateWherePending(t *testing.T) {
	for _, cached := range []bool{false, true} {
		client := newInvalidationClient(t)
		if cached {
			client.Set("users:42", 7)
			client.Invalidate("users:42")
		}
		updates, unsubscribe := client.Subscribe("users:42")
		t.Cleanup(unsubscribe)
		var calls atomic.Int32
		started, release := make(chan struct{}), make(chan struct{})
		fetch := func(ctx context.Context) (int, error) {
			call := calls.Add(1)
			if call == 1 {
				close(started)
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-release:
				}
			}
			return int(call), nil
		}
		client.Query("users:42", fetch)
		<-started
		matches := 0
		client.InvalidateWhere(func(key string) bool {
			matches++
			return key == "users:42"
		}, InvalidateOptions{Refetch: RefetchNone})
		close(release)
		state := awaitState(t, updates, func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
		if !state.Stale || state.Data != 1 || state.Err != nil || matches != 1 {
			t.Fatalf("pending predicate invalidation = %+v; matches = %d", state, matches)
		}
		if value, err := client.Fetch(context.Background(), "users:42", fetch); value != 2 || err != nil {
			t.Fatalf("read after predicate invalidation = %d, %v", value, err)
		}
	}
}

// TestInvalidateWhereReentrant evaluates callbacks outside locks and skips keys removed while matching.
func TestInvalidateWhereReentrant(t *testing.T) {
	client := newInvalidationClient(t)
	for _, key := range []string{"selected", "removed", "untouched"} {
		client.Set(key, 7)
	}
	finished := make(chan struct{})
	go func() {
		client.InvalidateWhere(func(key string) bool {
			client.Snapshot(key)
			client.Set("new", 99)
			client.Remove("removed")
			return key != "untouched"
		}, InvalidateOptions{Refetch: RefetchNone})
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("predicate could not reenter the client")
	}
	if state := client.Snapshot("selected"); !state.Stale || state.Data != 7 {
		t.Fatalf("selected query = %+v", state)
	}
	for key, value := range map[string]int{"untouched": 7, "new": 99} {
		if state := client.Snapshot(key); state.Stale || state.Data != value {
			t.Fatalf("excluded query %s = %+v", key, state)
		}
	}
	seen := make(map[string]bool)
	client.InvalidateWhere(func(key string) bool {
		seen[key] = true
		return false
	}, InvalidateOptions{})
	if seen["removed"] {
		t.Fatal("predicate invalidation recreated removed state")
	}
}

// TestInvalidateWhereBoundaries leaves state unchanged for nil, nonmatching, or closed-client predicates.
func TestInvalidateWhereBoundaries(t *testing.T) {
	client := newInvalidationClient(t)
	client.Set("users", 7)
	client.InvalidateWhere(nil, InvalidateOptions{})
	matches := 0
	client.InvalidateWhere(func(string) bool {
		matches++
		return false
	}, InvalidateOptions{})
	if state := client.Snapshot("users"); state.Stale || state.Data != 7 || matches != 1 {
		t.Fatalf("nonmatching predicate = %+v; matches = %d", state, matches)
	}
	client.Close()
	client.InvalidateWhere(func(string) bool {
		matches++
		return true
	}, InvalidateOptions{})
	if matches != 1 {
		t.Fatal("closed client evaluated its predicate")
	}
	if state := client.Snapshot("users"); state.HasData || !errors.Is(state.Err, ErrClosed) {
		t.Fatalf("closed predicate invalidation = %+v", state)
	}
}

// scopedQueryKey identifies a resource within one tenant without encoding fields into a string.
type scopedQueryKey struct {
	// resource identifies the kind of data returned by this query.
	resource string
	// tenant isolates queries belonging to separate tenants.
	tenant int
}

// TestInvalidateWhereTypedKeys matches structured keys without imposing a string or prefix convention.
func TestInvalidateWhereTypedKeys(t *testing.T) {
	client := NewClient[scopedQueryKey, int](Options{
		StaleTime: time.Hour, Clock: func() time.Time { return time.Unix(0, 0) },
	})
	t.Cleanup(client.Close)
	keys := []scopedQueryKey{
		{resource: "users", tenant: 42},
		{resource: "users", tenant: 7},
		{resource: "projects", tenant: 42},
	}
	for _, key := range keys {
		client.Set(key, 1)
	}
	client.InvalidateWhere(func(key scopedQueryKey) bool {
		return key.resource == "users" && key.tenant == 42
	}, InvalidateOptions{Refetch: RefetchNone})
	for _, key := range keys {
		if state := client.Snapshot(key); state.Stale != (key == keys[0]) || state.Data != 1 {
			t.Fatalf("typed query %v = %+v", key, state)
		}
	}
}
