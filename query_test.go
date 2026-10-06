package query

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// awaitState waits for an observable state transition with a deadlock guard independent of cache timing.
func awaitState[V any](t *testing.T, updates <-chan Snapshot[V], matches func(Snapshot[V]) bool) Snapshot[V] {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case state, open := <-updates:
			if !open {
				t.Fatal("subscription closed before the expected state")
			}
			if matches(state) {
				return state
			}
		case <-timer.C:
			t.Fatal("query did not publish the expected state")
		}
	}
}

// TestQueryBackgroundRefresh exposes stale data immediately and shares updates between components.
func TestQueryBackgroundRefresh(t *testing.T) {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	client := NewClient[string, string](Options{StaleTime: time.Hour, Clock: func() time.Time { return now }})
	defer client.Close()
	client.Set("models", "old")
	first, unsubscribeFirst := client.Subscribe("models")
	defer unsubscribeFirst()
	second, unsubscribeSecond := client.Subscribe("models")
	defer unsubscribeSecond()
	if state := <-first; !state.HasData || state.Stale || state.Data != "old" {
		t.Fatalf("fresh state = %+v", state)
	}
	<-second
	now = now.Add(time.Hour)
	started, release := make(chan struct{}), make(chan struct{})
	fetch := func(context.Context) (string, error) {
		close(started)
		<-release
		return "new", nil
	}
	state := client.Query("models", fetch)
	if !state.HasData || !state.Stale || !state.Fetching || state.Status != Success || state.Data != "old" {
		t.Fatalf("background state = %+v", state)
	}
	<-started
	close(release)
	for _, updates := range []<-chan Snapshot[string]{first, second} {
		state := awaitState(t, updates, func(s Snapshot[string]) bool { return s.Data == "new" && !s.Fetching })
		if state.Stale || state.Err != nil {
			t.Fatalf("refreshed state = %+v", state)
		}
	}
}

// TestObservedInvalidation refreshes active subscribers while inactive queries only become stale.
func TestObservedInvalidation(t *testing.T) {
	client := NewClient[string, int](Options{StaleTime: time.Hour})
	defer client.Close()
	var calls atomic.Int32
	fetch := func(context.Context) (int, error) { return int(calls.Add(1)), nil }
	if _, err := client.Fetch(context.Background(), "a", fetch); err != nil {
		t.Fatal(err)
	}
	client.Invalidate("a")
	if state := client.Snapshot("a"); !state.Stale || state.Fetching || calls.Load() != 1 {
		t.Fatalf("inactive invalidation = %+v", state)
	}
	updates, unsubscribe := client.Subscribe("a")
	defer unsubscribe()
	client.Invalidate("a")
	state := awaitState(t, updates, func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	if state.Stale || calls.Load() != 2 {
		t.Fatalf("active invalidation = %+v; loads = %d", state, calls.Load())
	}
}

// TestFailedRefreshRetainsData keeps a prior result available while exposing its stale error state.
func TestFailedRefreshRetainsData(t *testing.T) {
	client := NewClient[string, string](Options{StaleTime: time.Hour})
	defer client.Close()
	client.Set("a", "available")
	failure := errors.New("offline")
	_, err := client.Refetch(context.Background(), "a", func(context.Context) (string, error) { return "", failure })
	state := client.Snapshot("a")
	if !errors.Is(err, failure) || !state.HasData || state.Data != "available" || !state.Stale ||
		state.Status != Error || !errors.Is(state.Err, failure) || state.Fetching {
		t.Fatalf("failed refresh = %+v, %v", state, err)
	}
}

// TestSetAndRemovePreventLateWrites detaches old loaders before local updates or removal.
func TestSetAndRemovePreventLateWrites(t *testing.T) {
	for _, action := range []string{"set", "remove"} {
		t.Run(action, func(t *testing.T) {
			client := NewClient[string, string](Options{StaleTime: time.Hour})
			defer client.Close()
			started, release := make(chan struct{}), make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				_, err := client.Fetch(context.Background(), "a", func(context.Context) (string, error) {
					close(started)
					<-release
					return "obsolete", nil
				})
				finished <- err
			}()
			<-started
			if action == "set" {
				client.Set("a", "local")
			} else {
				client.Remove("a")
			}
			close(release)
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatalf("detached load = %v", err)
			}
			state := client.Snapshot("a")
			if action == "set" && state.Data != "local" || action == "remove" && state.HasData {
				t.Fatalf("late load replaced state: %+v", state)
			}
		})
	}
}

// TestInvalidationDuringLoad prevents a pre-invalidation response from being considered fresh.
func TestInvalidationDuringLoad(t *testing.T) {
	client := NewClient[string, string](Options{StaleTime: time.Hour})
	defer client.Close()
	started, release := make(chan struct{}), make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := client.Fetch(context.Background(), "a", func(context.Context) (string, error) {
			close(started)
			<-release
			return "loaded", nil
		})
		finished <- err
	}()
	<-started
	client.Invalidate("a")
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if state := client.Snapshot("a"); !state.HasData || !state.Stale {
		t.Fatalf("in-flight invalidation = %+v", state)
	}
}

// TestClientLifecycle closes observers safely and rejects requests after shutdown.
func TestClientLifecycle(t *testing.T) {
	client := NewClient[string, int](Options{StaleTime: time.Hour})
	updates, unsubscribe := client.Subscribe("a")
	<-updates
	client.Set("a", 1)
	client.Set("a", 2)
	if state := <-updates; state.Data != 2 {
		t.Fatalf("slow observer missed the latest state: %+v", state)
	}
	client.Clear()
	if state := <-updates; state.HasData || state.Status != Idle {
		t.Fatalf("clear = %+v", state)
	}
	client.Close()
	client.Close()
	unsubscribe()
	unsubscribe()
	if _, open := <-updates; open {
		t.Fatal("closed client retained an observer")
	}
	if _, err := client.Fetch(context.Background(), "a", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed client fetch = %v", err)
	}
}

// TestPrefetchAndCancel verifies cache warming and explicit cancellation without an automatic restart.
func TestPrefetchAndCancel(t *testing.T) {
	client := NewClient[string, string](Options{StaleTime: time.Hour})
	defer client.Close()
	if err := client.Prefetch(context.Background(), "warm", func(context.Context) (string, error) {
		return "ready", nil
	}); err != nil {
		t.Fatal(err)
	}
	if value, err := client.Fetch(context.Background(), "warm", nil); value != "ready" || err != nil {
		t.Fatalf("prefetched data = %q, %v", value, err)
	}
	started := make(chan struct{})
	finished := make(chan error, 1)
	go func() {
		_, err := client.Fetch(context.Background(), "active", func(ctx context.Context) (string, error) {
			close(started)
			<-ctx.Done()
			return "", ctx.Err()
		})
		finished <- err
	}()
	<-started
	client.Cancel("active")
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("explicit cancellation = %v", err)
	}
	if state := client.Snapshot("active"); state.Fetching || state.HasData {
		t.Fatalf("canceled query = %+v", state)
	}
}

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

// TestObserveDisabled reads existing state without loading, including after automatic invalidation.
func TestObserveDisabled(t *testing.T) {
	for _, cache := range []string{"missing", "fresh", "stale"} {
		t.Run(cache, func(t *testing.T) {
			client := newInvalidationClient(t)
			if cache != "missing" {
				client.Set("users", 7)
			}
			if cache == "stale" {
				client.Invalidate("users")
			}
			var calls atomic.Int32
			observer := client.Observe("users", func(context.Context) (int, error) {
				calls.Add(1)
				return 8, nil
			}, ObserveOptions{})
			t.Cleanup(observer.Close)
			state := <-observer.Updates()
			if state.Fetching || calls.Load() != 0 || state.HasData != (cache != "missing") {
				t.Fatalf("disabled initial state = %+v; calls = %d", state, calls.Load())
			}
			if state.Stale != (cache != "fresh") {
				t.Fatalf("disabled initial freshness = %+v", state)
			}
			client.Invalidate("users")
			state = awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Stale })
			if state.Fetching || calls.Load() != 0 {
				t.Fatalf("disabled invalidation = %+v; calls = %d", state, calls.Load())
			}
			client.InvalidateMany([]string{"users"}, InvalidateOptions{})
			if state = observer.Snapshot(); state.Fetching || calls.Load() != 0 {
				t.Fatalf("disabled batch invalidation = %+v; calls = %d", state, calls.Load())
			}
		})
	}
}

// TestObserveEnableTransitions requests missing or stale data and reuses fresh data when enabling.
func TestObserveEnableTransitions(t *testing.T) {
	client := newInvalidationClient(t)
	var calls atomic.Int32
	observer := client.Observe("users", func(context.Context) (int, error) {
		return int(calls.Add(1)), nil
	}, ObserveOptions{})
	t.Cleanup(observer.Close)
	observer.SetEnabled(true)
	state := awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
	if state.Stale || state.Err != nil {
		t.Fatalf("initial enable = %+v", state)
	}
	observer.SetEnabled(false)
	observer.SetEnabled(true)
	observer.SetEnabled(true)
	if calls.Load() != 1 {
		t.Fatal("enabling requested already fresh data")
	}
	client.Invalidate("users")
	awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	observer.SetEnabled(false)
	client.InvalidateMany([]string{"users"}, InvalidateOptions{})
	state = awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Stale && !s.Fetching })
	if state.Data != 2 || calls.Load() != 2 {
		t.Fatalf("disabled refresh = %+v; calls = %d", state, calls.Load())
	}
	observer.SetEnabled(true)
	state = awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Data == 3 && !s.Fetching })
	if state.Stale || calls.Load() != 3 {
		t.Fatalf("enabled stale refresh = %+v; calls = %d", state, calls.Load())
	}
}

// TestObserveFreshData retains its loader for invalidation even when no initial request is needed.
func TestObserveFreshData(t *testing.T) {
	client := newInvalidationClient(t)
	client.Set("users", 7)
	var calls atomic.Int32
	observer := client.Observe("users", func(context.Context) (int, error) {
		calls.Add(1)
		return 8, nil
	}, ObserveOptions{Enabled: true})
	t.Cleanup(observer.Close)
	if state := observer.Snapshot(); state.Data != 7 || state.Fetching || calls.Load() != 0 {
		t.Fatalf("fresh observation = %+v", state)
	}
	client.Invalidate("users")
	state := awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Data == 8 && !s.Fetching })
	if state.Stale || calls.Load() != 1 {
		t.Fatalf("fresh cache invalidation = %+v; calls = %d", state, calls.Load())
	}
}

// TestObserveSharedEnablement isolates observers' permissions while sharing requests and updates.
func TestObserveSharedEnablement(t *testing.T) {
	client := newInvalidationClient(t)
	var disabledCalls, activeCalls atomic.Int32
	disabled := client.Observe("users", func(context.Context) (int, error) {
		disabledCalls.Add(1)
		return 99, nil
	}, ObserveOptions{})
	t.Cleanup(disabled.Close)
	started, release := make(chan struct{}), make(chan struct{})
	fetch := func(ctx context.Context) (int, error) {
		call := activeCalls.Add(1)
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
	first := client.Observe("users", fetch, ObserveOptions{Enabled: true})
	t.Cleanup(first.Close)
	<-started
	second := client.Observe("users", fetch, ObserveOptions{Enabled: true})
	t.Cleanup(second.Close)
	close(release)
	for _, observer := range []*Observer[string, int]{disabled, first, second} {
		awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
	}
	first.SetEnabled(false)
	client.Invalidate("users")
	awaitState(t, disabled.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	awaitState(t, second.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	if disabledCalls.Load() != 0 || activeCalls.Load() != 2 {
		t.Fatalf("shared loads = disabled %d, active %d", disabledCalls.Load(), activeCalls.Load())
	}
	second.SetEnabled(false)
	client.Invalidate("users")
	if state := first.Snapshot(); state.Fetching || !state.Stale || activeCalls.Load() != 2 {
		t.Fatalf("all observers disabled = %+v", state)
	}
	disabled.SetEnabled(true)
	state := awaitState(t, second.Updates(), func(s Snapshot[int]) bool { return s.Data == 99 && !s.Fetching })
	if state.Stale || disabledCalls.Load() != 1 || activeCalls.Load() != 2 {
		t.Fatalf("observer-specific loader = %+v", state)
	}
}

// TestObserveManualRequests allows explicit client calls while keeping subsequent automatic loads disabled.
func TestObserveManualRequests(t *testing.T) {
	for _, action := range []string{"fetch", "query", "refetch"} {
		t.Run(action, func(t *testing.T) {
			client := newInvalidationClient(t)
			var calls atomic.Int32
			fetch := func(context.Context) (int, error) { return int(calls.Add(1)), nil }
			observer := client.Observe("users", fetch, ObserveOptions{})
			t.Cleanup(observer.Close)
			switch action {
			case "fetch":
				if _, err := client.Fetch(context.Background(), "users", fetch); err != nil {
					t.Fatal(err)
				}
			case "query":
				client.Query("users", fetch)
			case "refetch":
				if _, err := client.Refetch(context.Background(), "users", fetch); err != nil {
					t.Fatal(err)
				}
			}
			awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Data == 1 && !s.Fetching })
			client.Invalidate("users")
			if state := observer.Snapshot(); state.Fetching || !state.Stale || calls.Load() != 1 {
				t.Fatalf("disabled retained loader = %+v; calls = %d", state, calls.Load())
			}
		})
	}
}

// TestObserveDisableDuringLoad preserves work already started and defers further automatic requests.
func TestObserveDisableDuringLoad(t *testing.T) {
	client := newInvalidationClient(t)
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	observer := client.Observe("users", func(ctx context.Context) (int, error) {
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
	}, ObserveOptions{Enabled: true})
	t.Cleanup(observer.Close)
	<-started
	observer.SetEnabled(false)
	client.Invalidate("users")
	close(release)
	state := awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if state.Data != 1 || state.Err != nil || !state.Stale || calls.Load() != 1 {
		t.Fatalf("disabled in-flight completion = %+v", state)
	}
	observer.SetEnabled(true)
	state = awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
	if state.Stale || state.Err != nil {
		t.Fatalf("re-enabled completion = %+v", state)
	}
}

// TestObserveFailure retains stale cache and publishes the error without loading while disabled.
func TestObserveFailure(t *testing.T) {
	client := newInvalidationClient(t)
	client.Set("users", 7)
	client.Invalidate("users")
	failure := errors.New("offline")
	var calls atomic.Int32
	observer := client.Observe("users", func(context.Context) (int, error) {
		calls.Add(1)
		return 0, failure
	}, ObserveOptions{Enabled: true})
	t.Cleanup(observer.Close)
	state := awaitState(t, observer.Updates(), func(s Snapshot[int]) bool { return s.Err != nil && !s.Fetching })
	if state.Data != 7 || !state.HasData || !state.Stale || !errors.Is(state.Err, failure) {
		t.Fatalf("failed observation = %+v", state)
	}
	observer.SetEnabled(false)
	client.Invalidate("users")
	if state = observer.Snapshot(); state.Fetching || calls.Load() != 1 || !errors.Is(state.Err, failure) {
		t.Fatalf("disabled failed observation = %+v", state)
	}
}

// TestObserveClose releases subscriptions once and prevents enabling after observer or client closure.
func TestObserveClose(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		client := newInvalidationClient(t)
		var calls atomic.Int32
		observer := client.Observe("users", func(context.Context) (int, error) {
			calls.Add(1)
			return 1, nil
		}, ObserveOptions{})
		<-observer.Updates()
		if closeClient {
			client.Close()
		} else {
			observer.Close()
		}
		observer.Close()
		observer.SetEnabled(true)
		observer.SetEnabled(false)
		if _, open := <-observer.Updates(); open || calls.Load() != 0 {
			t.Fatal("a closed observer restarted work or retained its channel")
		}
		if closeClient {
			closed := client.Observe("users", nil, ObserveOptions{Enabled: true})
			state := <-closed.Updates()
			if !errors.Is(state.Err, ErrClosed) || state.Fetching {
				t.Fatalf("observation after client closure = %+v", state)
			}
			if _, open := <-closed.Updates(); open {
				t.Fatal("closed client accepted a new subscription")
			}
			closed.Close()
		}
	}
}

// TestObserveDisabledRetention protects subscribed data until the disabled observer is released.
func TestObserveDisabledRetention(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	client.Set("users", "cached")
	previous := scheduler.latest(t)
	observer := client.Observe("users", nil, ObserveOptions{})
	t.Cleanup(observer.Close)
	scheduler.now.Add(int64(time.Hour))
	previous.callback()
	if state := observer.Snapshot(); !state.HasData || state.Data != "cached" {
		t.Fatalf("disabled subscription retention = %+v", state)
	}
	observer.Close()
	scheduler.now.Add(int64(time.Minute))
	scheduler.latest(t).callback()
	if state := client.Snapshot("users"); state.HasData {
		t.Fatalf("closed observer retained unused data: %+v", state)
	}
}

// TestObserveCloseDuringLoad leaves background work running after its initiating observer is released.
func TestObserveCloseDuringLoad(t *testing.T) {
	client := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	observer := client.Observe("users", func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 7, nil
		}
	}, ObserveOptions{Enabled: true})
	t.Cleanup(observer.Close)
	<-started
	updates, unsubscribe := client.Subscribe("users")
	t.Cleanup(unsubscribe)
	observer.Close()
	close(release)
	state := awaitState(t, updates, func(s Snapshot[int]) bool { return !s.Fetching })
	if state.Data != 7 || !state.HasData || state.Err != nil {
		t.Fatalf("result after observer closure = %+v", state)
	}
}

// TestObserveClientCancellation stops an observer's automatic load when the owning client closes.
func TestObserveClientCancellation(t *testing.T) {
	client := newInvalidationClient(t)
	started := make(chan struct{})
	finished := make(chan error, 1)
	observer := client.Observe("users", func(ctx context.Context) (int, error) {
		close(started)
		<-ctx.Done()
		finished <- ctx.Err()
		return 0, ctx.Err()
	}, ObserveOptions{Enabled: true})
	t.Cleanup(observer.Close)
	<-started
	client.Close()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("client cancellation = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client closure did not cancel the automatic load")
	}
	if state := observer.Snapshot(); state.Fetching || !errors.Is(state.Err, ErrClosed) {
		t.Fatalf("state after client cancellation = %+v", state)
	}
}
