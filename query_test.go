package query

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// awaitState waits for an observable transition with a deadlock guard independent of cache timing.
func awaitState[V any](t *testing.T, updates <-chan Snapshot[V], matches func(Snapshot[V]) bool) Snapshot[V] {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case state, open := <-updates:
			if !open {
				t.Fatal("query closed before expected state")
			}
			if matches(state) {
				return state
			}
		case <-timer.C:
			t.Fatal("query did not publish expected state")
		}
	}
}

// newInvalidationClient fixes freshness comparisons independently of loader scheduling.
func newInvalidationClient(t *testing.T) *Client {
	t.Helper()
	client := NewClient(Options{StaleTime: time.Hour, Clock: func() time.Time { return time.Unix(0, 0) }})
	t.Cleanup(client.Close)
	return client
}

// TestQueryDisabled reads existing state without loading, including after automatic invalidation.
func TestQueryDisabled(t *testing.T) {
	for _, cache := range []string{"missing", "fresh", "stale"} {
		t.Run(cache, func(t *testing.T) {
			client := newInvalidationClient(t)
			if cache != "missing" {
				Set(client, "users", 7)
			}
			if cache == "stale" {
				client.Invalidate("users")
			}
			var calls atomic.Int32
			observer := Query(client, "users", func(context.Context) (int, error) {
				calls.Add(1)
				return 8, nil
			}, QueryOptions{Enabled: false})
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
			client.InvalidateMany([]any{"users"}, InvalidateOptions{})
			if state = observer.Snapshot(); state.Fetching || calls.Load() != 0 {
				t.Fatalf("disabled batch invalidation = %+v; calls = %d", state, calls.Load())
			}
		})
	}
}

// TestQueryEnableTransitions requests missing or stale data and reuses fresh data when enabling.
func TestQueryEnableTransitions(t *testing.T) {
	client := newInvalidationClient(t)
	var calls atomic.Int32
	observer := Query(client, "users", func(context.Context) (int, error) {
		return int(calls.Add(1)), nil
	}, QueryOptions{Enabled: false})
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
	client.InvalidateMany([]any{"users"}, InvalidateOptions{})
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

// TestQueryFreshData retains its loader for invalidation even when no initial request is needed.
func TestQueryFreshData(t *testing.T) {
	client := newInvalidationClient(t)
	Set(client, "users", 7)
	var calls atomic.Int32
	observer := Query(client, "users", func(context.Context) (int, error) {
		calls.Add(1)
		return 8, nil
	}, QueryOptions{Enabled: true})
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

// TestQuerySharedEnablement isolates observers' permissions while sharing requests and updates.
func TestQuerySharedEnablement(t *testing.T) {
	client := newInvalidationClient(t)
	var disabledCalls, activeCalls atomic.Int32
	disabled := Query(client, "users", func(context.Context) (int, error) {
		disabledCalls.Add(1)
		return 99, nil
	}, QueryOptions{Enabled: false})
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
	first := Query(client, "users", fetch, QueryOptions{Enabled: true})
	t.Cleanup(first.Close)
	<-started
	second := Query(client, "users", fetch, QueryOptions{Enabled: true})
	t.Cleanup(second.Close)
	close(release)
	for _, observer := range []*QueryHandle[int]{disabled, first, second} {
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

// TestQueryManualRequests allows explicit client calls while keeping subsequent automatic loads disabled.
func TestQueryManualRequests(t *testing.T) {
	for _, action := range []string{"fetch", "query", "refetch"} {
		t.Run(action, func(t *testing.T) {
			client := newInvalidationClient(t)
			var calls atomic.Int32
			fetch := func(context.Context) (int, error) { return int(calls.Add(1)), nil }
			observer := Query(client, "users", fetch, QueryOptions{Enabled: false})
			t.Cleanup(observer.Close)
			switch action {
			case "fetch":
				if _, err := Fetch(context.Background(), client, "users", fetch); err != nil {
					t.Fatal(err)
				}
			case "query":
				active := Query(client, "users", fetch)
				active.Close()
			case "refetch":
				if _, err := observer.Refetch(context.Background()); err != nil {
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

// TestQueryDisableDuringLoad preserves work already started and defers further automatic requests.
func TestQueryDisableDuringLoad(t *testing.T) {
	client := newInvalidationClient(t)
	var calls atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	observer := Query(client, "users", func(ctx context.Context) (int, error) {
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
	}, QueryOptions{Enabled: true})
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

// TestQueryFailure retains stale cache and publishes the error without loading while disabled.
func TestQueryFailure(t *testing.T) {
	client := newInvalidationClient(t)
	Set(client, "users", 7)
	client.Invalidate("users")
	failure := errors.New("offline")
	var calls atomic.Int32
	observer := Query(client, "users", func(context.Context) (int, error) {
		calls.Add(1)
		return 0, failure
	}, QueryOptions{Enabled: true})
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

// TestQueryClose releases subscriptions once and prevents enabling after observer or client closure.
func TestQueryClose(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		client := newInvalidationClient(t)
		var calls atomic.Int32
		observer := Query(client, "users", func(context.Context) (int, error) {
			calls.Add(1)
			return 1, nil
		}, QueryOptions{Enabled: false})
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
			closed := Query[int](client, "users", nil, QueryOptions{Enabled: true})
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

// TestQueryDisabledRetention protects subscribed data until the disabled observer is released.
func TestQueryDisabledRetention(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	Set(client, "users", "cached")
	previous := scheduler.latest(t)
	observer := Query(
		client,
		"users",
		func(context.Context) (string, error) { return "unused", nil },
		QueryOptions{Enabled: false},
	)
	t.Cleanup(observer.Close)
	scheduler.now.Add(int64(time.Hour))
	previous.callback()
	if state := observer.Snapshot(); !state.HasData || state.Data != "cached" {
		t.Fatalf("disabled subscription retention = %+v", state)
	}
	observer.Close()
	scheduler.now.Add(int64(time.Minute))
	scheduler.latest(t).callback()
	if state := Get[string](client, "users"); state.HasData {
		t.Fatalf("closed observer retained unused data: %+v", state)
	}
}

// TestQueryCloseDuringLoad leaves background work running after its initiating observer is released.
func TestQueryCloseDuringLoad(t *testing.T) {
	client := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	observer := Query(client, "users", func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 7, nil
		}
	}, QueryOptions{Enabled: true})
	t.Cleanup(observer.Close)
	<-started
	passive := Query(
		client,
		"users",
		func(context.Context) (int, error) { return 0, nil },
		QueryOptions{Enabled: false},
	)
	updates, unsubscribe := passive.Updates(), passive.Close
	t.Cleanup(unsubscribe)
	observer.Close()
	close(release)
	state := awaitState(t, updates, func(s Snapshot[int]) bool { return !s.Fetching })
	if state.Data != 7 || !state.HasData || state.Err != nil {
		t.Fatalf("result after observer closure = %+v", state)
	}
}

// TestQueryClientCancellation stops an observer's automatic load when the owning client closes.
func TestQueryClientCancellation(t *testing.T) {
	client := newInvalidationClient(t)
	started := make(chan struct{})
	finished := make(chan error, 1)
	observer := Query(client, "users", func(ctx context.Context) (int, error) {
		close(started)
		<-ctx.Done()
		finished <- ctx.Err()
		return 0, ctx.Err()
	}, QueryOptions{Enabled: true})
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

// testUser represents detail and list values whose static types must remain distinct.
type testUser struct {
	// Name identifies the version returned by a test loader.
	Name string
}

// TestHeterogeneousQueries keeps detail, list, and configuration results in one shared client.
func TestHeterogeneousQueries(t *testing.T) {
	c := newInvalidationClient(t)
	var detailCalls, listCalls, settingsCalls atomic.Int32
	detail := Query(c, "user:42", func(context.Context) (testUser, error) {
		detailCalls.Add(1)
		return testUser{Name: "Alice"}, nil
	})
	list := Query(c, "users", func(context.Context) ([]testUser, error) {
		listCalls.Add(1)
		return []testUser{{Name: "Alice"}}, nil
	})
	settings := Query(c, "settings", func(context.Context) (map[string]bool, error) {
		settingsCalls.Add(1)
		return map[string]bool{"dark": true}, nil
	})
	t.Cleanup(detail.Close)
	t.Cleanup(list.Close)
	t.Cleanup(settings.Close)
	awaitState(t, detail.Updates(), func(s Snapshot[testUser]) bool { return s.HasData && !s.Fetching })
	awaitState(t, list.Updates(), func(s Snapshot[[]testUser]) bool { return s.HasData && !s.Fetching })
	awaitState(t, settings.Updates(), func(s Snapshot[map[string]bool]) bool { return s.HasData && !s.Fetching })
	if detail.Snapshot().Data.Name != "Alice" || list.Snapshot().Data[0].Name != "Alice" ||
		!settings.Snapshot().Data["dark"] {
		t.Fatal("heterogeneous cache lost typed values")
	}
	if err := c.InvalidateMany([]any{"user:42", "users", "users"}, InvalidateOptions{}); err != nil {
		t.Fatal(err)
	}
	awaitState(t, detail.Updates(), func(s Snapshot[testUser]) bool { return detailCalls.Load() == 2 && !s.Fetching })
	awaitState(t, list.Updates(), func(s Snapshot[[]testUser]) bool { return listCalls.Load() == 2 && !s.Fetching })
	if detail.Snapshot().Stale || list.Snapshot().Stale || settingsCalls.Load() != 1 {
		t.Fatal("batch invalidation failed to refresh only related types")
	}
}

// TestTypeMismatch rejects incompatible reads, writes, and subscriptions without corrupting existing data.
func TestTypeMismatch(t *testing.T) {
	c := newInvalidationClient(t)
	if err := Set(c, "user", testUser{Name: "Alice"}); err != nil {
		t.Fatal(err)
	}
	if err := Set(c, "user", []testUser{}); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("set mismatch = %v", err)
	}
	if state := Get[[]testUser](c, "user"); state.HasData || !errors.Is(state.Err, ErrTypeMismatch) {
		t.Fatalf("get mismatch = %+v", state)
	}
	var calls atomic.Int32
	wrong := Query(c, "user", func(context.Context) (int, error) { calls.Add(1); return 0, nil })
	if state := <-wrong.Updates(); !errors.Is(state.Err, ErrTypeMismatch) {
		t.Fatalf("query mismatch = %+v", state)
	}
	if _, open := <-wrong.Updates(); open {
		t.Fatal("failed query retained a subscription")
	}
	if _, err := wrong.Refetch(context.Background()); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("refetch = %v", err)
	}
	if _, err := Fetch(context.Background(), c, "user", func(context.Context) (int, error) {
		calls.Add(1)
		return 0, nil
	}); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("fetch mismatch = %v", err)
	}
	wrong.Close()
	if Get[testUser](c, "user").Data.Name != "Alice" || calls.Load() != 0 {
		t.Fatal("rejected operations changed existing query")
	}
}

// TestPendingTypeMismatch checks the binding before joining or canceling an in-flight request.
func TestPendingTypeMismatch(t *testing.T) {
	c := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	good := Query(c, "user", func(ctx context.Context) (testUser, error) {
		close(started)
		select {
		case <-ctx.Done():
			return testUser{}, ctx.Err()
		case <-release:
			return testUser{Name: "Alice"}, nil
		}
	})
	t.Cleanup(good.Close)
	<-started
	if err := Set(c, "user", 42); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("pending set = %v", err)
	}
	if _, err := Fetch(context.Background(), c, "user", func(context.Context) (int, error) {
		return 42, nil
	}); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("pending fetch = %v", err)
	}
	close(release)
	state := awaitState(t, good.Updates(), func(s Snapshot[testUser]) bool { return !s.Fetching })
	if !state.HasData || state.Data.Name != "Alice" || state.Err != nil {
		t.Fatalf("pending result = %+v", state)
	}
}

// TestLiveHandleTypeBinding retains the type contract across Remove and Clear until the handle leaves.
func TestLiveHandleTypeBinding(t *testing.T) {
	for _, action := range []string{"remove", "clear"} {
		t.Run(action, func(t *testing.T) {
			c := newInvalidationClient(t)
			handle := Query(c, "user", func(context.Context) (testUser, error) { return testUser{Name: "Alice"}, nil })
			awaitState(t, handle.Updates(), func(s Snapshot[testUser]) bool { return s.HasData && !s.Fetching })
			if action == "remove" {
				c.Remove("user")
			} else {
				c.Clear()
			}
			if state := handle.Snapshot(); state.HasData || state.Status != Idle {
				t.Fatalf("cleared state = %+v", state)
			}
			if err := Set(c, "user", 42); !errors.Is(err, ErrTypeMismatch) {
				t.Fatalf("live binding = %v", err)
			}
			if value, err := handle.Refetch(context.Background()); err != nil || value.Name != "Alice" {
				t.Fatalf("refetch after clear = %+v, %v", value, err)
			}
			handle.Close()
			c.Remove("user")
			if err := Set(c, "user", 42); err != nil {
				t.Fatal(err)
			}
			if Get[int](c, "user").Data != 42 {
				t.Fatal("unused removed key could not be rebound")
			}
			if _, err := handle.Refetch(context.Background()); !errors.Is(err, ErrQueryClosed) {
				t.Fatalf("closed handle refetch = %v", err)
			}
		})
	}
}

// TestNilAndInterfaceResults preserves typed nil, valid zero values, and statically declared interfaces.
func TestNilAndInterfaceResults(t *testing.T) {
	c := newInvalidationClient(t)
	pointer, err := Fetch(
		context.Background(),
		c,
		"pointer",
		func(context.Context) (*testUser, error) { return nil, nil },
	)
	if pointer != nil || err != nil || !Get[*testUser](c, "pointer").HasData {
		t.Fatalf("nil pointer = %v, %v", pointer, err)
	}
	value, err := Fetch[any](
		context.Background(),
		c,
		"interface",
		func(context.Context) (any, error) { return nil, nil },
	)
	if value != nil || err != nil || !Get[any](c, "interface").HasData {
		t.Fatalf("nil interface = %v, %v", value, err)
	}
	if err := Set[any](c, "interface", "text"); err != nil {
		t.Fatal(err)
	}
	if Get[any](c, "interface").Data != "text" {
		t.Fatal("interface value was not retained")
	}
	if err := Set(c, "interface", "text"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("static interface mismatch = %v", err)
	}
	Set(c, "zero", 0)
	if !Get[int](c, "zero").HasData {
		t.Fatal("zero value treated as missing")
	}
	if state := Get[int](c, "unused"); state.Status != Idle {
		t.Fatalf("unused read = %+v", state)
	}
	if err := Set(c, "unused", "different"); err != nil {
		t.Fatal("read bound an unused key")
	}
}

// TestInvalidKeys rejects unhashable keys and validates batches before changing any selected entry.
func TestInvalidKeys(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "safe", 1)
	keys := []any{nil, []string{"users"}, map[string]int{}, struct{ Value any }{Value: []int{1}}}
	for _, key := range keys {
		if err := Set(c, key, 1); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid set = %v", err)
		}
		if state := Get[int](c, key); !errors.Is(state.Err, ErrInvalidKey) {
			t.Fatalf("invalid get = %+v", state)
		}
		if _, err := Fetch(
			context.Background(),
			c,
			key,
			func(context.Context) (int, error) { return 1, nil },
		); !errors.Is(
			err,
			ErrInvalidKey,
		) {
			t.Fatalf("invalid fetch = %v", err)
		}
		handle := Query(c, key, func(context.Context) (int, error) { return 1, nil })
		if !errors.Is(handle.Snapshot().Err, ErrInvalidKey) {
			t.Fatal("invalid query accepted")
		}
		if err := c.Cancel(key); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid cancel = %v", err)
		}
		if err := c.Remove(key); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid remove = %v", err)
		}
		if err := c.Invalidate(key); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid invalidate = %v", err)
		}
		if err := c.InvalidateMany([]any{"safe", key}, InvalidateOptions{}); !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid batch = %v", err)
		}
		if Get[int](c, "safe").Stale {
			t.Fatal("invalid batch partially invalidated data")
		}
	}
}

// TestQueryLifecycle closes channels once, retains the latest state, and rejects requests after release.
func TestQueryLifecycle(t *testing.T) {
	c := newInvalidationClient(t)
	handle := Query(c, "user", func(context.Context) (int, error) { return 0, nil }, QueryOptions{Enabled: false})
	<-handle.Updates()
	Set(c, "user", 1)
	Set(c, "user", 2)
	if state := <-handle.Updates(); state.Data != 2 {
		t.Fatalf("latest state = %+v", state)
	}
	handle.Close()
	handle.Close()
	if err := handle.SetEnabled(true); !errors.Is(err, ErrQueryClosed) {
		t.Fatalf("closed enable = %v", err)
	}
	if _, err := handle.Refetch(context.Background()); !errors.Is(err, ErrQueryClosed) {
		t.Fatalf("closed refetch = %v", err)
	}
	if _, open := <-handle.Updates(); open {
		t.Fatal("released handle channel remains open")
	}
	c.Close()
	c.Close()
	if _, err := Fetch[int](context.Background(), c, "user", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed fetch = %v", err)
	}
	if err := Set(c, "user", 3); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed set = %v", err)
	}
}

// TestNoFetcher reports missing loaders without creating a cache type binding.
func TestNoFetcher(t *testing.T) {
	c := newInvalidationClient(t)
	if _, err := Fetch[int](context.Background(), c, "user", nil); !errors.Is(err, ErrNoFetcher) {
		t.Fatalf("nil fetch = %v", err)
	}
	handle := Query[int](c, "user", nil, QueryOptions{Enabled: false})
	if !errors.Is(handle.Snapshot().Err, ErrNoFetcher) {
		t.Fatal("nil query loader accepted")
	}
	if err := Set(c, "user", "text"); err != nil {
		t.Fatal("rejected loader left a binding")
	}
}

// TestFailedRefreshRetainsData publishes an error without discarding a previously usable result.
func TestFailedRefreshRetainsData(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "user", "cached")
	failure := errors.New("offline")
	handle := Query(c, "user", func(context.Context) (string, error) { return "", failure })
	t.Cleanup(handle.Close)
	if _, err := handle.Refetch(context.Background()); !errors.Is(err, failure) {
		t.Fatalf("refresh = %v", err)
	}
	state := handle.Snapshot()
	if !state.HasData || state.Data != "cached" || !state.Stale || state.Fetching ||
		state.Status != Error || !errors.Is(state.Err, failure) {
		t.Fatalf("retained failure = %+v", state)
	}
}

// TestSetAndRemovePreventLateWrites prevents a detached loader from installing an obsolete result.
func TestSetAndRemovePreventLateWrites(t *testing.T) {
	for _, action := range []string{"set", "remove"} {
		t.Run(action, func(t *testing.T) {
			c := newInvalidationClient(t)
			started, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				_, err := Fetch(context.Background(), c, "user", func(context.Context) (string, error) {
					close(started)
					<-release
					return "obsolete", nil
				})
				finished <- err
			}()
			<-started
			if action == "set" {
				Set(c, "user", "local")
			} else {
				c.Remove("user")
				Set(c, "user", 42)
			}
			close(release)
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatalf("detached result = %v", err)
			}
			if action == "set" && Get[string](c, "user").Data != "local" {
				t.Fatal("set overwritten")
			}
			if action == "remove" && Get[int](c, "user").Data != 42 {
				t.Fatal("rebound key overwritten")
			}
		})
	}
}

// TestInvalidateDeferred preserves mixed-type data without starting loads until the next explicit use.
func TestInvalidateDeferred(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "user", "cached")
	Set(c, "users", []string{"cached"})
	var calls atomic.Int32
	handle := Query(c, "user", func(context.Context) (string, error) { calls.Add(1); return "new", nil })
	t.Cleanup(handle.Close)
	c.InvalidateMany([]any{"user", "users"}, InvalidateOptions{Refetch: RefetchNone})
	if state := handle.Snapshot(); !state.Stale || state.Data != "cached" || state.Fetching || calls.Load() != 0 {
		t.Fatalf("deferred = %+v", state)
	}
	if !Get[[]string](c, "users").Stale {
		t.Fatal("second type was not invalidated")
	}
	if value, err := Fetch(context.Background(), c, "user", func(context.Context) (string, error) {
		calls.Add(1)
		return "new", nil
	}); value != "new" || err != nil || calls.Load() != 1 {
		t.Fatalf("next fetch = %q, %v", value, err)
	}
	c.Invalidate("user", InvalidateOptions{Refetch: RefetchNone})
	next := Query(c, "user", func(context.Context) (string, error) { return "newer", nil })
	t.Cleanup(next.Close)
	awaitState(t, next.Updates(), func(s Snapshot[string]) bool { return s.Data == "newer" && !s.Fetching })
}

// TestInvalidateDuringLoad keeps a response stale when invalidation occurred after its load began.
func TestInvalidateDuringLoad(t *testing.T) {
	c := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	handle := Query(c, "user", func(ctx context.Context) (string, error) {
		calls.Add(1)
		close(started)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-release:
			return "loaded", nil
		}
	})
	t.Cleanup(handle.Close)
	<-started
	c.InvalidateMany([]any{"user", "user"}, InvalidateOptions{})
	close(release)
	state := awaitState(t, handle.Updates(), func(s Snapshot[string]) bool { return s.HasData && !s.Fetching })
	if !state.Stale || state.Data != "loaded" || calls.Load() != 1 {
		t.Fatalf("in-flight invalidation = %+v", state)
	}
}

// testScopedKey selects typed resource and tenant fields without string encoding.
type testScopedKey struct {
	// Resource identifies the kind of query result.
	Resource string
	// Tenant isolates otherwise identical resource keys.
	Tenant int
}

// TestInvalidateWhere supports structured keys, reentrant matching, and removal during a predicate.
func TestInvalidateWhere(t *testing.T) {
	c := newInvalidationClient(t)
	key := testScopedKey{Resource: "users", Tenant: 42}
	Set(c, key, []string{"Alice"})
	Set(c, "users:42", "Alice")
	Set(c, "unrelated", 7)
	Set(c, "removed", 1)
	seen := make(map[any]int)
	c.InvalidateWhere(func(candidate any) bool {
		seen[candidate]++
		Get[int](c, "unrelated")
		Set(c, "added", true)
		c.Remove("removed")
		return candidate != "unrelated"
	}, InvalidateOptions{Refetch: RefetchNone})
	if !Get[[]string](c, key).Stale || !Get[string](c, "users:42").Stale || Get[int](c, "unrelated").Stale {
		t.Fatal("predicate selected wrong typed keys")
	}
	if seen["added"] != 0 || Get[bool](c, "added").Stale {
		t.Fatal("predicate included newly added key")
	}
	foundRemoved := false
	c.InvalidateWhere(
		func(candidate any) bool { foundRemoved = foundRemoved || candidate == "removed"; return false },
		InvalidateOptions{},
	)
	if foundRemoved {
		t.Fatal("predicate recreated removed cache")
	}
	for _, count := range seen {
		if count != 1 {
			t.Fatal("predicate evaluated key twice")
		}
	}
	c.InvalidateWhere(nil, InvalidateOptions{})
	c.Close()
	c.InvalidateWhere(func(any) bool { t.Fatal("closed client evaluated predicate"); return true }, InvalidateOptions{})
}

// TestQueryBackgroundRefresh immediately exposes stale data while sharing the replacement load.
func TestQueryBackgroundRefresh(t *testing.T) {
	now := time.Unix(0, 0)
	c := NewClient(Options{StaleTime: time.Hour, Clock: func() time.Time { return now }})
	t.Cleanup(c.Close)
	Set(c, "user", testUser{Name: "old"})
	started, release := make(chan struct{}), make(chan struct{})
	fetch := func(ctx context.Context) (testUser, error) {
		close(started)
		select {
		case <-ctx.Done():
			return testUser{}, ctx.Err()
		case <-release:
			return testUser{Name: "new"}, nil
		}
	}
	first := Query(c, "user", fetch)
	t.Cleanup(first.Close)
	if first.Snapshot().Fetching {
		t.Fatal("fresh cached query started a load")
	}
	now = now.Add(time.Hour)
	second := Query(c, "user", fetch)
	t.Cleanup(second.Close)
	<-started
	state := second.Snapshot()
	if !state.HasData || !state.Stale || !state.Fetching || state.Status != Success || state.Data.Name != "old" {
		t.Fatalf("background refresh = %+v", state)
	}
	close(release)
	for _, h := range []*QueryHandle[testUser]{first, second} {
		result := awaitState(
			t,
			h.Updates(),
			func(s Snapshot[testUser]) bool { return s.Data.Name == "new" && !s.Fetching },
		)
		if result.Stale || result.Err != nil {
			t.Fatalf("refreshed state = %+v", result)
		}
	}
}

// TestPrefetchAndCancel warms the shared cache and cancels a load without starting a replacement.
func TestPrefetchAndCancel(t *testing.T) {
	c := newInvalidationClient(t)
	if err := Prefetch(
		context.Background(),
		c,
		"warm",
		func(context.Context) (string, error) { return "ready", nil },
	); err != nil {
		t.Fatal(err)
	}
	if value, err := Fetch[string](context.Background(), c, "warm", nil); value != "ready" || err != nil {
		t.Fatalf("prefetched value = %q, %v", value, err)
	}
	started, finished := make(chan struct{}), make(chan error, 1)
	go func() {
		_, err := Fetch(context.Background(), c, "active", func(ctx context.Context) (int, error) {
			close(started)
			<-ctx.Done()
			return 0, ctx.Err()
		})
		finished <- err
	}()
	<-started
	if err := c.Cancel("active"); err != nil {
		t.Fatal(err)
	}
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
	if state := Get[int](c, "active"); state.Fetching || state.HasData {
		t.Fatalf("cancel state = %+v", state)
	}
}
