package query

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

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
