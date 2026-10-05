package query

import (
	"context"
	"errors"
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
