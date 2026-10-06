package query

import (
	"context"
	"errors"
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
