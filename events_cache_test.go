package cacheq

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestEventsLocalWrite records successful updates without emitting events for rejected types or absent removals.
func TestEventsLocalWrite(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	if err := Set(c, "a", 7); err != nil {
		t.Fatal(err)
	}
	if event := receiveTestEvent(
		t,
		s.Events(),
	); event.Kind != EventLocalWrite || event.Reason != ReasonSet ||
		event.LoadID != 0 {
		t.Fatalf("local write = %+v", event)
	}
	if err := Set(c, "a", "wrong type"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("rejected write = %v", err)
	}
	if err := c.Remove("absent"); err != nil {
		t.Fatal(err)
	}
	if state := Get[int](c, "a"); state.Data != 7 || state.Err != nil {
		t.Fatalf("retained local value = %+v", state)
	}
	select {
	case event := <-s.Events():
		t.Fatalf("rejected or passive operation produced %+v", event)
	default:
	}
}

// TestEventsBatchInvalidation emits one record per selected mixed-type key and avoids loading unobserved entries.
func TestEventsBatchInvalidation(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	Set(c, "a", 7)
	Set(c, "b", "list")
	Set(c, "c", true)
	s := subscribeTestEvents(t, c, EventOptions{})
	if err := c.Invalidate([]any{"a", "b", "a"}, InvalidateOptions{Refetch: RefetchNone}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		event := receiveTestEvent(t, s.Events())
		if event.Key != key || event.Kind != EventInvalidated || event.Reason != ReasonInvalidated {
			t.Fatalf("batch invalidation = %+v", event)
		}
	}
	if err := c.Invalidate(func(key any) bool { return key != "b" }); err != nil {
		t.Fatal(err)
	}
	seen := make(map[any]bool)
	for range 2 {
		event := receiveTestEvent(t, s.Events())
		if event.Kind != EventInvalidated || event.Trigger != TriggerInvalidate || seen[event.Key] {
			t.Fatalf("predicate invalidation = %+v", event)
		}
		seen[event.Key] = true
	}
	if !seen["a"] || !seen["c"] || c.Stats().Loads != 0 {
		t.Fatalf("selected keys = %v; stats = %+v", seen, c.Stats())
	}
}

// TestEventsInflightInvalidation correlates an invalidated operation while keeping its applied result stale.
func TestEventsInflightInvalidation(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	h := Query(c, "a", func(ctx context.Context) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 7, nil
		}
	})
	t.Cleanup(h.Close)
	start := receiveTestEvent(t, s.Events())
	if err := c.Invalidate("a"); err != nil {
		t.Fatal(err)
	}
	invalidated := receiveTestEvent(t, s.Events())
	if invalidated.Kind != EventInvalidated || invalidated.LoadID != start.LoadID {
		t.Fatalf("in-flight invalidation = %+v", invalidated)
	}
	unblock()
	end := receiveTestEvent(t, s.Events())
	if end.Kind != EventLoadFinished || !end.Applied || !h.Snapshot().Stale {
		t.Fatalf("invalidated completion = %+v; query = %+v", end, h.Snapshot())
	}
}

// TestEventsRemovalCauses distinguishes capacity, idle collection, explicit deletion, and bulk clearing.
func TestEventsRemovalCauses(t *testing.T) {
	for _, reason := range []EventReason{ReasonCapacity, ReasonInactive, ReasonRemove, ReasonClear} {
		t.Run(reason.String(), func(t *testing.T) {
			now := time.Unix(0, 0)
			c := NewClient(Options{StaleTime: time.Hour, MaxEntries: 1,
				GCTime: time.Hour, Clock: func() time.Time { return now }})
			t.Cleanup(c.Close)
			var collect func()
			c.gcAfterFunc = func(_ time.Duration, run func()) func() {
				collect = run
				return func() {}
			}
			if err := Set(c, "a", 7); err != nil {
				t.Fatal(err)
			}
			s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
			switch reason {
			case ReasonCapacity:
				Set(c, "b", 8)
			case ReasonInactive:
				now = now.Add(time.Hour)
				collect()
				collect()
			case ReasonRemove:
				c.Remove("a")
			case ReasonClear:
				c.Clear()
			}
			removed := receiveTestEvent(t, s.Events())
			if removed.Kind != EventCacheRemoved || removed.Reason != reason || Get[int](c, "a").HasData {
				t.Fatalf("removal = %+v", removed)
			}
			_, err := Fetch(context.Background(), c, "a", func(context.Context) (int, error) { return 9, nil })
			if err != nil {
				t.Fatal(err)
			}
			// Removal history lives in the event stream, not in an unbounded map of deleted keys.
			if start := receiveTestEvent(
				t,
				s.Events(),
			); start.Kind != EventLoadStarted ||
				start.Reason != ReasonMissing {
				t.Fatalf("load after removal = %+v", start)
			}
		})
	}
}

// TestEventsMaxAge observes lazy data expiration once and preserves its cause for a retained type binding.
func TestEventsMaxAge(t *testing.T) {
	now := time.Unix(0, 0)
	c := NewClient(Options{StaleTime: time.Hour, MaxAge: time.Minute, Clock: func() time.Time { return now }})
	t.Cleanup(c.Close)
	Set(c, "a", 7)
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	now = now.Add(time.Minute)
	if state := Get[int](c, "a"); state.HasData {
		t.Fatalf("over-age state = %+v", state)
	}
	removed := receiveTestEvent(t, s.Events())
	if removed.Kind != EventCacheRemoved || removed.Reason != ReasonMaxAge {
		t.Fatalf("age removal = %+v", removed)
	}
	Get[int](c, "a")
	_, err := Fetch(context.Background(), c, "a", func(context.Context) (int, error) { return 9, nil })
	if err != nil {
		t.Fatal(err)
	}
	start := receiveTestEvent(t, s.Events())
	end := receiveTestEvent(t, s.Events())
	if start.Reason != ReasonMaxAge || end.Kind != EventLoadFinished || c.Stats().AgeExpirations != 1 {
		t.Fatalf("age reload start = %+v; completion = %+v", start, end)
	}
}

// TestEventsCloseWithActiveLoad closes diagnostics immediately even when a canceled loader returns late.
func TestEventsCloseWithActiveLoad(t *testing.T) {
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	s := subscribeTestEvents(t, c, EventOptions{Key: "a"})
	entered := make(chan error, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	h := Query(c, "a", func(context.Context) (int, error) {
		entered <- nil
		<-release
		return 7, nil
	})
	t.Cleanup(h.Close)
	receiveTestEvent(t, s.Events())
	if err := waitEventCall(t, entered); err != nil {
		t.Fatal(err)
	}
	c.Close()
	if terminal := receiveTestEvent(t, s.Events()); terminal.Kind != EventClientClosed {
		t.Fatalf("active close = %+v", terminal)
	}
	unblock()
	awaitStats(t, c, func(state Stats) bool { return state.LoadCancellations == 1 })
	if _, open := <-s.Events(); open {
		t.Fatal("late completion published into a closed stream")
	}
}
