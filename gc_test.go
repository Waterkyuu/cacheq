package query

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualGCTimer lets tests deliver callbacks, including callbacks already racing with Stop.
type manualGCTimer struct {
	// delay records the requested timer duration independently of wall-clock time.
	delay time.Duration
	// callback runs the scheduled cleanup when the test delivers it.
	callback func()
	// stopped reports whether the client released this timer.
	stopped atomic.Bool
}

// manualGCScheduler supplies a logical clock and explicitly delivered cleanup timers.
type manualGCScheduler struct {
	// mu protects timers created by background query completions.
	mu sync.Mutex
	// timers records scheduled callbacks in creation order.
	timers []*manualGCTimer
	// now stores the logical clock as Unix nanoseconds for concurrent readers.
	now atomic.Int64
}

// schedule records a callback instead of waiting for a real timer to expire.
func (s *manualGCScheduler) schedule(delay time.Duration, callback func()) func() {
	timer := &manualGCTimer{delay: delay, callback: callback}
	s.mu.Lock()
	s.timers = append(s.timers, timer)
	s.mu.Unlock()
	return func() { timer.stopped.Store(true) }
}

// latest returns the last scheduled timer after background loads have synchronized with the test.
func (s *manualGCScheduler) latest(t *testing.T) *manualGCTimer {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.timers) == 0 {
		t.Fatal("no cleanup was scheduled")
	}
	return s.timers[len(s.timers)-1]
}

// count reports how many timers have been scheduled without exposing concurrent slice accesses.
func (s *manualGCScheduler) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.timers)
}

// newGCClient creates a client whose clock and cleanup callbacks are controlled by the test.
func newGCClient(t *testing.T, retention time.Duration) (*Client[string, string], *manualGCScheduler) {
	t.Helper()
	scheduler := &manualGCScheduler{timers: make([]*manualGCTimer, 0)}
	scheduler.now.Store(time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC).UnixNano())
	client := NewClient[string, string](Options{
		StaleTime: time.Hour,
		GCTime:    retention,
		Clock:     func() time.Time { return time.Unix(0, scheduler.now.Load()) },
	})
	client.gcAfterFunc = scheduler.schedule
	t.Cleanup(client.Close)
	return client, scheduler
}

// TestGCRetentionBoundary distinguishes cache retention from data freshness at the exact deadline.
func TestGCRetentionBoundary(t *testing.T) {
	for _, elapsed := range []time.Duration{time.Minute - time.Nanosecond, time.Minute, time.Minute + time.Nanosecond} {
		t.Run(elapsed.String(), func(t *testing.T) {
			client, scheduler := newGCClient(t, time.Minute)
			client.Set("a", "cached")
			timer := scheduler.latest(t)
			scheduler.now.Add(int64(elapsed))
			timer.callback()
			if elapsed < time.Minute {
				if delay := scheduler.latest(t).delay; delay != time.Nanosecond {
					t.Fatalf("remaining retention = %v", delay)
				}
			}
			state := client.Snapshot("a")
			if want := elapsed < time.Minute; state.HasData != want {
				t.Fatalf("state after %v = %+v; want data = %v", elapsed, state, want)
			}
			if state.HasData && state.Stale {
				t.Fatal("retention changed the freshness window")
			}
		})
	}
}

// TestGCDisabled keeps the previous unlimited-retention behavior unless cleanup is explicitly enabled.
func TestGCDisabled(t *testing.T) {
	for _, retention := range []time.Duration{0, -time.Minute} {
		t.Run(retention.String(), func(t *testing.T) {
			client, scheduler := newGCClient(t, retention)
			client.Set("a", "cached")
			scheduler.now.Add(int64(24 * time.Hour))
			if state := client.Snapshot("a"); !state.HasData || !state.Stale {
				t.Fatalf("disabled cleanup = %+v", state)
			}
			if count := scheduler.count(); count != 0 {
				t.Fatalf("disabled cleanup scheduled %d timers", count)
			}
		})
	}
}

// TestGCUseRestartsRetention prevents previously scheduled callbacks from deleting reused data.
func TestGCUseRestartsRetention(t *testing.T) {
	for _, action := range []string{"fetch", "query", "snapshot", "set", "invalidate"} {
		t.Run(action, func(t *testing.T) {
			client, scheduler := newGCClient(t, time.Minute)
			client.Set("a", "cached")
			previous := scheduler.latest(t)
			scheduler.now.Add(int64(30 * time.Second))
			switch action {
			case "fetch":
				if _, err := client.Fetch(context.Background(), "a", nil); err != nil {
					t.Fatal(err)
				}
			case "query":
				client.Query("a", nil)
			case "snapshot":
				client.Snapshot("a")
			case "set":
				client.Set("a", "updated")
			case "invalidate":
				client.Invalidate("a")
			}
			current := scheduler.latest(t)
			if !previous.stopped.Load() || current == previous {
				t.Fatal("reuse did not replace the cleanup timer")
			}
			scheduler.now.Add(int64(30 * time.Second))
			previous.callback()
			if state := client.Snapshot("a"); !state.HasData {
				t.Fatal("a stopped callback deleted reused data")
			}
			scheduler.now.Add(int64(time.Minute))
			scheduler.latest(t).callback()
			if state := client.Snapshot("a"); state.HasData {
				t.Fatalf("unused data was retained: %+v", state)
			}
		})
	}
}

// TestGCSubscriptions retains data until the final observer leaves and then starts a full retention window.
func TestGCSubscriptions(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	client.Set("a", "cached")
	previous := scheduler.latest(t)
	first, cancelFirst := client.Subscribe("a")
	defer cancelFirst()
	second, cancelSecond := client.Subscribe("a")
	defer cancelSecond()
	<-first
	<-second
	scheduler.now.Add(int64(time.Hour))
	previous.callback()
	if state := client.Snapshot("a"); !state.HasData {
		t.Fatal("cleanup deleted subscribed data")
	}
	if !previous.stopped.Load() || scheduler.count() != 1 {
		t.Fatal("subscription did not suspend cleanup")
	}
	cancelFirst()
	if scheduler.count() != 1 {
		t.Fatal("cleanup resumed while another subscription remained")
	}
	cancelSecond()
	current := scheduler.latest(t)
	if current == previous || current.delay != time.Minute {
		t.Fatal("last unsubscribe did not start a new retention window")
	}
	scheduler.now.Add(int64(time.Minute))
	current.callback()
	if state := client.Snapshot("a"); state.HasData {
		t.Fatalf("unsubscribed data was retained: %+v", state)
	}
}

// TestGCLoads retains stale data during a refresh and starts cleanup after success, failure, or cancellation.
func TestGCLoads(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			client, scheduler := newGCClient(t, time.Minute)
			client.Set("a", "old")
			previous := scheduler.latest(t)
			scheduler.now.Add(int64(time.Hour))
			started, release := make(chan struct{}), make(chan struct{})
			failure := errors.New("offline")
			fetch := func(ctx context.Context) (string, error) {
				close(started)
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-release:
					if outcome == "failure" {
						return "", failure
					}
					return "new", nil
				}
			}
			finished := make(chan error, 1)
			go func() {
				_, err := client.Fetch(context.Background(), "a", fetch)
				finished <- err
			}()
			<-started
			previous.callback()
			if state := client.Snapshot("a"); !state.HasData || !state.Fetching || state.Data != "old" {
				t.Fatalf("loading data = %+v", state)
			}
			if !previous.stopped.Load() || scheduler.count() != 1 {
				t.Fatal("loading did not suspend cleanup")
			}
			scheduler.now.Add(int64(time.Hour))
			if outcome == "cancel" {
				client.Cancel("a")
			} else {
				close(release)
			}
			err := <-finished
			switch outcome {
			case "success":
				if err != nil {
					t.Fatal(err)
				}
			case "failure":
				if !errors.Is(err, failure) {
					t.Fatalf("refresh failure = %v", err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("refresh cancellation = %v", err)
				}
			}
			current := scheduler.latest(t)
			if current == previous || current.delay != time.Minute {
				t.Fatal("load completion did not start a full retention window")
			}
			scheduler.now.Add(int64(time.Minute))
			current.callback()
			if state := client.Snapshot("a"); state.HasData || state.Err != nil || state.Status != Idle {
				t.Fatalf("completed load remained cached: %+v", state)
			}
		})
	}
}

// TestGCInitialLoad schedules cleanup for newly loaded data even when no previous entry exists.
func TestGCInitialLoad(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	if _, err := client.Fetch(context.Background(), "a", func(context.Context) (string, error) {
		return "loaded", nil
	}); err != nil {
		t.Fatal(err)
	}
	scheduler.now.Add(int64(time.Minute))
	scheduler.latest(t).callback()
	if state := client.Snapshot("a"); state.HasData {
		t.Fatalf("initial load remained cached: %+v", state)
	}
	value, err := client.Fetch(context.Background(), "a", func(context.Context) (string, error) {
		return "reloaded", nil
	})
	if value != "reloaded" || err != nil {
		t.Fatalf("fetch after cleanup = %q, %v", value, err)
	}
}

// TestGCInitialFailure deletes cached error state even when the key never acquired usable data.
func TestGCInitialFailure(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	failure := errors.New("offline")
	_, err := client.Fetch(context.Background(), "a", func(context.Context) (string, error) {
		return "", failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("initial failure = %v", err)
	}
	scheduler.now.Add(int64(time.Minute))
	scheduler.latest(t).callback()
	if state := client.Snapshot("a"); state.Err != nil || state.Status != Idle {
		t.Fatalf("initial failure remained cached: %+v", state)
	}
}

// TestGCKeyIsolation expires one key without deleting another key's more recently installed data.
func TestGCKeyIsolation(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	client.Set("a", "first")
	first := scheduler.latest(t)
	scheduler.now.Add(int64(30 * time.Second))
	client.Set("b", "second")
	scheduler.now.Add(int64(30 * time.Second))
	first.callback()
	if state := client.Snapshot("a"); state.HasData {
		t.Fatalf("expired key = %+v", state)
	}
	if state := client.Snapshot("b"); !state.HasData || state.Data != "second" {
		t.Fatalf("independent key = %+v", state)
	}
}

// TestGCRemovalStopsTimers isolates recreated keys from cleanup belonging to removed state.
func TestGCRemovalStopsTimers(t *testing.T) {
	for _, action := range []string{"remove", "clear", "close"} {
		t.Run(action, func(t *testing.T) {
			client, scheduler := newGCClient(t, time.Minute)
			client.Set("a", "old")
			previous := scheduler.latest(t)
			switch action {
			case "remove":
				client.Remove("a")
			case "clear":
				client.Clear()
			case "close":
				client.Close()
			}
			if !previous.stopped.Load() {
				t.Fatal("discarding state did not stop cleanup")
			}
			client.Set("a", "new")
			scheduler.now.Add(int64(time.Minute))
			previous.callback()
			state := client.Snapshot("a")
			if action == "close" {
				if state.HasData || !errors.Is(state.Err, ErrClosed) || scheduler.count() != 1 {
					t.Fatalf("cleanup after close = %+v", state)
				}
				return
			}
			if !state.HasData || state.Data != "new" {
				t.Fatalf("a stopped cleanup deleted recreated data: %+v", state)
			}
		})
	}
}
