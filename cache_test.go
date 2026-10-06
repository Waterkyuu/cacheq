package query

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCacheExpiration refreshes at the exact expiration boundary while isolating keys.
func TestCacheExpiration(t *testing.T) {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	cache := NewClient(Options{Clock: func() time.Time { return now }})
	calls := 0
	load := func(context.Context) (int, time.Time, error) {
		calls++
		return calls, now.Add(time.Hour), nil
	}
	for _, key := range []string{"a", "a", "b"} {
		if _, err := FetchWithExpiry(context.Background(), cache, key, load); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("loads = %d, want one per key", calls)
	}
	now = now.Add(time.Hour)
	if got, err := FetchWithExpiry(context.Background(), cache, "a", load); got != 3 || err != nil {
		t.Fatalf("expired result = %d, %v", got, err)
	}
}

// TestCacheRetryExpiration reuses an explicitly cached failure and retries when its delay expires.
func TestCacheRetryExpiration(t *testing.T) {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	cache := NewClient(Options{Clock: func() time.Time { return now }})
	failure := errors.New("unavailable")
	calls := 0
	load := func(context.Context) (string, time.Time, error) {
		calls++
		return "offline", now.Add(time.Minute), failure
	}
	for range 2 {
		if value, err := FetchWithExpiry(context.Background(), cache,
			"a",
			load,
		); value != "offline" ||
			!errors.Is(err, failure) {
			t.Fatalf("fallback = %q, %v", value, err)
		}
	}
	if calls != 1 {
		t.Fatalf("failure loaded %d times before retry expiration", calls)
	}
	now = now.Add(time.Minute)
	_, _ = FetchWithExpiry(context.Background(), cache, "a", load)
	if calls != 2 {
		t.Fatalf("failure was not retried: loads = %d", calls)
	}
}

// TestCacheZeroExpiration leaves uncached results eligible for the next load.
func TestCacheZeroExpiration(t *testing.T) {
	cache := NewClient(Options{})
	calls := 0
	load := func(context.Context) (int, time.Time, error) {
		calls++
		return calls, time.Time{}, nil
	}
	for want := 1; want <= 2; want++ {
		if value, err := FetchWithExpiry(context.Background(), cache, "a", load); value != want || err != nil {
			t.Fatalf("uncached value = %d, %v; want %d", value, err, want)
		}
	}
}

// TestCacheConcurrentLoads merges requests for one key without blocking other keys.
func TestCacheConcurrentLoads(t *testing.T) {
	cache := NewClient(Options{})
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	load := func(ctx context.Context) (string, time.Time, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-ctx.Done():
			return "", time.Time{}, ctx.Err()
		case <-release:
			return "shared", time.Now().Add(time.Hour), nil
		}
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if value, err := FetchWithExpiry(context.Background(), cache, "a", load); value != "shared" || err != nil {
				t.Errorf("shared result = %q, %v", value, err)
			}
		}()
	}
	<-started
	other := make(chan string, 1)
	go func() {
		value, _ := FetchWithExpiry(context.Background(), cache, "b", func(context.Context) (string, time.Time, error) {
			return "independent", time.Now().Add(time.Hour), nil
		})
		other <- value
	}()
	select {
	case value := <-other:
		if value != "independent" {
			t.Errorf("other key = %q", value)
		}
	case <-time.After(3 * time.Second):
		t.Error("one key's loader blocked another key")
	}
	close(release)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("same key loaded %d times", got)
	}
}

// TestCacheWaiterCancellation does not cancel the caller that owns an active load.
func TestCacheWaiterCancellation(t *testing.T) {
	cache := NewClient(Options{})
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	load := func(context.Context) (string, time.Time, error) {
		close(started)
		<-release
		return "loaded", time.Now().Add(time.Hour), nil
	}
	go func() {
		_, err := FetchWithExpiry(context.Background(), cache, "a", load)
		finished <- err
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() {
		_, err := FetchWithExpiry(ctx, cache, "a", load)
		waiter <- err
	}()
	cancel()
	if err := <-waiter; !errors.Is(err, context.Canceled) {
		t.Errorf("waiter cancellation = %v", err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if value, err := FetchWithExpiry(context.Background(), cache, "a", load); value != "loaded" || err != nil {
		t.Fatalf("waiter discarded loaded value: %q, %v", value, err)
	}
}

// TestCacheLoaderCancellation allows another caller to retry and never caches a canceled load.
func TestCacheLoaderCancellation(t *testing.T) {
	cache := NewClient(Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := FetchWithExpiry(ctx, cache, "a", func(ctx context.Context) (string, time.Time, error) {
			close(started)
			<-ctx.Done()
			return "canceled", time.Now().Add(time.Hour), nil
		})
		first <- err
	}()
	<-started
	second := make(chan error, 1)
	go func() {
		value, err := FetchWithExpiry(context.Background(), cache,
			"a",
			func(context.Context) (string, time.Time, error) {
				return "retried", time.Now().Add(time.Hour), nil
			},
		)
		if value != "retried" {
			t.Errorf("remaining caller = %q", value)
		}
		second <- err
	}()
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Errorf("loader cancellation = %v", err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if _, err := FetchWithExpiry[string](ctx, cache, "a", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller used a cached result: %v", err)
	}
}

// TestClientRetries covers recovery and exhaustion using an immediate deterministic retry policy.
func TestClientRetries(t *testing.T) {
	for _, succeed := range []bool{true, false} {
		client := NewClient(Options{
			StaleTime: time.Hour, Retry: 2, RetryDelay: func(int) time.Duration { return 0 },
		})
		calls := 0
		failure := errors.New("unavailable")
		value, err := Fetch(context.Background(), client, "a", func(context.Context) (string, error) {
			calls++
			if succeed && calls == 3 {
				return "recovered", nil
			}
			return "", failure
		})
		if calls != 3 || succeed && (err != nil || value != "recovered") || !succeed && !errors.Is(err, failure) {
			t.Fatalf("retry result = %q, %v; calls = %d", value, err, calls)
		}
		client.Close()
	}
}

// TestRetryCancellation stops during backoff without starting another attempt.
func TestRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(Options{
		Retry: 3,
		RetryDelay: func(int) time.Duration {
			cancel()
			return time.Hour
		},
	})
	defer client.Close()
	_, err := Fetch(ctx, client, "a", func(context.Context) (string, error) { return "", errors.New("offline") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry cancellation = %v", err)
	}
}

// TestClientTimeout bounds all attempts and does not restart its own deadline as a canceled owner.
func TestClientTimeout(t *testing.T) {
	client := NewClient(Options{Timeout: time.Millisecond, Retry: 3})
	defer client.Close()
	_, err := Fetch(context.Background(), client, "a", func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("query timeout = %v", err)
	}
}

// TestDefaultRetryBackoff doubles retry delays while bounding later attempts.
func TestDefaultRetryBackoff(t *testing.T) {
	want := []time.Duration{
		time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		30 * time.Second,
	}
	for i, delay := range want {
		if got := retryDelay(i + 1); got != delay {
			t.Fatalf("retry %d delay = %v, want %v", i+1, got, delay)
		}
	}
	if got := retryDelay(100); got != 30*time.Second {
		t.Fatalf("late retry delay = %v", got)
	}
}

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
func newGCClient(t *testing.T, retention time.Duration) (*Client, *manualGCScheduler) {
	t.Helper()
	scheduler := &manualGCScheduler{timers: make([]*manualGCTimer, 0)}
	scheduler.now.Store(time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC).UnixNano())
	client := NewClient(Options{
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
			Set(client, "a", "cached")
			timer := scheduler.latest(t)
			scheduler.now.Add(int64(elapsed))
			timer.callback()
			if elapsed < time.Minute {
				if delay := scheduler.latest(t).delay; delay != time.Nanosecond {
					t.Fatalf("remaining retention = %v", delay)
				}
			}
			state := Get[string](client, "a")
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
			Set(client, "a", "cached")
			scheduler.now.Add(int64(24 * time.Hour))
			if state := Get[string](client, "a"); !state.HasData || !state.Stale {
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
			Set(client, "a", "cached")
			previous := scheduler.latest(t)
			scheduler.now.Add(int64(30 * time.Second))
			switch action {
			case "fetch":
				if _, err := Fetch[string](context.Background(), client, "a", nil); err != nil {
					t.Fatal(err)
				}
			case "query":
				handle := Query(client, "a", func(context.Context) (string, error) { return "unused", nil })
				handle.Close()
			case "snapshot":
				Get[string](client, "a")
			case "set":
				Set(client, "a", "updated")
			case "invalidate":
				client.Invalidate("a")
			}
			current := scheduler.latest(t)
			if !previous.stopped.Load() || current == previous {
				t.Fatal("reuse did not replace the cleanup timer")
			}
			scheduler.now.Add(int64(30 * time.Second))
			previous.callback()
			if state := Get[string](client, "a"); !state.HasData {
				t.Fatal("a stopped callback deleted reused data")
			}
			scheduler.now.Add(int64(time.Minute))
			scheduler.latest(t).callback()
			if state := Get[string](client, "a"); state.HasData {
				t.Fatalf("unused data was retained: %+v", state)
			}
		})
	}
}

// TestGCSubscriptions retains data until the final observer leaves and then starts a full retention window.
func TestGCSubscriptions(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	Set(client, "a", "cached")
	previous := scheduler.latest(t)
	firstHandle := Query(
		client,
		"a",
		func(context.Context) (string, error) { return "unused", nil },
		QueryOptions{Enabled: false},
	)
	first, cancelFirst := firstHandle.Updates(), firstHandle.Close
	defer cancelFirst()
	secondHandle := Query(
		client,
		"a",
		func(context.Context) (string, error) { return "unused", nil },
		QueryOptions{Enabled: false},
	)
	second, cancelSecond := secondHandle.Updates(), secondHandle.Close
	defer cancelSecond()
	<-first
	<-second
	scheduler.now.Add(int64(time.Hour))
	previous.callback()
	if state := Get[string](client, "a"); !state.HasData {
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
	if state := Get[string](client, "a"); state.HasData {
		t.Fatalf("unsubscribed data was retained: %+v", state)
	}
}

// TestGCLoads retains stale data during a refresh and starts cleanup after success, failure, or cancellation.
func TestGCLoads(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			client, scheduler := newGCClient(t, time.Minute)
			Set(client, "a", "old")
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
				_, err := Fetch(context.Background(), client, "a", fetch)
				finished <- err
			}()
			<-started
			previous.callback()
			if state := Get[string](client, "a"); !state.HasData || !state.Fetching || state.Data != "old" {
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
			if state := Get[string](client, "a"); state.HasData || state.Err != nil || state.Status != Idle {
				t.Fatalf("completed load remained cached: %+v", state)
			}
		})
	}
}

// TestGCInitialLoad schedules cleanup for newly loaded data even when no previous entry exists.
func TestGCInitialLoad(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	if _, err := Fetch(context.Background(), client, "a", func(context.Context) (string, error) {
		return "loaded", nil
	}); err != nil {
		t.Fatal(err)
	}
	scheduler.now.Add(int64(time.Minute))
	scheduler.latest(t).callback()
	if state := Get[string](client, "a"); state.HasData {
		t.Fatalf("initial load remained cached: %+v", state)
	}
	value, err := Fetch(context.Background(), client, "a", func(context.Context) (string, error) {
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
	_, err := Fetch(context.Background(), client, "a", func(context.Context) (string, error) {
		return "", failure
	})
	if !errors.Is(err, failure) {
		t.Fatalf("initial failure = %v", err)
	}
	scheduler.now.Add(int64(time.Minute))
	scheduler.latest(t).callback()
	if state := Get[string](client, "a"); state.Err != nil || state.Status != Idle {
		t.Fatalf("initial failure remained cached: %+v", state)
	}
}

// TestGCKeyIsolation expires one key without deleting another key's more recently installed data.
func TestGCKeyIsolation(t *testing.T) {
	client, scheduler := newGCClient(t, time.Minute)
	Set(client, "a", "first")
	first := scheduler.latest(t)
	scheduler.now.Add(int64(30 * time.Second))
	Set(client, "b", "second")
	scheduler.now.Add(int64(30 * time.Second))
	first.callback()
	if state := Get[string](client, "a"); state.HasData {
		t.Fatalf("expired key = %+v", state)
	}
	if state := Get[string](client, "b"); !state.HasData || state.Data != "second" {
		t.Fatalf("independent key = %+v", state)
	}
}

// TestGCRemovalStopsTimers isolates recreated keys from cleanup belonging to removed state.
func TestGCRemovalStopsTimers(t *testing.T) {
	for _, action := range []string{"remove", "clear", "close"} {
		t.Run(action, func(t *testing.T) {
			client, scheduler := newGCClient(t, time.Minute)
			Set(client, "a", "old")
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
			Set(client, "a", "new")
			scheduler.now.Add(int64(time.Minute))
			previous.callback()
			state := Get[string](client, "a")
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

// Example_sharedCache shares one result across readers while its freshness window remains open.
func Example_sharedCache() {
	client := NewClient(Options{StaleTime: time.Hour})
	defer client.Close()
	calls := 0
	fetch := func(context.Context) (string, error) {
		calls++
		return "shared", nil
	}
	first, _ := Fetch(context.Background(), client, "models", fetch)
	second, _ := Fetch(context.Background(), client, "models", fetch)
	fmt.Println(first, second, calls)
	// Output: shared shared 1
}

// Example_cacheFreshness checks expiration without starting a request.
func Example_cacheFreshness() {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	client := NewClient(Options{
		StaleTime: time.Hour, Clock: func() time.Time { return now },
	})
	defer client.Close()
	Set(client, "models", "available")
	fmt.Println(Get[string](client, "models").Stale)
	now = now.Add(time.Hour)
	fmt.Println(Get[string](client, "models").Stale)
	// Output:
	// false
	// true
}
