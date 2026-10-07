package cacheq

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// awaitStats waits for an observable counter transition with a deadlock guard, without sleep-based ordering.
func awaitStats(t *testing.T, c *Client, matches func(Stats) bool) Stats {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		state := c.Stats()
		if matches(state) {
			return state
		}
		select {
		case <-timer.C:
			t.Fatalf("expected statistics were not published: %+v", state)
		default:
			runtime.Gosched()
		}
	}
}

// TestStatsCacheDecisions counts fresh reuse and misses without counting passive reads or forced refreshes.
func TestStatsCacheDecisions(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "a", 1)
	disabled := Query(
		c,
		"a",
		func(context.Context) (int, error) { return 2, nil },
		QueryOptions{Enabled: new(bool)},
	)
	t.Cleanup(disabled.Close)
	Get[int](c, "a")
	disabled.Snapshot()
	if state := c.Stats(); state.CacheHits != 0 || state.CacheMisses != 0 || state.CacheEntries != 1 {
		t.Fatalf("passive reads = %+v", state)
	}
	if _, err := Fetch[int](
		context.Background(),
		c,
		"a",
		nil,
	); err != nil {
		t.Fatal(err)
	}
	if err := disabled.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	if err := disabled.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	active := Query(c, "a", func(context.Context) (int, error) { return 3, nil })
	t.Cleanup(active.Close)
	if _, err := active.Refetch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.Invalidate("a"); err != nil {
		t.Fatal(err)
	}
	awaitState(t, active.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if _, err := Fetch[int](
		context.Background(),
		c,
		"missing",
		nil,
	); !errors.Is(err, ErrNoFetcher) {
		t.Fatal(err)
	}
	state := c.Stats()
	if state.CacheHits != 3 || state.CacheMisses != 1 || state.Loads != 2 || state.LoadSuccesses != 2 {
		t.Fatalf("cache decisions and explicit refreshes = %+v", state)
	}
}

// TestStatsRejectedCalls leaves cache counters unchanged when validation rejects a request before cache lookup.
func TestStatsRejectedCalls(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "a", 1)
	before := c.Stats()
	Fetch[string](
		context.Background(),
		c,
		"a",
		nil,
	)
	Fetch[int](
		context.Background(),
		c,
		[]string{"invalid"},
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	Fetch[int](
		ctx,
		c,
		"a",
		nil,
	)
	invalid := Query[int](c, "a", nil)
	invalid.Close()
	if state := c.Stats(); state != before {
		t.Fatalf("rejected requests changed statistics: %+v", state)
	}
}

// TestStatsLoadOutcomes counts one shared operation, its actual retries, and independently measured elapsed time.
func TestStatsLoadOutcomes(t *testing.T) {
	failure := errors.New("offline")
	for _, tc := range []struct {
		// name identifies the final outcome being classified.
		name string
		// failures determines how many initial attempts return the transient failure.
		failures int
		// terminal is returned once the transient failures are exhausted.
		terminal error
		// wantErr is the final result expected after applying the retry policy.
		wantErr error
		// attempts is the expected number of actual loader invocations.
		attempts int
	}{
		{name: "recovered", failures: 2, attempts: 3},
		{name: "exhausted", failures: 3, wantErr: failure, attempts: 3},
		{name: "canceled", terminal: context.Canceled, wantErr: context.Canceled, attempts: 1},
		{name: "deadline", terminal: context.DeadlineExceeded, wantErr: context.DeadlineExceeded, attempts: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, freshness := newAgeClient(t, Options{Retry: 2, RetryDelay: func(int) time.Duration { return 0 }})
			var elapsed atomic.Int64
			c.loadNow = func() time.Time { return time.Unix(0, elapsed.Load()) }
			calls := 0
			_, err := Fetch(
				context.Background(),
				c,
				"a",
				func(context.Context) (int, error) {
					calls++
					elapsed.Add(int64(10 * time.Millisecond))
					freshness.Add(int64(time.Hour))
					if calls <= tc.failures {
						return 0, failure
					}
					return 1, tc.terminal
				},
			)
			if !errors.Is(err, tc.wantErr) || calls != tc.attempts {
				t.Fatalf("load = %v, calls = %d", err, calls)
			}
			state := c.Stats()
			if state.Loads != 1 || state.Retries != uint64(tc.attempts-1) || state.CacheMisses != 1 ||
				state.TotalLoadDuration != time.Duration(tc.attempts)*10*time.Millisecond {
				t.Fatalf("attempts and elapsed time = %+v", state)
			}
			switch tc.wantErr {
			case nil:
				if state.LoadSuccesses != 1 || state.LoadFailures != 0 || state.LoadCancellations != 0 {
					t.Fatalf("successful outcome = %+v", state)
				}
			case failure:
				if state.LoadSuccesses != 0 || state.LoadFailures != 1 || state.LoadCancellations != 0 {
					t.Fatalf("failed outcome = %+v", state)
				}
			default:
				if state.LoadSuccesses != 0 || state.LoadFailures != 0 || state.LoadCancellations != 1 {
					t.Fatalf("canceled outcome = %+v", state)
				}
			}
		})
	}
}

// TestStatsFreshFallback counts reuse as a cache hit even when the retained fallback includes an error.
func TestStatsFreshFallback(t *testing.T) {
	c := newInvalidationClient(t)
	failure := errors.New("fallback")
	load := func(context.Context) (int, time.Time, error) { return 7, c.options.Clock().Add(time.Hour), failure }
	for range 2 {
		if value, err := FetchWithExpiry(
			context.Background(),
			c,
			"a",
			load,
		); value != 7 || !errors.Is(err, failure) {
			t.Fatalf("fallback = %d, %v", value, err)
		}
	}
	if state := c.Stats(); state.CacheHits != 1 || state.CacheMisses != 1 || state.LoadFailures != 1 ||
		state.Loads != 1 || state.CacheEntries != 1 {
		t.Fatalf("fresh fallback statistics = %+v", state)
	}
}

// TestStatsOwnerCancellation counts each waiting call once even when it starts a replacement for a canceled owner.
func TestStatsOwnerCancellation(t *testing.T) {
	c := newInvalidationClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	owner := make(chan error, 1)
	go func() {
		_, err := Fetch(
			ctx,
			c,
			"a",
			func(ctx context.Context) (int, error) {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			},
		)
		owner <- err
	}()
	<-started
	waiter := make(chan error, 1)
	go func() {
		_, err := Fetch(
			context.Background(),
			c,
			"a",
			func(context.Context) (int, error) { return 2, nil },
		)
		waiter <- err
	}()
	awaitStats(t, c, func(s Stats) bool { return s.MergedRequests == 1 })
	cancel()
	if err := <-owner; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := <-waiter; err != nil {
		t.Fatal(err)
	}
	if state := c.Stats(); state.CacheMisses != 2 || state.CacheHits != 0 || state.MergedRequests != 1 ||
		state.Loads != 2 || state.LoadSuccesses != 1 || state.LoadCancellations != 1 {
		t.Fatalf("replacement work was counted as another caller: %+v", state)
	}
}

// TestStatsWaiterCancellation counts a canceled waiter as sharing work, not as canceling the shared load.
func TestStatsWaiterCancellation(t *testing.T) {
	c := newInvalidationClient(t)
	started, release := make(chan struct{}), make(chan struct{})
	query := Query(c, "a", func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-release:
			return 1, nil
		}
	})
	t.Cleanup(query.Close)
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := Fetch(
			ctx,
			c,
			"a",
			func(context.Context) (int, error) { return 2, nil },
		)
		finished <- err
	}()
	awaitStats(t, c, func(s Stats) bool { return s.MergedRequests == 1 })
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	awaitState(t, query.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if state := c.Stats(); state.Loads != 1 || state.LoadSuccesses != 1 || state.LoadCancellations != 0 ||
		state.MergedRequests != 1 || state.CacheMisses != 2 {
		t.Fatalf("waiting caller canceled shared statistics: %+v", state)
	}
}

// TestStatsDetachedLoads includes canceled work after Set, Remove, Clear, or Close detaches it from the cache.
func TestStatsDetachedLoads(t *testing.T) {
	for _, action := range []string{"set", "remove", "clear", "close"} {
		t.Run(action, func(t *testing.T) {
			c := newInvalidationClient(t)
			started := make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				_, err := Fetch(
					context.Background(),
					c,
					"a",
					func(ctx context.Context) (int, error) {
						close(started)
						<-ctx.Done()
						return 0, ctx.Err()
					},
				)
				finished <- err
			}()
			<-started
			switch action {
			case "set":
				Set(c, "a", 2)
			case "remove":
				c.Remove("a")
			case "clear":
				c.Clear()
			case "close":
				c.Close()
			}
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			state := c.Stats()
			if state.Loads != 1 || state.LoadCancellations != 1 || state.LoadFailures != 0 {
				t.Fatalf("detached load = %+v", state)
			}
		})
	}
}

// TestStatsCanceledBackoff counts only additional attempts that actually invoke the loader.
func TestStatsCanceledBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewClient(Options{Retry: 3, RetryDelay: func(int) time.Duration { cancel(); return time.Hour }})
	t.Cleanup(c.Close)
	_, err := Fetch(
		ctx,
		c,
		"a",
		func(context.Context) (int, error) { return 0, errors.New("offline") },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	state := awaitStats(t, c, func(s Stats) bool { return s.LoadCancellations == 1 })
	if state.Loads != 1 || state.Retries != 0 {
		t.Fatalf("canceled wait counted as an attempt: %+v", state)
	}
}

// TestStatsCleanup counts each effective cleanup once and leaves age and retention unchanged when inspected.
func TestStatsCleanup(t *testing.T) {
	c, scheduler := newGCClient(t, time.Minute)
	c.options.MaxAge = 30 * time.Second
	Set(c, "a", 1)
	previous := scheduler.latest(t)
	scheduler.now.Add(int64(30 * time.Second))
	before := c.Stats()
	for range 3 {
		if state := c.Stats(); state != before || state.AgeExpirations != 0 || state.CacheEntries != 1 {
			t.Fatalf("inspection ran cleanup: %+v", state)
		}
	}
	if previous.stopped.Load() || scheduler.count() != 1 {
		t.Fatal("inspection changed cache retention")
	}
	Get[int](c, "a")
	Get[int](c, "a")
	previous.callback()
	if state := c.Stats(); state.AgeExpirations != 1 || state.GCCollections != 0 || state.CacheEntries != 0 {
		t.Fatalf("age cleanup = %+v", state)
	}
	Set(c, "b", 2)
	scheduler.now.Add(int64(time.Minute))
	current := scheduler.latest(t)
	current.callback()
	current.callback()
	if state := c.Stats(); state.GCCollections != 1 || state.AgeExpirations != 1 || state.CacheEntries != 0 {
		t.Fatalf("timed cleanup = %+v", state)
	}

	bounded := NewClient(Options{MaxEntries: 1})
	t.Cleanup(bounded.Close)
	Set(bounded, "a", 1)
	Set(bounded, "b", 2)
	bounded.Remove("b")
	if state := bounded.Stats(); state.LRUEvictions != 1 || state.GCCollections != 0 || state.CacheEntries != 0 {
		t.Fatalf("capacity and manual removal = %+v", state)
	}
}

// TestStatsLifetime returns independent copies, preserves counters through cleanup, and isolates client instances.
func TestStatsLifetime(t *testing.T) {
	c := newInvalidationClient(t)
	for range 2 {
		if _, err := Fetch(
			context.Background(),
			c,
			"a",
			func(context.Context) (int, error) { return 1, nil },
		); err != nil {
			t.Fatal(err)
		}
	}
	before := c.Stats()
	copyOfStats := before
	copyOfStats.CacheHits = 100
	if c.Stats() != before {
		t.Fatal("returned statistics share mutable client state")
	}
	c.Clear()
	before.CacheEntries = 0
	if state := c.Stats(); state != before {
		t.Fatalf("clear reset lifetime counters: %+v", state)
	}
	c.Close()
	if state := c.Stats(); state != before {
		t.Fatalf("closed statistics = %+v", state)
	}
	other := newInvalidationClient(t)
	if state := other.Stats(); state != (Stats{}) {
		t.Fatalf("statistics leaked between clients: %+v", state)
	}
}
