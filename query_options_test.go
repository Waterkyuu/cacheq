package cacheq

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// TestQueryOptionsFreshness keeps same-key data shared while consumers evaluate independent deadlines.
func TestQueryOptionsFreshness(t *testing.T) {
	c, now := newAgeClient(t, Options{StaleTime: time.Minute})
	if err := Set(c, "users", 7); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	fetch := func(context.Context) (int, error) {
		calls.Add(1)
		return 8, nil
	}
	zero, hour := time.Duration(0), time.Hour
	fast := Query(c, "users", fetch, QueryOptions{StaleTime: &zero})
	slow := Query(c, "users", fetch, QueryOptions{StaleTime: &hour})
	t.Cleanup(fast.Close)
	t.Cleanup(slow.Close)
	for _, tc := range []struct {
		// handle selects the consumer whose initial snapshot is checked.
		handle *QueryHandle[int]
		// stale is the consumer's expected freshness for the shared value.
		stale bool
		// deadline is the consumer's effective freshness window since installation.
		deadline time.Duration
	}{
		{handle: fast, stale: true},
		{handle: slow, deadline: time.Hour},
	} {
		state := <-tc.handle.Updates()
		if state.Data != 7 || state.Stale != tc.stale || !state.ExpiresAt.Equal(time.Unix(0, 0).Add(tc.deadline)) {
			t.Fatalf("initial snapshot = %+v", state)
		}
	}
	now.Store(int64(time.Minute))
	if !Get[int](c, "users").Stale || slow.Snapshot().Stale {
		t.Fatal("consumer freshness leaked into Client defaults")
	}
	if err := slow.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("enabling the fresh consumer started a request")
	}
	if err := fast.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	fastState := awaitState(t, fast.Updates(), func(s Snapshot[int]) bool { return s.Data == 8 && !s.Fetching })
	slowState := awaitState(t, slow.Updates(), func(s Snapshot[int]) bool { return s.Data == 8 && !s.Fetching })
	if !fastState.Stale || slowState.Stale || calls.Load() != 1 {
		t.Fatalf("shared refresh: fast=%+v slow=%+v calls=%d", fastState, slowState, calls.Load())
	}
	if err := c.Invalidate("users", InvalidateOptions{Refetch: RefetchNone}); err != nil {
		t.Fatal(err)
	}
	if !fast.Snapshot().Stale || !slow.Snapshot().Stale {
		t.Fatal("a longer freshness policy bypassed explicit invalidation")
	}
	if err := Set(c, "users", 9); err != nil {
		t.Fatal(err)
	}
	fastState = awaitState(t, fast.Updates(), func(s Snapshot[int]) bool { return s.Data == 9 })
	slowState = awaitState(t, slow.Updates(), func(s Snapshot[int]) bool { return s.Data == 9 })
	if !fastState.Stale || slowState.Stale || Get[int](c, "users").Stale {
		t.Fatal("local writes did not preserve independent freshness")
	}
}

// TestFetchOptionsFreshness applies per-call cache decisions without registering policies on the key.
func TestFetchOptionsFreshness(t *testing.T) {
	c, now := newAgeClient(t, Options{StaleTime: time.Minute})
	var calls int
	fetch := func(context.Context) (int, error) {
		calls++
		return calls, nil
	}
	hour, zero := time.Hour, time.Duration(0)
	if err := Prefetch(context.Background(), c, "users", fetch); err != nil {
		t.Fatal(err)
	}
	now.Store(int64(time.Minute))
	value, err := FetchWithOptions(context.Background(), c, "users", fetch, FetchOptions{StaleTime: &hour})
	if err != nil || value != 1 || calls != 1 {
		t.Fatalf("longer policy = %d, %v; calls=%d", value, err, calls)
	}
	value, err = Fetch(context.Background(), c, "users", fetch)
	if err != nil || value != 2 || calls != 2 {
		t.Fatalf("default policy = %d, %v; calls=%d", value, err, calls)
	}
	value, err = FetchWithOptions(context.Background(), c, "users", fetch, FetchOptions{StaleTime: &zero})
	if err != nil || value != 3 || calls != 3 {
		t.Fatalf("zero freshness = %d, %v; calls=%d", value, err, calls)
	}
	if Get[int](c, "users").Stale {
		t.Fatal("the last caller's zero freshness changed Client freshness")
	}
	value, err = FetchWithOptions[int](context.Background(), c, "users", nil, FetchOptions{StaleTime: &hour})
	if err != nil || value != 3 {
		t.Fatalf("cache-only lookup = %d, %v", value, err)
	}
}

// TestQueryOptionsExplicitExpiry preserves loader deadlines and Client availability limits.
func TestQueryOptionsExplicitExpiry(t *testing.T) {
	c, now := newAgeClient(t, Options{StaleTime: time.Minute, MaxAge: 5 * time.Minute})
	hour, zero := time.Hour, time.Duration(0)
	fetch := func(context.Context) (int, error) { return 7, nil }
	ordinary := Query(c, "ordinary", fetch, QueryOptions{Enabled: true, StaleTime: &hour})
	t.Cleanup(ordinary.Close)
	state := awaitState(t, ordinary.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if !state.ExpiresAt.Equal(time.Unix(0, 0).Add(5 * time.Minute)) {
		t.Fatalf("availability cap = %+v", state)
	}
	deadline := time.Unix(0, 0).Add(2 * time.Minute)
	if _, err := FetchWithExpiry(context.Background(), c, "explicit", func(context.Context) (int, time.Time, error) {
		return 9, deadline, nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, duration := range []time.Duration{zero, hour} {
		handle := Query(c, "explicit", fetch, QueryOptions{StaleTime: &duration})
		t.Cleanup(handle.Close)
		state := handle.Snapshot()
		if state.Stale || !state.ExpiresAt.Equal(deadline) {
			t.Fatalf("explicit deadline changed: %+v", state)
		}
	}
	now.Store(int64(2 * time.Minute))
	if _, err := FetchWithOptions[int](
		context.Background(), c, "explicit", nil, FetchOptions{StaleTime: &hour},
	); !errors.Is(err, ErrNoFetcher) {
		t.Fatalf("expired explicit deadline = %v", err)
	}
	now.Store(int64(5 * time.Minute))
	if ordinary.Snapshot().HasData {
		t.Fatal("consumer freshness extended maximum data age")
	}
}

// TestQueryOptionsZeroClock distinguishes a valid zero deadline from explicit invalidation.
func TestQueryOptionsZeroClock(t *testing.T) {
	c := NewClient(Options{Clock: func() time.Time { return time.Time{} }})
	t.Cleanup(c.Close)
	if err := Set(c, "users", 7); err != nil {
		t.Fatal(err)
	}
	hour := time.Hour
	handle := Query(c, "users", func(context.Context) (int, error) { return 8, nil }, QueryOptions{StaleTime: &hour})
	t.Cleanup(handle.Close)
	if handle.Snapshot().Stale || !Get[int](c, "users").Stale {
		t.Fatal("a zero installation deadline was mistaken for explicit invalidation")
	}
	if err := c.Invalidate("users", InvalidateOptions{Refetch: RefetchNone}); err != nil {
		t.Fatal(err)
	}
	if !handle.Snapshot().Stale {
		t.Fatal("explicit invalidation was hidden by consumer freshness")
	}
}

// TestQueryOptionsRetries covers inheritance, zero overrides, error filtering, and terminal cancellation.
func TestQueryOptionsRetries(t *testing.T) {
	transient, permanent := errors.New("offline"), errors.New("forbidden")
	zero, one := 0, 1
	for _, tc := range []struct {
		// name identifies the policy and terminal error combination.
		name string
		// options supplies the per-consumer overrides being exercised.
		options FetchOptions
		// failure is returned by every loader attempt.
		failure error
		// attempts is the expected number of actual loader invocations.
		attempts int
	}{
		{name: "inherit", failure: transient, attempts: 3},
		{name: "disable", options: FetchOptions{Retry: &zero}, failure: transient, attempts: 1},
		{name: "limit", options: FetchOptions{Retry: &one}, failure: transient, attempts: 2},
		{name: "client predicate", failure: permanent, attempts: 1},
		{
			name: "override predicate", failure: permanent, attempts: 3,
			options: FetchOptions{RetryIf: func(error) bool { return true }},
		},
		{
			name: "reject transient", failure: transient, attempts: 1,
			options: FetchOptions{RetryIf: func(error) bool { return false }},
		},
		{name: "cancel", failure: context.Canceled, attempts: 1},
		{name: "deadline", failure: context.DeadlineExceeded, attempts: 1},
	} {
		for _, api := range []string{"fetch", "query"} {
			t.Run(tc.name+"/"+api, func(t *testing.T) {
				c := NewClient(Options{
					Retry: 2, RetryDelay: func(int) time.Duration { return 0 },
					RetryIf: func(err error) bool { return errors.Is(err, transient) },
				})
				t.Cleanup(c.Close)
				var calls int
				fetch := func(context.Context) (int, error) {
					calls++
					return 0, fmt.Errorf("load: %w", tc.failure)
				}
				var err error
				if api == "fetch" {
					_, err = FetchWithOptions(context.Background(), c, "users", fetch, tc.options)
				} else {
					handle := Query(c, "users", fetch, QueryOptions{
						Retry: tc.options.Retry, RetryIf: tc.options.RetryIf, RetryDelay: tc.options.RetryDelay,
					})
					t.Cleanup(handle.Close)
					_, err = handle.Refetch(context.Background())
				}
				if !errors.Is(err, tc.failure) || calls != tc.attempts {
					t.Fatalf("attempts=%d error=%v, want %d and %v", calls, err, tc.attempts, tc.failure)
				}
				if c.Stats().Retries != uint64(tc.attempts-1) {
					t.Fatalf("retry statistics = %+v", c.Stats())
				}
			})
		}
	}
}

// TestQueryOptionsCopied preserves each handle's policy when caller-owned option values change.
func TestQueryOptionsCopied(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Minute})
	hour, retries := time.Hour, 1
	var calls atomic.Int32
	handle := Query(c, "users", func(context.Context) (int, error) {
		if calls.Add(1) == 1 {
			return 0, errors.New("offline")
		}
		return 7, nil
	}, QueryOptions{
		StaleTime: &hour, Retry: &retries, RetryDelay: func(int) time.Duration { return 0 },
	})
	t.Cleanup(handle.Close)
	hour, retries = 0, 0
	if err := handle.SetEnabled(true); err != nil {
		t.Fatal(err)
	}
	state := awaitState(t, handle.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if state.Stale || !state.ExpiresAt.Equal(time.Unix(0, 0).Add(time.Hour)) || calls.Load() != 2 {
		t.Fatalf("copied policy = %+v; calls=%d", state, calls.Load())
	}
}

// TestQueryOptionsTimeout copies whole-load deadlines and permits explicit timeout removal.
func TestQueryOptionsTimeout(t *testing.T) {
	hour, zero := time.Hour, time.Duration(0)
	for _, tc := range []struct {
		// name identifies the inherited or overridden timeout.
		name string
		// timeout overrides the Client timeout when non-nil.
		timeout *time.Duration
		// duration is the expected effective timeout, or zero for none.
		duration time.Duration
	}{
		{name: "inherit", duration: 2 * time.Hour},
		{name: "override", timeout: &hour, duration: time.Hour},
		{name: "disable", timeout: &zero},
	} {
		for _, api := range []string{"fetch", "query"} {
			t.Run(tc.name+"/"+api, func(t *testing.T) {
				c := NewClient(Options{Timeout: 2 * time.Hour})
				t.Cleanup(c.Close)
				before := time.Now()
				fetch := func(ctx context.Context) (time.Time, error) {
					deadline, _ := ctx.Deadline()
					return deadline, nil
				}
				var deadline time.Time
				var err error
				if api == "fetch" {
					deadline, err = FetchWithOptions(
						context.Background(),
						c,
						"users",
						fetch,
						FetchOptions{Timeout: tc.timeout},
					)
				} else {
					handle := Query(c, "users", fetch, QueryOptions{Timeout: tc.timeout})
					t.Cleanup(handle.Close)
					deadline, err = handle.Refetch(context.Background())
				}
				after := time.Now()
				if err != nil {
					t.Fatal(err)
				}
				if tc.duration == 0 {
					if !deadline.IsZero() {
						t.Fatalf("disabled timeout = %v", deadline)
					}
				} else if deadline.Before(before.Add(tc.duration)) || deadline.After(after.Add(tc.duration)) {
					t.Fatalf(
						"deadline = %v, want between %v and %v",
						deadline,
						before.Add(tc.duration),
						after.Add(tc.duration),
					)
				}
			})
		}
	}
	c := NewClient(Options{Timeout: time.Hour})
	t.Cleanup(c.Close)
	callerDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), callerDeadline)
	defer cancel()
	deadline, err := FetchWithOptions(ctx, c, "users", func(ctx context.Context) (time.Time, error) {
		deadline, _ := ctx.Deadline()
		return deadline, nil
	}, FetchOptions{Timeout: &zero})
	if err != nil || !deadline.Equal(callerDeadline) {
		t.Fatalf("caller deadline = %v, %v", deadline, err)
	}
}

// TestQueryOptionsSharedRequest preserves the initiator's policy when differently configured consumers join.
func TestQueryOptionsSharedRequest(t *testing.T) {
	c, _ := newAgeClient(t, Options{StaleTime: time.Minute})
	started, release := make(chan struct{}), make(chan struct{})
	hour, zero, retries := time.Hour, time.Duration(0), 1
	var ownerCalls, joiningCalls, delays atomic.Int32
	failure := errors.New("offline")
	owner := Query(c, "users", func(ctx context.Context) (int, error) {
		if _, exists := ctx.Deadline(); !exists {
			return 0, errors.New("owner timeout disappeared")
		}
		if ownerCalls.Add(1) == 1 {
			close(started)
			select {
			case <-release:
				return 0, failure
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}
		return 7, nil
	}, QueryOptions{
		Enabled: true, StaleTime: &hour, Retry: &retries, Timeout: &hour,
		RetryIf: func(err error) bool { return errors.Is(err, failure) },
		RetryDelay: func(int) time.Duration {
			delays.Add(1)
			return 0
		},
	})
	t.Cleanup(owner.Close)
	<-started
	joinFetch := func(context.Context) (int, error) {
		joiningCalls.Add(1)
		return 99, nil
	}
	joiner := Query(c, "users", joinFetch, QueryOptions{
		Enabled: true, StaleTime: &zero, Timeout: &zero, RetryIf: func(error) bool { return false },
	})
	t.Cleanup(joiner.Close)
	result := make(chan error, 1)
	go func() {
		_, err := FetchWithOptions(context.Background(), c, "users", joinFetch, FetchOptions{
			Timeout: &zero, RetryIf: func(error) bool { return false },
		})
		result <- err
	}()
	awaitStats(t, c, func(s Stats) bool { return s.MergedRequests == 2 })
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	ownerState := awaitState(t, owner.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	joinState := awaitState(t, joiner.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if ownerCalls.Load() != 2 || joiningCalls.Load() != 0 || delays.Load() != 1 {
		t.Fatalf("owner=%d joiner=%d delays=%d", ownerCalls.Load(), joiningCalls.Load(), delays.Load())
	}
	if ownerState.Stale || !joinState.Stale || ownerState.Data != 7 || joinState.Data != 7 {
		t.Fatalf("shared states: owner=%+v joiner=%+v", ownerState, joinState)
	}
	if value, err := joiner.Refetch(context.Background()); err != nil || value != 99 {
		t.Fatalf("joining consumer's next request = %d, %v", value, err)
	}
}

// TestQueryOptionsRetryCallback permits Client calls and stops when the callback cancels the owner.
func TestQueryOptionsRetryCallback(t *testing.T) {
	c := NewClient(Options{Retry: 2})
	t.Cleanup(c.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls, predicates, delays int
	_, err := FetchWithOptions(ctx, c, "users", func(context.Context) (int, error) {
		calls++
		return 0, errors.New("offline")
	}, FetchOptions{
		RetryIf: func(error) bool {
			predicates++
			c.Stats()
			cancel()
			return true
		},
		RetryDelay: func(int) time.Duration {
			delays++
			return 0
		},
	})
	awaitStats(t, c, func(s Stats) bool { return s.LoadCancellations == 1 })
	if !errors.Is(err, context.Canceled) || calls != 1 || predicates != 1 || delays != 0 {
		t.Fatalf("callback cancellation: err=%v calls=%d predicates=%d delays=%d", err, calls, predicates, delays)
	}
}

// TestQueryOptionsAutomaticRefresh consistently chooses the oldest enabled consumer's loader and policy.
func TestQueryOptionsAutomaticRefresh(t *testing.T) {
	c := newInvalidationClient(t)
	var calls atomic.Int32
	retries := 1
	first := Query(c, "users", func(context.Context) (int, error) {
		if calls.Add(1)%2 == 1 {
			return 0, errors.New("offline")
		}
		return 7, nil
	}, QueryOptions{Enabled: true, Retry: &retries, RetryDelay: func(int) time.Duration { return 0 }})
	t.Cleanup(first.Close)
	awaitState(t, first.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	second := Query(c, "users", func(context.Context) (int, error) { return 99, nil })
	t.Cleanup(second.Close)
	if err := c.Invalidate("users"); err != nil {
		t.Fatal(err)
	}
	awaitStats(t, c, func(s Stats) bool { return s.LoadSuccesses == 2 })
	if first.Snapshot().Data != 7 || second.Snapshot().Data != 7 || calls.Load() != 4 {
		t.Fatal("automatic refresh did not retain the oldest enabled consumer's policy")
	}
	if err := first.SetEnabled(false); err != nil {
		t.Fatal(err)
	}
	if err := c.Invalidate("users"); err != nil {
		t.Fatal(err)
	}
	awaitState(t, second.Updates(), func(s Snapshot[int]) bool { return s.Data == 99 && !s.Fetching })
	if calls.Load() != 4 {
		t.Fatal("a disabled consumer supplied automatic refresh work")
	}
}
