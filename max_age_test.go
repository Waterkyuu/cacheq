package cacheq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// newAgeClient supplies an atomic clock so refresh completion and age-boundary tests remain deterministic.
func newAgeClient(t *testing.T, options Options) (*Client, *atomic.Int64) {
	t.Helper()
	now := &atomic.Int64{}
	options.Clock = func() time.Time { return time.Unix(0, now.Load()) }
	c := NewClient(options)
	t.Cleanup(c.Close)
	return c, now
}

// TestMaxAgeBoundary keeps data until the exact deadline and prevents reads or invalidation from renewing it.
func TestMaxAgeBoundary(t *testing.T) {
	for _, action := range []string{"get", "fetch", "invalidate"} {
		t.Run(action, func(t *testing.T) {
			c, now := newAgeClient(t, Options{StaleTime: time.Hour, MaxAge: time.Minute})
			Set(c, "a", "old")
			now.Store(int64(time.Minute - time.Nanosecond))
			switch action {
			case "get":
				Get[string](c, "a")
			case "fetch":
				if _, err := Fetch[string](context.Background(), c, "a", nil); err != nil {
					t.Fatal(err)
				}
			case "invalidate":
				c.Invalidate("a", InvalidateOptions{Refetch: RefetchNone})
			}
			if state := Get[string](c, "a"); !state.HasData || state.Data != "old" {
				t.Fatalf("before deadline = %+v", state)
			}
			now.Add(1)
			state := Get[string](c, "a")
			if state.HasData || state.Data != "" || !state.Stale || state.Status != Idle {
				t.Fatalf("at deadline = %+v", state)
			}
			if _, err := Fetch[string](context.Background(), c, "a", nil); !errors.Is(err, ErrNoFetcher) {
				t.Fatalf("over-age fresh cache was reused: %v", err)
			}
		})
	}
}

// TestMaxAgeDisabled keeps stale data indefinitely when age limits are zero or negative.
func TestMaxAgeDisabled(t *testing.T) {
	for _, age := range []time.Duration{0, -time.Second} {
		c, now := newAgeClient(t, Options{MaxAge: age})
		Set(c, "a", 1)
		now.Store(int64(24 * time.Hour))
		if state := Get[int](c, "a"); !state.HasData || state.Data != 1 || !state.Stale {
			t.Fatalf("disabled age %v = %+v", age, state)
		}
	}
}

// TestMaxAgeStaleData bounds retained data independently of its freshness deadline.
func TestMaxAgeStaleData(t *testing.T) {
	c, now := newAgeClient(t, Options{StaleTime: time.Minute, MaxAge: 5 * time.Minute})
	Set(c, "a", 1)
	now.Store(int64(time.Minute))
	if state := Get[int](c, "a"); !state.Stale || !state.HasData || state.Data != 1 {
		t.Fatalf("stale but usable = %+v", state)
	}
	now.Store(int64(5 * time.Minute))
	if state := Get[int](c, "a"); state.HasData || state.Data != 0 {
		t.Fatalf("over-age stale data = %+v", state)
	}
}

// TestMaxAgeSubscriptions expires data even for disabled consumers and notifies readers without loading.
func TestMaxAgeSubscriptions(t *testing.T) {
	c, now := newAgeClient(t, Options{StaleTime: time.Hour, MaxAge: time.Minute})
	Set(c, "a", 1)
	query := Query(c, "a", func(context.Context) (int, error) {
		t.Fatal("disabled consumer started a load")
		return 0, nil
	}, QueryOptions{Disable: true})
	t.Cleanup(query.Close)
	<-query.Updates()
	now.Store(int64(time.Minute))
	if state := query.Snapshot(); state.HasData || state.Data != 0 || state.Fetching {
		t.Fatalf("subscribed expiry = %+v", state)
	}
	if state := <-query.Updates(); state.HasData || state.Data != 0 {
		t.Fatalf("expiry notification = %+v", state)
	}
	if err := Set(c, "a", "wrong type"); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("expired binding = %v", err)
	}
}

// TestMaxAgeFailedRefresh removes old data at its original deadline while preserving the refresh error.
func TestMaxAgeFailedRefresh(t *testing.T) {
	c, now := newAgeClient(t, Options{MaxAge: time.Minute})
	Set(c, "a", "old")
	now.Store(int64(30 * time.Second))
	failure := errors.New("offline")
	fetch := func(context.Context) (string, error) { return "", failure }
	if _, err := Fetch(context.Background(), c, "a", fetch); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if state := Get[string](c, "a"); !state.HasData || state.Data != "old" || !errors.Is(state.Err, failure) {
		t.Fatalf("failed refresh before deadline = %+v", state)
	}
	now.Store(int64(time.Minute))
	if _, err := Fetch(context.Background(), c, "a", fetch); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if state := Get[string](c, "a"); state.HasData || state.Data != "" || !errors.Is(state.Err, failure) {
		t.Fatalf("failed refresh at deadline = %+v", state)
	}
}

// TestMaxAgeRefreshCompletion expires old data during shared work without canceling success or retaining it on failure.
func TestMaxAgeRefreshCompletion(t *testing.T) {
	for _, outcome := range []string{"success", "failure", "cancel"} {
		t.Run(outcome, func(t *testing.T) {
			c, now := newAgeClient(t, Options{MaxAge: time.Minute})
			Set(c, "a", "old")
			started, release := make(chan struct{}), make(chan struct{})
			finished := make(chan error, 1)
			failure := errors.New("offline")
			go func() {
				_, err := Fetch(context.Background(), c, "a", func(ctx context.Context) (string, error) {
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
				})
				finished <- err
			}()
			<-started
			now.Store(int64(time.Minute))
			if state := Get[string](c, "a"); state.HasData || state.Data != "" || !state.Fetching {
				t.Fatalf("expired data during refresh = %+v", state)
			}
			if outcome == "cancel" {
				c.Cancel("a")
			} else {
				close(release)
			}
			err := <-finished
			state := Get[string](c, "a")
			switch outcome {
			case "success":
				if err != nil || !state.HasData || state.Data != "new" {
					t.Fatalf("successful refresh = %+v, %v", state, err)
				}
			case "failure":
				if !errors.Is(err, failure) || state.HasData || state.Data != "" {
					t.Fatalf("failed refresh = %+v, %v", state, err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) || state.HasData || state.Data != "" {
					t.Fatalf("canceled refresh = %+v, %v", state, err)
				}
			}
		})
	}
}

// TestMaxAgeRenewal starts a new availability window only when a write or successful load installs data.
func TestMaxAgeRenewal(t *testing.T) {
	for _, action := range []string{"set", "fetch"} {
		t.Run(action, func(t *testing.T) {
			c, now := newAgeClient(t, Options{MaxAge: time.Minute})
			Set(c, "a", "old")
			now.Store(int64(30 * time.Second))
			if action == "set" {
				Set(c, "a", "new")
			} else if _, err := Fetch(context.Background(), c, "a", func(context.Context) (string, error) {
				return "new", nil
			}); err != nil {
				t.Fatal(err)
			}
			now.Store(int64(time.Minute))
			if state := Get[string](c, "a"); !state.HasData || state.Data != "new" {
				t.Fatalf("renewed data = %+v", state)
			}
			now.Store(int64(90 * time.Second))
			if state := Get[string](c, "a"); state.HasData {
				t.Fatalf("renewed deadline = %+v", state)
			}
		})
	}
}

// TestMaxAgeExplicitExpiry preserves earlier loader deadlines and caps later fallback deadlines.
func TestMaxAgeExplicitExpiry(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		for _, expiry := range []time.Duration{30 * time.Second, time.Hour} {
			c, now := newAgeClient(t, Options{MaxAge: time.Minute})
			var failure error
			if fallback {
				failure = errors.New("fallback")
			}
			_, err := FetchWithExpiry(context.Background(), c, "a", func(context.Context) (string, time.Time, error) {
				return "loaded", time.Unix(0, int64(expiry)), failure
			})
			if !errors.Is(err, failure) {
				t.Fatal(err)
			}
			deadline := min(expiry, time.Minute)
			if state := Get[string](c, "a"); !state.ExpiresAt.Equal(time.Unix(0, int64(deadline))) {
				t.Fatalf("bounded freshness = %+v", state)
			}
			now.Store(int64(time.Minute))
			if state := Get[string](c, "a"); state.HasData || !errors.Is(state.Err, failure) {
				t.Fatalf("expired explicit result = %+v", state)
			}
		}
	}
}
