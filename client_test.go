package query

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCacheExpiration refreshes at the exact expiration boundary while isolating keys.
func TestCacheExpiration(t *testing.T) {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	cache := NewClient[string, int](Options{Clock: func() time.Time { return now }})
	calls := 0
	load := func(context.Context) (int, time.Time, error) {
		calls++
		return calls, now.Add(time.Hour), nil
	}
	for _, key := range []string{"a", "a", "b"} {
		if _, err := cache.FetchWithExpiry(context.Background(), key, load); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("loads = %d, want one per key", calls)
	}
	now = now.Add(time.Hour)
	if got, err := cache.FetchWithExpiry(context.Background(), "a", load); got != 3 || err != nil {
		t.Fatalf("expired result = %d, %v", got, err)
	}
}

// TestCacheRetryExpiration reuses an explicitly cached failure and retries when its delay expires.
func TestCacheRetryExpiration(t *testing.T) {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	cache := NewClient[string, string](Options{Clock: func() time.Time { return now }})
	failure := errors.New("unavailable")
	calls := 0
	load := func(context.Context) (string, time.Time, error) {
		calls++
		return "offline", now.Add(time.Minute), failure
	}
	for range 2 {
		if value, err := cache.FetchWithExpiry(
			context.Background(),
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
	_, _ = cache.FetchWithExpiry(context.Background(), "a", load)
	if calls != 2 {
		t.Fatalf("failure was not retried: loads = %d", calls)
	}
}

// TestCacheZeroExpiration leaves uncached results eligible for the next load.
func TestCacheZeroExpiration(t *testing.T) {
	cache := NewClient[string, int](Options{})
	calls := 0
	load := func(context.Context) (int, time.Time, error) {
		calls++
		return calls, time.Time{}, nil
	}
	for want := 1; want <= 2; want++ {
		if value, err := cache.FetchWithExpiry(context.Background(), "a", load); value != want || err != nil {
			t.Fatalf("uncached value = %d, %v; want %d", value, err, want)
		}
	}
}

// TestCacheConcurrentLoads merges requests for one key without blocking other keys.
func TestCacheConcurrentLoads(t *testing.T) {
	cache := NewClient[string, string](Options{})
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
			if value, err := cache.FetchWithExpiry(context.Background(), "a", load); value != "shared" || err != nil {
				t.Errorf("shared result = %q, %v", value, err)
			}
		}()
	}
	<-started
	other := make(chan string, 1)
	go func() {
		value, _ := cache.FetchWithExpiry(context.Background(), "b", func(context.Context) (string, time.Time, error) {
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
	cache := NewClient[string, string](Options{})
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	load := func(context.Context) (string, time.Time, error) {
		close(started)
		<-release
		return "loaded", time.Now().Add(time.Hour), nil
	}
	go func() {
		_, err := cache.FetchWithExpiry(context.Background(), "a", load)
		finished <- err
	}()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() {
		_, err := cache.FetchWithExpiry(ctx, "a", load)
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
	if value, err := cache.FetchWithExpiry(context.Background(), "a", load); value != "loaded" || err != nil {
		t.Fatalf("waiter discarded loaded value: %q, %v", value, err)
	}
}

// TestCacheLoaderCancellation allows another caller to retry and never caches a canceled load.
func TestCacheLoaderCancellation(t *testing.T) {
	cache := NewClient[string, string](Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := cache.FetchWithExpiry(ctx, "a", func(ctx context.Context) (string, time.Time, error) {
			close(started)
			<-ctx.Done()
			return "canceled", time.Now().Add(time.Hour), nil
		})
		first <- err
	}()
	<-started
	second := make(chan error, 1)
	go func() {
		value, err := cache.FetchWithExpiry(
			context.Background(),
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
	if _, err := cache.FetchWithExpiry(ctx, "a", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled caller used a cached result: %v", err)
	}
}
