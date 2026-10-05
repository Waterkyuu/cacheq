package query

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestClientRetries covers recovery and exhaustion using an immediate deterministic retry policy.
func TestClientRetries(t *testing.T) {
	for _, succeed := range []bool{true, false} {
		client := NewClient[string, string](Options{
			StaleTime: time.Hour, Retry: 2, RetryDelay: func(int) time.Duration { return 0 },
		})
		calls := 0
		failure := errors.New("unavailable")
		value, err := client.Fetch(context.Background(), "a", func(context.Context) (string, error) {
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
	client := NewClient[string, string](Options{
		Retry: 3,
		RetryDelay: func(int) time.Duration {
			cancel()
			return time.Hour
		},
	})
	defer client.Close()
	_, err := client.Fetch(ctx, "a", func(context.Context) (string, error) { return "", errors.New("offline") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("retry cancellation = %v", err)
	}
}

// TestClientTimeout bounds all attempts and does not restart its own deadline as a canceled owner.
func TestClientTimeout(t *testing.T) {
	client := NewClient[string, string](Options{Timeout: time.Millisecond, Retry: 3})
	defer client.Close()
	_, err := client.Fetch(context.Background(), "a", func(ctx context.Context) (string, error) {
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
