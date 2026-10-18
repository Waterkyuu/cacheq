package cacheq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestExecutionSharing limits distinct loads without charging same-key joiners or cache hits.
func TestExecutionSharing(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	c := NewClient(Options{Executor: e, StaleTime: time.Hour, Retry: 3,
		RetryIf: func(error) bool { t.Error("admission failure retried"); return true },
	})
	t.Cleanup(c.Close)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	load := func(context.Context) (int, error) { close(started); <-release; return 1, nil }
	first := Query(c, "first", load)
	t.Cleanup(first.Close)
	<-started
	queuedCalls := atomic.Int32{}
	queued := Query(
		c,
		"queued",
		func(context.Context) (int, error) { queuedCalls.Add(1); return 2, nil },
	)
	t.Cleanup(queued.Close)
	awaitExecution(t, e, 1, 1)
	joined := make(chan error, 1)
	go func() { _, err := Fetch(context.Background(), c, "first", load); joined <- err }()
	awaitStats(t, c, func(s Stats) bool { return s.MergedRequests == 1 })
	if _, err := Fetch(context.Background(), c, "overload", load); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	Set(c, "cached", 9)
	if value, err := Fetch[int](context.Background(), c, "cached", nil); err != nil || value != 9 {
		t.Fatalf("cached value = %d, %v", value, err)
	}
	c.Cancel("queued")
	awaitExecution(t, e, 1, 0)
	once.Do(func() { close(release) })
	if err := <-joined; err != nil {
		t.Fatal(err)
	}
	awaitExecution(t, e, 0, 0)
	if queuedCalls.Load() != 0 || e.Stats().Rejected != 1 {
		t.Fatalf("queued calls = %d, budget = %+v", queuedCalls.Load(), e.Stats())
	}
}

// TestExecutionRetryRelease returns capacity before application backoff callbacks and subsequent retries.
func TestExecutionRetryRelease(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	other := NewClient(Options{Executor: e})
	t.Cleanup(other.Close)
	c := NewClient(Options{Executor: e, Retry: 1, RetryDelay: func(int) time.Duration {
		if e.Stats().Active != 0 {
			t.Error("retry delay retained execution capacity")
		}
		if _, err := Fetch(
			context.Background(),
			other,
			"other",
			func(context.Context) (int, error) { return 2, nil },
		); err != nil {
			t.Error(err)
		}
		return 0
	}})
	t.Cleanup(c.Close)
	calls := 0
	value, err := Fetch(context.Background(), c, "key", func(context.Context) (int, error) {
		calls++
		if calls == 1 {
			return 0, errors.New("transient")
		}
		return 1, nil
	})
	if err != nil || value != 1 || calls != 2 || c.Stats().Retries != 1 {
		t.Fatalf("retry = %d, %v, calls=%d, %+v", value, err, calls, c.Stats())
	}
	awaitExecution(t, e, 0, 0)
}

// TestExecutionClientLifetime cancels queued work without releasing slots still used by non-cooperative loaders.
func TestExecutionClientLifetime(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	c := NewClient(Options{Executor: e})
	t.Cleanup(c.Close)
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	first := Query(
		c,
		"first",
		func(context.Context) (int, error) { close(started); <-release; return 1, nil },
	)
	t.Cleanup(first.Close)
	<-started
	other := NewClient(Options{Executor: e})
	t.Cleanup(other.Close)
	queued := Query(
		other,
		"queued",
		func(context.Context) (int, error) { t.Error("canceled loader ran"); return 0, nil },
	)
	t.Cleanup(queued.Close)
	awaitExecution(t, e, 1, 1)
	other.Close()
	awaitExecution(t, e, 1, 0)
	c.Close()
	if e.Stats().Active != 1 {
		t.Fatal("cancellation released a still-running callback")
	}
	once.Do(func() { close(release) })
	awaitExecution(t, e, 0, 0)
}
