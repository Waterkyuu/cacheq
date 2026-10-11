package cacheq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestBatcherExecution limits bulk callbacks after collection while allowing per-key Client loads to group.
func TestBatcherExecution(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var calls atomic.Int32
	b, err := NewBatcher(context.Background(), func(_ context.Context, keys []int) (map[int]BatchResult[int], error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		results := make(map[int]BatchResult[int], len(keys))
		for _, key := range keys {
			results[key] = BatchResult[int]{Data: key}
		}
		return results, nil
	}, BatchOptions{Wait: time.Hour, MaxBatchSize: 2, Executor: e})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	c := NewClient(Options{StaleTime: time.Hour, Retry: 3,
		RetryIf: func(error) bool { t.Error("batch admission failure retried"); return true },
	})
	t.Cleanup(c.Close)
	queries := make([]*QueryHandle[int], 0)
	for key := range 6 {
		h := Query(c, key, b.Fetcher(key))
		t.Cleanup(h.Close)
		queries = append(queries, h)
		if key == 1 {
			<-started
		}
		if key == 3 {
			awaitExecution(t, e, 1, 1)
		}
	}
	for _, h := range queries[4:] {
		state := awaitState(t, h.Updates(), func(s Snapshot[int]) bool { return s.Err != nil })
		if !errors.Is(state.Err, ErrQueueFull) {
			t.Fatal(state.Err)
		}
	}
	once.Do(func() { close(release) })
	for key, h := range queries[:4] {
		state := awaitState(t, h.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
		if state.Err != nil || state.Data != key {
			t.Fatalf("key %d = %+v", key, state)
		}
	}
	awaitExecution(t, e, 0, 0)
	if calls.Load() != 2 || e.Stats().Rejected != 1 || c.Stats().Retries != 0 {
		t.Fatalf("calls=%d budget=%+v client=%+v", calls.Load(), e.Stats(), c.Stats())
	}
}

// TestBatcherExecutionClose cancels queued callbacks without invoking them or leaking budget capacity.
func TestBatcherExecutionClose(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	release, _ := e.acquire(context.Background())
	defer release()
	b, err := NewBatcher(context.Background(), func(context.Context, []int) (map[int]BatchResult[int], error) {
		t.Error("closed queued callback executed")
		return nil, nil
	}, BatchOptions{MaxBatchSize: 1, Executor: e})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	finished := make(chan error, 1)
	go func() { _, loadErr := b.Load(context.Background(), 1); finished <- loadErr }()
	awaitExecution(t, e, 1, 1)
	b.Close()
	if err := <-finished; !errors.Is(err, ErrBatcherClosed) {
		t.Fatal(err)
	}
	awaitExecution(t, e, 1, 0)
}
