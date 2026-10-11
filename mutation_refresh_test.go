package cacheq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// mutationRefreshResult transports both the committed result and the separate business error.
type mutationRefreshResult struct {
	// result retains data and post-commit synchronization errors.
	result MutationResult[int]
	// err reports business failure separately from result.SyncErr.
	err error
}

// TestMutationWaitForRefresh stays Pending through the post-invalidation load and releases execution capacity.
func TestMutationWaitForRefresh(t *testing.T) {
	budget, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	c := newInvalidationClient(t)
	c.options.Executor = budget
	events := subscribeTestEvents(t, c, EventOptions{Buffer: 64})
	var backend, calls atomic.Int32
	backend.Store(1)
	started := make(chan int32, 2)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	q := Query(c, "key", func(ctx context.Context) (int, error) {
		value := int(backend.Load())
		call := calls.Add(1)
		started <- call
		select {
		case <-releases[call-1]:
			return value, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}, QueryOptions{DisableExecutor: true})
	t.Cleanup(q.Close)
	<-started
	h := Mutation(c, func(_ context.Context, value int) (int, error) {
		backend.Store(int32(value))
		return value, nil
	}, MutationOptions[int, int]{
		Invalidate:     func(int, int) any { return "key" },
		WaitForRefresh: true,
	})
	t.Cleanup(h.Close)
	done := make(chan mutationRefreshResult, 1)
	go func() {
		result, err := h.Mutate(context.Background(), 2)
		done <- mutationRefreshResult{result: result, err: err}
	}()
	awaitInvalidation(t, c, events)
	if h.Snapshot().Status != Pending || budget.Stats().Active != 0 {
		t.Fatal("synchronization did not remain pending or retained write capacity")
	}
	if _, err := h.Mutate(context.Background(), 3); !errors.Is(err, ErrMutationBusy) {
		t.Fatal(err)
	}
	close(releases[0])
	<-started
	select {
	case result := <-done:
		t.Fatalf("mutation completed before follow-up: %+v", result)
	default:
	}
	close(releases[1])
	result := <-done
	if result.err != nil || result.result.SyncErr != nil || result.result.Data != 2 || q.Snapshot().Data != 2 {
		t.Fatalf("synchronized mutation = %+v", result)
	}
	if h.Snapshot().Status != Success || calls.Load() != 2 {
		t.Fatal("completed mutation has incorrect state")
	}
}

// TestMutationRefreshFailure separates a successful write from local update and remote refresh errors.
func TestMutationRefreshFailure(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "key", 1)
	failure := errors.New("refresh failed")
	updateFailure := errors.New("local update failed")
	q := Query(c, "key", func(context.Context) (int, error) { return 0, failure })
	t.Cleanup(q.Close)
	writes := 0
	h := Mutation(c, func(context.Context, int) (int, error) { writes++; return 2, nil }, MutationOptions[int, int]{
		OnSuccess:      func(int, int) error { return updateFailure },
		Invalidate:     func(int, int) any { return "key" },
		WaitForRefresh: true,
	})
	t.Cleanup(h.Close)
	result, err := h.Mutate(context.Background(), 2)
	if err != nil || result.Data != 2 || !errors.Is(result.SyncErr, failure) ||
		!errors.Is(result.SyncErr, updateFailure) {
		t.Fatalf("post-commit failure = %+v, %v", result, err)
	}
	if state := h.Snapshot(); state.Status != Success || state.Err != nil || !errors.Is(state.SyncErr, failure) {
		t.Fatalf("post-commit state = %+v", state)
	}
	if writes != 1 {
		t.Fatalf("write count = %d", writes)
	}
}

// TestMutationRefreshCancellation stops synchronization waiting without repeating or undoing committed writes.
func TestMutationRefreshCancellation(t *testing.T) {
	for _, stop := range []string{"caller", "handle", "client"} {
		t.Run(stop, func(t *testing.T) {
			c := newInvalidationClient(t)
			Set(c, "key", 1)
			started, release := make(chan struct{}), make(chan struct{})
			q := Query(c, "key", func(ctx context.Context) (int, error) {
				close(started)
				select {
				case <-release:
					return 2, nil
				case <-ctx.Done():
					return 0, ctx.Err()
				}
			})
			t.Cleanup(q.Close)
			h := Mutation(c, func(context.Context, int) (int, error) { return 2, nil }, MutationOptions[int, int]{
				Invalidate:     func(int, int) any { return "key" },
				WaitForRefresh: true,
			})
			t.Cleanup(h.Close)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan mutationRefreshResult, 1)
			go func() {
				result, err := h.Mutate(ctx, 2)
				done <- mutationRefreshResult{result: result, err: err}
			}()
			<-started
			want := error(context.Canceled)
			switch stop {
			case "caller":
				cancel()
			case "handle":
				h.Close()
			case "client":
				c.Close()
				want = ErrClosed
			}
			result := <-done
			matches := errors.Is(result.result.SyncErr, want)
			if stop == "client" {
				// Client closure can settle the receipt or cancel the handle context first.
				matches = matches || errors.Is(result.result.SyncErr, context.Canceled)
			}
			if result.err != nil || result.result.Data != 2 || !matches {
				t.Fatalf("canceled synchronization = %+v", result)
			}
			if h.Snapshot().Status != Success {
				t.Fatal("committed write became an error")
			}
			close(release)
			if stop != "client" {
				awaitState(t, q.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
			}
		})
	}
}

// TestMutationRefreshLateSuccess still invalidates after a committed write races with context cancellation.
func TestMutationRefreshLateSuccess(t *testing.T) {
	c := newInvalidationClient(t)
	Set(c, "key", 1)
	started, release := make(chan struct{}), make(chan struct{})
	q := Query(c, "key", func(ctx context.Context) (int, error) {
		close(started)
		select {
		case <-release:
			return 2, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	})
	t.Cleanup(q.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := Mutation(c, func(context.Context, int) (int, error) {
		cancel()
		return 2, nil
	}, MutationOptions[int, int]{Invalidate: func(int, int) any { return "key" }, WaitForRefresh: true})
	t.Cleanup(h.Close)
	result, err := h.Mutate(ctx, 2)
	if err != nil || result.Data != 2 || !errors.Is(result.SyncErr, context.Canceled) {
		t.Fatalf("late commit = %+v, %v", result, err)
	}
	<-started
	close(release)
	awaitState(t, q.Updates(), func(s Snapshot[int]) bool { return s.Data == 2 && !s.Fetching })
}
