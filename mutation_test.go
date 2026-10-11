package cacheq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestMutationSynchronization updates detail data and invalidates related list and summary queries once.
func TestMutationSynchronization(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour, Retry: 5})
	t.Cleanup(c.Close)
	Set(c, "detail", 1)
	Set(c, "list", []int{1})
	Set(c, "summary", 1)
	Set(c, "other-tenant", 1)
	var calls atomic.Int32
	list := Query(c, "list", func(context.Context) ([]int, error) { return []int{2}, nil })
	t.Cleanup(list.Close)
	h := Mutation(
		c,
		func(_ context.Context, input int) (int, error) { calls.Add(1); return input, nil },
		MutationOptions[int, int]{
			OnSuccess:  func(_ int, value int) error { return Set(c, "detail", value) },
			Invalidate: func(int, int) any { return []any{"list", "summary", "list"} },
		},
	)
	t.Cleanup(h.Close)
	if initial := <-h.Updates(); initial.Status != Idle || calls.Load() != 0 {
		t.Fatalf("initial = %+v, calls = %d", initial, calls.Load())
	}
	for range 2 {
		result, err := h.Mutate(context.Background(), 2)
		if err != nil || result.SyncErr != nil || result.Data != 2 {
			t.Fatalf("mutation = %+v, %v", result, err)
		}
		awaitState(t, list.Updates(), func(s Snapshot[[]int]) bool { return s.HasData && !s.Fetching })
	}
	if calls.Load() != 2 || Get[int](c, "detail").Data != 2 || !Get[int](c, "summary").Stale {
		t.Fatalf("calls=%d detail=%+v summary=%+v", calls.Load(), Get[int](c, "detail"), Get[int](c, "summary"))
	}
	if Get[int](c, "other-tenant").Stale {
		t.Fatal("unrelated query invalidated")
	}
	if state := <-h.Updates(); state.Status != Success || !state.HasData || state.Data != 2 {
		t.Fatalf("completed = %+v", state)
	}
}

// TestMutationFailure never retries writes or synchronizes cache after a business error.
func TestMutationFailure(t *testing.T) {
	c := NewClient(Options{Retry: 10, RetryDelay: func(int) time.Duration { return 0 }})
	t.Cleanup(c.Close)
	failure := errors.New("write rejected")
	calls := 0
	h := Mutation(c, func(context.Context, int) (int, error) { calls++; return 0, failure }, MutationOptions[int, int]{
		OnSuccess:  func(int, int) error { t.Fatal("unexpected synchronization"); return nil },
		Invalidate: func(int, int) any { t.Fatal("unexpected invalidation"); return "key" },
	})
	t.Cleanup(h.Close)
	result, err := h.Mutate(context.Background(), 1)
	if !errors.Is(err, failure) || result.SyncErr != nil || calls != 1 {
		t.Fatalf("failed write = %+v, %v, calls=%d", result, err, calls)
	}
	if state := h.Snapshot(); state.Status != Error || state.HasData || !errors.Is(state.Err, failure) {
		t.Fatalf("failure state = %+v", state)
	}
}

// TestMutationSyncFailure distinguishes committed writes from errors in either synchronization step.
func TestMutationSyncFailure(t *testing.T) {
	c := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(c.Close)
	Set(c, "list", 1)
	failure := errors.New("local update rejected")
	h := Mutation(c, func(context.Context, int) (int, error) { return 0, nil }, MutationOptions[int, int]{
		OnSuccess:         func(int, int) error { return failure },
		Invalidate:        func(int, int) any { return func(key any) bool { return key == "list" } },
		InvalidateOptions: InvalidateOptions{Refetch: RefetchNone},
	})
	t.Cleanup(h.Close)
	result, err := h.Mutate(context.Background(), 1)
	if err != nil || !errors.Is(result.SyncErr, failure) || !Get[int](c, "list").Stale {
		t.Fatalf("synchronization = %+v, %v", result, err)
	}
	state := h.Snapshot()
	if state.Status != Success || !state.HasData || state.Err != nil || !errors.Is(state.SyncErr, failure) {
		t.Fatalf("committed state = %+v", state)
	}
	invalid := Mutation(c, func(context.Context, int) (int, error) { return 7, nil }, MutationOptions[int, int]{
		OnSuccess:  func(int, int) error { return failure },
		Invalidate: func(int, int) any { return []any{"list", []int{1}} },
	})
	t.Cleanup(invalid.Close)
	result, err = invalid.Mutate(context.Background(), 1)
	if err != nil || !errors.Is(result.SyncErr, ErrInvalidKey) || !errors.Is(result.SyncErr, failure) {
		t.Fatalf("combined synchronization errors = %+v, %v", result, err)
	}
}

// TestMutationCancellation bounds ownership and rejects overlapping calls without corrupting active state.
func TestMutationCancellation(t *testing.T) {
	for _, stop := range []string{"caller", "handle", "client"} {
		t.Run(stop, func(t *testing.T) {
			c := NewClient(Options{})
			t.Cleanup(c.Close)
			started := make(chan struct{})
			h := Mutation(c, func(ctx context.Context, _ int) (int, error) {
				close(started)
				<-ctx.Done()
				return 0, ctx.Err()
			})
			t.Cleanup(h.Close)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() { _, err := h.Mutate(ctx, 1); finished <- err }()
			<-started
			if _, err := h.Mutate(context.Background(), 2); !errors.Is(err, ErrMutationBusy) {
				t.Fatal(err)
			}
			if h.Snapshot().Status != Pending {
				t.Fatal("rejected call changed active state")
			}
			switch stop {
			case "caller":
				cancel()
			case "handle":
				h.Close()
			case "client":
				c.Close()
			}
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if stop != "caller" {
				for range h.Updates() {
				}
			}
		})
	}
}

// TestMutationLateSuccess preserves confirmed commits even when cancellation or closure races with success.
func TestMutationLateSuccess(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "handle", true: "client"}[closeClient], func(t *testing.T) {
			c := NewClient(Options{})
			t.Cleanup(c.Close)
			started, release := make(chan struct{}), make(chan struct{})
			h := Mutation(c, func(context.Context, int) (int, error) {
				close(started)
				<-release
				return 42, nil
			}, MutationOptions[int, int]{OnSuccess: func(_ int, value int) error { return Set(c, "key", value) }})
			t.Cleanup(h.Close)
			finished := make(chan MutationResult[int], 1)
			go func() {
				result, err := h.Mutate(context.Background(), 1)
				if err != nil {
					t.Errorf("confirmed write reported as failed: %v", err)
				}
				finished <- result
			}()
			<-started
			if closeClient {
				c.Close()
			} else {
				h.Close()
			}
			close(release)
			result := <-finished
			if result.Data != 42 || h.Snapshot().Status != Success {
				t.Fatalf("late success = %+v, %+v", result, h.Snapshot())
			}
			if closeClient {
				if !errors.Is(result.SyncErr, ErrClosed) {
					t.Fatal(result.SyncErr)
				}
			} else if result.SyncErr != nil || Get[int](c, "key").Data != 42 {
				t.Fatalf("late synchronization = %+v", result)
			}
		})
	}
}

// TestMutationValidation rejects invalid handles and already-canceled calls without executing business code.
func TestMutationValidation(t *testing.T) {
	c := NewClient(Options{})
	t.Cleanup(c.Close)
	nilHandle := Mutation[int, int](c, nil)
	if _, err := nilHandle.Mutate(context.Background(), 1); !errors.Is(err, ErrNoMutator) {
		t.Fatal(err)
	}
	nilHandle.Close()
	never := func(context.Context, int) (int, error) { t.Fatal("unexpected write"); return 0, nil }
	h := Mutation(c, never)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.Mutate(ctx, 1); !errors.Is(err, context.Canceled) || h.Snapshot().Status != Idle {
		t.Fatalf("pre-canceled mutation = %v", err)
	}
	h.Close()
	h.Close()
	if _, err := h.Mutate(context.Background(), 1); !errors.Is(err, ErrMutationClosed) {
		t.Fatal(err)
	}
	c.Close()
	closed := Mutation(c, never)
	if _, err := closed.Mutate(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
