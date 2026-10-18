package soroi

import (
	"context"
	"errors"
	"testing"
)

// TestMutationExecution shares Client capacity but releases it before cache synchronization starts new reads.
func TestMutationExecution(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	c := NewClient(Options{Executor: e})
	t.Cleanup(c.Close)
	h := Mutation(c, func(context.Context, int) (int, error) {
		if e.Stats().Active != 1 {
			t.Error("write did not acquire Client budget")
		}
		return 1, nil
	}, func(ctx context.Context, _, _ int) error {
		_, err := Fetch(ctx, c, "related", func(context.Context) (int, error) { return 2, nil })
		return err
	})
	t.Cleanup(h.Close)
	release, _ := e.acquire(context.Background())
	if _, err := h.Mutate(context.Background(), 0); !errors.Is(err, ErrQueueFull) {
		t.Fatal(err)
	}
	release()
	result, err := h.Mutate(context.Background(), 0)
	if err != nil || result.SyncErr != nil || result.Data != 1 {
		t.Fatalf("write and synchronization = %+v, %v", result, err)
	}
	awaitExecution(t, e, 0, 0)
}

// TestMutationExecutionOverride isolates a selected write from an unrelated saturated Client budget.
func TestMutationExecutionOverride(t *testing.T) {
	shared, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	separate, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	c := NewClient(Options{Executor: shared})
	t.Cleanup(c.Close)
	release, _ := shared.acquire(context.Background())
	defer release()
	h := Mutation(
		c,
		func(context.Context, int) (int, error) { return 3, nil },
		nil,
		MutationOptions{Executor: separate},
	)
	t.Cleanup(h.Close)
	if result, err := h.Mutate(context.Background(), 0); err != nil || result.Data != 3 {
		t.Fatalf("separate budget = %+v, %v", result, err)
	}
	awaitExecution(t, separate, 0, 0)
}

// TestMutationExecutionCancellation removes queued writes without invoking application code.
func TestMutationExecutionCancellation(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	c := NewClient(Options{Executor: e})
	t.Cleanup(c.Close)
	release, _ := e.acquire(context.Background())
	defer release()
	h := Mutation(
		c,
		func(context.Context, int) (int, error) { t.Error("canceled queued write executed"); return 0, nil },
		nil,
	)
	t.Cleanup(h.Close)
	finished := make(chan error, 1)
	go func() { _, err := h.Mutate(context.Background(), 0); finished <- err }()
	awaitExecution(t, e, 1, 1)
	h.Close()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	awaitExecution(t, e, 1, 0)
}
