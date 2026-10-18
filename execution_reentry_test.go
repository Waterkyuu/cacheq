package soroi

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestBatcherExecutionReentry rejects nested budgeting before collection, regardless of available capacity.
func TestBatcherExecutionReentry(t *testing.T) {
	for _, concurrent := range []int{1, 2} {
		for _, queued := range []int{0, 1} {
			for _, size := range []int{1, 2} {
				t.Run(fmt.Sprintf("concurrent=%d/queued=%d/batch=%d", concurrent, queued, size), func(t *testing.T) {
					e, err := NewExecutor(ExecutionOptions{MaxConcurrent: concurrent, MaxQueued: queued})
					if err != nil {
						t.Fatal(err)
					}
					b, err := NewBatcher(
						context.Background(),
						func(context.Context, []int) (map[int]BatchResult[int], error) {
							t.Error("recursive budget invoked the backend")
							return nil, nil
						},
						BatchOptions{Wait: time.Hour, MaxBatchSize: size, Executor: e},
					)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(b.Close)
					c := NewClient(Options{Executor: e, Retry: 3,
						RetryIf: func(error) bool { t.Error("recursive budget was retried"); return true },
					})
					t.Cleanup(c.Close)
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()
					_, err = Fetch(ctx, c, "key", b.Fetcher(1))
					if !errors.Is(err, ErrExecutionReentry) {
						t.Fatalf("nested load = %v, want ErrExecutionReentry", err)
					}
					if stats := e.Stats(); stats.Active != 0 || stats.Queued != 0 || stats.Rejected != 0 {
						t.Fatalf("recursive acquisition changed capacity: %+v", stats)
					}
					if c.Stats().Retries != 0 {
						t.Fatal("recursive acquisition was retried")
					}
				})
			}
		}
	}
}

// TestExecutorContextOwnership isolates distinct budgets and clears ownership when callbacks return.
func TestExecutorContextOwnership(t *testing.T) {
	e, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, release, err := e.acquireContext(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { release() }()
	if _, err := e.acquire(ctx); !errors.Is(err, ErrExecutionReentry) {
		t.Fatalf("active callback acquisition = %v", err)
	}
	freeOther, err := other.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	freeOther()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := e.acquire(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost precedence: %v", err)
	}
	// Release once before reusing the escaped callback context.
	release()
	release = func() {}
	free, err := e.acquire(ctx)
	if err != nil {
		t.Fatalf("completed callback retained ownership: %v", err)
	}
	free()
	awaitExecution(t, e, 0, 0)
}

// TestExecutionReentryCallbacks propagates budget ownership through query, mutation, and bulk callbacks.
func TestExecutionReentryCallbacks(t *testing.T) {
	for _, boundary := range []string{"query", "mutation", "batch"} {
		t.Run(boundary, func(t *testing.T) {
			e, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
			if err != nil {
				t.Fatal(err)
			}
			c := NewClient(Options{Executor: e, Retry: 3,
				RetryIf: func(error) bool { t.Error("recursive budget was retried"); return true },
			})
			t.Cleanup(c.Close)
			load := func(ctx context.Context) (int, error) {
				_, err := Fetch(ctx, c, "inner", func(context.Context) (int, error) {
					t.Error("recursive query executed")
					return 1, nil
				})
				return 0, fmt.Errorf("nested query: %w", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			switch boundary {
			case "query":
				_, err = Fetch(ctx, c, "outer", load)
			case "mutation":
				h := Mutation(
					c,
					func(ctx context.Context, _ int) (int, error) { return load(ctx) },
					nil,
				)
				t.Cleanup(h.Close)
				_, err = h.Mutate(ctx, 1)
			case "batch":
				b, batchErr := NewBatcher(
					context.Background(),
					func(ctx context.Context, _ []int) (map[int]BatchResult[int], error) {
						_, err := load(ctx)
						return nil, err
					},
					BatchOptions{MaxBatchSize: 1, Executor: e},
				)
				if batchErr != nil {
					t.Fatal(batchErr)
				}
				t.Cleanup(b.Close)
				_, err = b.Load(ctx, 1)
			}
			if !errors.Is(err, ErrExecutionReentry) {
				t.Fatalf("%s callback = %v, want ErrExecutionReentry", boundary, err)
			}
			awaitExecution(t, e, 0, 0)
			if e.Stats().Rejected != 0 || c.Stats().Retries != 0 {
				t.Fatal("recursive execution counted as overload or was retried")
			}
		})
	}
}
