package soroi

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// awaitExecution waits for observable scheduler state with a watchdog rather than scheduling sleeps.
func awaitExecution(t *testing.T, e *Executor, active, queued int) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		state := e.Stats()
		if state.Active == active && state.Queued == queued {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("execution state = %+v, want active=%d queued=%d", state, active, queued)
		default:
			runtime.Gosched()
		}
	}
}

// TestExecutorValidation rejects unusable budgets and preserves cancellation for unrestricted execution.
func TestExecutorValidation(t *testing.T) {
	for _, options := range []ExecutionOptions{{}, {MaxConcurrent: -1}, {MaxConcurrent: 1, MaxQueued: -1}} {
		if _, err := NewExecutor(options); !errors.Is(err, ErrInvalidExecutionOptions) {
			t.Fatal(err)
		}
	}
	var unrestricted *Executor
	release, err := unrestricted.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := unrestricted.acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

// TestExecutorFIFO enforces concurrency and queue bounds while admitting live callers in arrival order.
func TestExecutorFIFO(t *testing.T) {
	e, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release, err := e.acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	order := make(chan int, 2)
	step := make(chan struct{})
	for i := range 2 {
		go func() {
			free, acquireErr := e.acquire(ctx)
			if acquireErr != nil {
				return
			}
			defer free()
			order <- i
			select {
			case <-step:
			case <-ctx.Done():
			}
		}()
		awaitExecution(t, e, 1, i+1)
	}
	if _, err := e.acquire(ctx); !errors.Is(err, ErrQueueFull) || e.Stats().Rejected != 1 {
		t.Fatalf("overload = %v, %+v", err, e.Stats())
	}
	release()
	if first := <-order; first != 0 {
		t.Fatalf("first caller = %d", first)
	}
	step <- struct{}{}
	if second := <-order; second != 1 {
		t.Fatalf("second caller = %d", second)
	}
	step <- struct{}{}
	awaitExecution(t, e, 0, 0)
}

// TestExecutorCancelHandoff never leaks or double-releases a slot when admission races with cancellation.
func TestExecutorCancelHandoff(t *testing.T) {
	e, _ := NewExecutor(ExecutionOptions{MaxConcurrent: 1, MaxQueued: 1})
	for range 100 {
		release, err := e.acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			free, acquireErr := e.acquire(ctx)
			if acquireErr == nil {
				free()
			} else if !errors.Is(acquireErr, context.Canceled) {
				t.Errorf("canceled waiter = %v", acquireErr)
			}
			close(done)
		}()
		awaitExecution(t, e, 1, 1)
		var racers sync.WaitGroup
		racers.Add(2)
		go func() { defer racers.Done(); cancel() }()
		go func() { defer racers.Done(); release() }()
		racers.Wait()
		<-done
		awaitExecution(t, e, 0, 0)
	}
}
