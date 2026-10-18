package soroi

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrInvalidExecutionOptions indicates a non-positive concurrency limit or a negative queue capacity.
var ErrInvalidExecutionOptions = errors.New("invalid execution options")

// ErrQueueFull indicates an execution budget has no active or queued capacity for new work.
// Client retries never retry this admission failure.
var ErrQueueFull = errors.New("execution queue is full")

// ErrExecutionReentry indicates work waiting on an Executor already held by its caller's context.
// Apply the budget at the backend boundary instead of acquiring it at both layers.
var ErrExecutionReentry = errors.New("execution budget is already held by caller")

// ExecutionOptions bounds concurrently running callbacks and callers waiting to start.
type ExecutionOptions struct {
	// MaxConcurrent is the positive maximum number of callbacks executing under this budget.
	MaxConcurrent int
	// MaxQueued is the non-negative maximum waiting count; zero rejects work when all slots are busy.
	MaxQueued int
}

// ExecutionStats reports current budget use and cumulative overload rejections.
type ExecutionStats struct {
	// Active counts acquired slots, including callbacks that have not returned after cancellation.
	Active int
	// Queued counts waiting callers, including canceled callers until their cleanup runs.
	Queued int
	// Rejected counts arrivals rejected with ErrQueueFull.
	Rejected uint64
}

// Executor is an explicitly shared concurrency budget with a bounded first-in-first-out queue.
// Construct it with NewExecutor. It owns no goroutines or timers and needs no Close.
// Recursive acquisition through a callback's context returns ErrExecutionReentry.
type Executor struct {
	// mu protects admission, queued handoffs, and usage statistics.
	mu sync.Mutex
	// options holds immutable validated capacity limits.
	options ExecutionOptions
	// active counts slots acquired by callbacks or handed to queued callers.
	active int
	// queue orders waiting callers; cancellation removes entries under mu.
	queue []*executionWaiter
	// rejected accumulates overload failures without counting canceled callers.
	rejected uint64
}

// executionWaiter owns a queue position until cancellation or slot handoff.
type executionWaiter struct {
	// ctx allows slot release to skip callers whose cancellation cleanup has not run yet.
	ctx context.Context
	// ready closes when this caller owns an execution slot.
	ready chan struct{}
	// granted distinguishes cancellation before admission from cancellation after a handoff.
	granted bool
}

// executionContextKey identifies one budget's active callback ownership in a context chain.
type executionContextKey struct {
	// executor keeps ownership independent for distinct budgets used by nested callbacks.
	executor *Executor
}

// NewExecutor validates capacities and constructs a reusable budget independent of any Client lifetime.
func NewExecutor(options ExecutionOptions) (*Executor, error) {
	if options.MaxConcurrent <= 0 || options.MaxQueued < 0 {
		return nil, ErrInvalidExecutionOptions
	}
	return &Executor{options: options, queue: make([]*executionWaiter, 0)}, nil
}

// Stats returns a consistent usage snapshot without affecting queue order.
func (e *Executor) Stats() ExecutionStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return ExecutionStats{Active: e.active, Queued: len(e.queue), Rejected: e.rejected}
}

// acquire reserves one slot until the returned release function is called exactly once.
// A nil budget preserves unrestricted execution. Cancellation never releases a still-running callback's slot.
func (e *Executor) acquire(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if e.heldBy(ctx) {
		return nil, ErrExecutionReentry
	}
	if e == nil {
		return func() {}, nil
	}
	e.mu.Lock()
	if e.active < e.options.MaxConcurrent {
		e.active++
		e.mu.Unlock()
		return e.release, nil
	}
	if len(e.queue) >= e.options.MaxQueued {
		e.rejected++
		e.mu.Unlock()
		return nil, ErrQueueFull
	}
	waiter := &executionWaiter{ctx: ctx, ready: make(chan struct{})}
	e.queue = append(e.queue, waiter)
	e.mu.Unlock()
	select {
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			e.release()
			return nil, err
		}
		return e.release, nil
	case <-ctx.Done():
		e.mu.Lock()
		if waiter.granted {
			e.releaseLocked()
		} else {
			for i, queued := range e.queue {
				if queued == waiter {
					e.removeWaiterLocked(i)
					break
				}
			}
		}
		e.mu.Unlock()
		return nil, ctx.Err()
	}
}

// acquireContext marks slot ownership for nested operations until the callback releases capacity.
func (e *Executor) acquireContext(ctx context.Context) (context.Context, func(), error) {
	release, err := e.acquire(ctx)
	if err != nil || e == nil {
		return ctx, release, err
	}
	held := &atomic.Bool{}
	held.Store(true)
	ctx = context.WithValue(ctx, executionContextKey{executor: e}, held)
	return ctx, func() {
		// Callback contexts may escape; completed callbacks must not leave stale ownership markers.
		held.Store(false)
		release()
	}, nil
}

// heldBy reports whether a callback in ctx's ancestry still owns capacity from this budget.
func (e *Executor) heldBy(ctx context.Context) bool {
	if e == nil {
		return false
	}
	held, _ := ctx.Value(executionContextKey{executor: e}).(*atomic.Bool)
	return held != nil && held.Load()
}

// release hands a slot to the next live waiter or returns it to unused capacity.
func (e *Executor) release() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.releaseLocked()
}

// releaseLocked maintains the active count across cancellation races and FIFO handoffs.
func (e *Executor) releaseLocked() {
	for len(e.queue) > 0 {
		waiter := e.queue[0]
		e.removeWaiterLocked(0)
		if waiter.ctx.Err() != nil {
			continue
		}
		waiter.granted = true
		close(waiter.ready)
		return
	}
	e.active--
}

// removeWaiterLocked releases the retained context and channel references for one queue position.
func (e *Executor) removeWaiterLocked(index int) {
	copy(e.queue[index:], e.queue[index+1:])
	e.queue[len(e.queue)-1] = nil
	e.queue = e.queue[:len(e.queue)-1]
}
