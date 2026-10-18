package cacheq

import (
	"context"
	"sync"
)

// MutationHandle owns write state and a latest-state subscription until Close or Client shutdown.
// One execution at a time is accepted per handle; independent handles are not mutually serialized.
type MutationHandle[I, V any] struct {
	// mu protects state, execution admission, and channel closure.
	mu sync.Mutex
	// client owns associated cached queries and the parent lifetime.
	client *Client
	// ctx bounds this handle's lifetime independently of any one caller.
	ctx context.Context
	// cancel stops work when the handle closes.
	cancel context.CancelFunc
	// stopLifetime detaches the parent's automatic Close callback.
	stopLifetime func() bool
	// write performs one application-owned write without automatic retries.
	write Mutator[I, V]
	// onSuccess synchronizes cache state after a committed write, or is nil to skip synchronization.
	onSuccess func(context.Context, V, I) error
	// options holds the copied write execution policy.
	options MutationOptions
	// state retains the latest execution snapshot under mu.
	state MutationSnapshot[V]
	// updates retains the newest unread snapshot without blocking execution.
	updates chan MutationSnapshot[V]
	// running rejects overlapping calls, including during post-write synchronization.
	running bool
	// closed prevents new executions after subscription release.
	closed bool
	// err preserves construction failure for subsequent calls.
	err error
}

// Mutation constructs an idle handle without executing a write, with zero or one execution configuration.
// Extra configurations are reported as ErrTooManyOptions through the handle without writing or synchronizing.
// Input and result types are inferred from write and onSuccess and checked together at compile time.
// onSuccess receives the committed result and original input once outside internal locks; nil skips it.
// Mutate stays Pending through synchronization and reports callback errors through SyncErr.
// Late write success still invokes onSuccess with the possibly canceled context.
// Captured state and input/result references remain application-owned and require synchronization.
// Nil writes and closed clients are reported through Snapshot, Updates, and Mutate.
// The caller owns Close; Client closure also cancels execution and closes Updates asynchronously.
func Mutation[I, V any](
	client *Client,
	write Mutator[I, V],
	onSuccess func(context.Context, V, I) error,
	options ...MutationOptions,
) *MutationHandle[I, V] {
	option, optionErr := singleOptions(options)
	ctx, cancel := context.WithCancel(client.ctx) // #nosec G118 -- Close owns this handle's lifetime.
	h := &MutationHandle[I, V]{
		client: client, ctx: ctx, cancel: cancel, write: write, onSuccess: onSuccess, options: option, err: optionErr,
		updates: make(chan MutationSnapshot[V], 1),
	}
	if h.options.Executor == nil {
		h.options.Executor = client.options.Executor
	}
	if h.err == nil {
		if client.ctx.Err() != nil {
			h.err = ErrClosed
		} else if write == nil {
			h.err = ErrNoMutator
		}
	}
	if h.err != nil {
		h.state = MutationSnapshot[V]{Status: Error, Err: h.err}
		h.updates <- h.state
		close(h.updates)
		h.closed = true
		cancel()
		return h
	}
	h.mu.Lock()
	h.updates <- h.state
	h.stopLifetime = context.AfterFunc(client.ctx, h.Close)
	h.mu.Unlock()
	return h
}

// Mutate performs one write and then synchronizes associated cache state on success.
// The returned error describes admission, cancellation before execution, or business write failure.
// A nil error means the write succeeded; inspect result.SyncErr separately for cache repair.
// Success returned by a write is honored even if its context was canceled concurrently.
// Overlapping calls return ErrMutationBusy without queuing, deduplication, or changing the active state.
func (h *MutationHandle[I, V]) Mutate(ctx context.Context, input I) (MutationResult[V], error) {
	var result MutationResult[V]
	if err := ctx.Err(); err != nil {
		return result, err
	}
	h.mu.Lock()
	if err := h.admitLocked(); err != nil {
		h.mu.Unlock()
		return result, err
	}
	h.running = true
	h.state = MutationSnapshot[V]{Status: Pending}
	h.publishLocked()
	h.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(h.ctx, cancel)
	defer stop()
	// AfterFunc is asynchronous: explicitly observe a lifetime that already ended.
	if h.ctx.Err() != nil {
		cancel()
	}
	err := ctx.Err()
	if err == nil {
		result.Data, err = h.execute(ctx, input)
	}
	if err == nil {
		result.SyncErr = h.synchronize(ctx, input, result.Data)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.running = false
	h.state = MutationSnapshot[V]{Status: Error, Err: err}
	if err == nil {
		h.state = MutationSnapshot[V]{Data: result.Data, HasData: true, Status: Success, SyncErr: result.SyncErr}
	}
	if !h.closed {
		h.publishLocked()
	}
	return result, err
}

// execute holds budget capacity only while the business callback is running.
func (h *MutationHandle[I, V]) execute(ctx context.Context, input I) (V, error) {
	ctx, release, err := h.options.Executor.acquireContext(ctx)
	if err != nil {
		var zero V
		return zero, err
	}
	defer release()
	return h.write(ctx, input)
}

// admitLocked validates handle ownership without acquiring the Client lock.
func (h *MutationHandle[I, V]) admitLocked() error {
	switch {
	case h.err != nil:
		return h.err
	case h.client.ctx.Err() != nil:
		return ErrClosed
	case h.closed:
		return ErrMutationClosed
	case h.running:
		return ErrMutationBusy
	default:
		return nil
	}
}

// synchronize waits for application-owned post-commit work without treating its error as write failure.
func (h *MutationHandle[I, V]) synchronize(ctx context.Context, input I, value V) error {
	if h.onSuccess == nil {
		return nil
	}
	return h.onSuccess(ctx, value, input)
}

// Snapshot returns the latest execution outcome without starting work.
func (h *MutationHandle[I, V]) Snapshot() MutationSnapshot[V] {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

// Updates returns initial and latest execution states; slow consumers may skip intermediate states.
func (h *MutationHandle[I, V]) Updates() <-chan MutationSnapshot[V] { return h.updates }

// Close cancels active work and closes Updates once, without waiting for application callbacks.
// A late successful write still synchronizes cache state and updates Snapshot, but sends no notifications.
func (h *MutationHandle[I, V]) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	h.stopLifetime()
	h.cancel()
	close(h.updates)
}

// publishLocked replaces an unread snapshot while mu excludes subscription closure.
func (h *MutationHandle[I, V]) publishLocked() {
	select {
	case h.updates <- h.state:
	default:
		select {
		case <-h.updates:
		default:
		}
		h.updates <- h.state
	}
}
