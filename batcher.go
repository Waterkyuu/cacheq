package cacheq

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"
)

// Batcher groups independent loads and shares in-flight results without retaining a cache.
// Construct it with NewBatcher, reuse it within one data and authorization scope, and call Close.
// Distinct batches may execute concurrently; MaxBatchSize limits keys, not callback concurrency.
type Batcher[K comparable, V any] struct {
	// mu protects pending loads, the collection window, and lifecycle transitions.
	mu sync.Mutex
	// ctx owns batch contexts independently of individual callers' cancellation and values.
	ctx context.Context
	// cancel releases all batch contexts when this scheduler closes.
	cancel context.CancelFunc
	// stopLifetime detaches the parent cancellation callback during explicit closure.
	stopLifetime func() bool
	// load performs the business-supplied bulk operation outside mu.
	load BatchFunc[K, V]
	// options contains the validated, copied collection policy.
	options BatchOptions
	// pending indexes queued and running loads so duplicate keys share one result.
	pending map[K]*batchLoad[K, V]
	// queued owns the current collection window, or is nil between windows.
	queued *batchGroup[K, V]
	// afterFunc schedules collection windows; tests replace it with a controlled timer source.
	afterFunc func(time.Duration, func()) func() bool
	// err records the first terminal lifecycle error and prevents new work.
	err error
}

// batchLoad retains one key's shared outcome until all current callers receive it.
type batchLoad[K comparable, V any] struct {
	// key identifies this item independently of callback map or slice mutations.
	key K
	// done closes after result has been assigned under the batcher's lock.
	done chan struct{}
	// result is immutable after done closes.
	result BatchResult[V]
	// waiters counts callers that have not released their wait.
	waiters int
	// finished prevents canceled or detached work from publishing another outcome.
	finished bool
	// group owns this item's collection window and eventual shared callback context.
	group *batchGroup[K, V]
}

// batchGroup owns the keys collected for one callback and its cancellation resources.
type batchGroup[K comparable, V any] struct {
	// loads contains queued items; it becomes immutable when dispatch starts.
	loads []*batchLoad[K, V]
	// remaining counts unfinished keys with at least one caller.
	remaining int
	// started distinguishes collection from a dispatched callback.
	started bool
	// stopTimer cancels this group's collection timer when dispatched or abandoned.
	stopTimer func() bool
	// cancel stops the callback after all keys lose their callers or complete.
	cancel context.CancelFunc
}

// NewBatcher constructs a scheduler owned by ctx rather than any one Load caller.
// Nil callbacks return ErrNoFetcher; negative Wait or non-positive MaxBatchSize return ErrInvalidBatchOptions.
// An already-canceled lifetime returns ctx.Err(). The caller must provide a non-nil context.
func NewBatcher[K comparable, V any](
	ctx context.Context,
	load BatchFunc[K, V],
	options BatchOptions,
) (*Batcher[K, V], error) {
	if load == nil {
		return nil, ErrNoFetcher
	}
	if options.Wait < 0 || options.MaxBatchSize <= 0 {
		return nil, ErrInvalidBatchOptions
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Ownership transfers to Close, or to shutdown when the parent lifetime ends.
	lifetime, cancel := context.WithCancel(ctx) // #nosec G118 -- Close and shutdown release the scheduler's lifetime.
	b := &Batcher[K, V]{
		ctx: lifetime, cancel: cancel, load: load, options: options,
		pending: make(map[K]*batchLoad[K, V]),
		afterFunc: func(delay time.Duration, run func()) func() bool {
			return time.AfterFunc(delay, run).Stop
		},
	}
	// A parent canceled during construction may run this callback immediately.
	// Holding mu until stopLifetime is installed keeps shutdown from observing a partial owner.
	b.mu.Lock()
	b.stopLifetime = context.AfterFunc(lifetime, func() { b.shutdown(lifetime.Err()) })
	b.mu.Unlock()
	return b, nil
}

// Load queues or joins a key's in-flight load and waits for its matched result.
// Canceling ctx releases only this caller; callbacks use the NewBatcher lifetime's values and deadline.
// Nil, dynamically non-comparable, and non-reflexive keys return ErrInvalidKey.
// Results are not cached: a call after completion starts a new load.
func (b *Batcher[K, V]) Load(ctx context.Context, key K) (V, error) {
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	value := reflect.ValueOf(key)
	if !value.IsValid() || !value.Comparable() {
		return zero, ErrInvalidKey
	}
	if key != key { //nolint:gocritic // NaN-containing keys cannot be retrieved from a map, even with the original key.
		return zero, ErrInvalidKey
	}
	b.mu.Lock()
	if err := b.lifecycleErrorLocked(); err != nil {
		b.mu.Unlock()
		return zero, err
	}
	item := b.pending[key]
	if item == nil {
		item = b.enqueueLocked(key)
	} else {
		item.waiters++
	}
	b.mu.Unlock()
	defer b.releaseWaiter(item)
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-item.done:
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		return item.result.Data, item.result.Err
	}
}

// Fetcher binds key for use with Fetch, FetchWithOptions, Query, and Prefetch.
// Creating the function starts no work; the Client invokes it only when a load is needed.
// Use a scoped Client cache key separately from this batch key to avoid sharing unrelated data.
func (b *Batcher[K, V]) Fetcher(key K) Fetcher[V] {
	return func(ctx context.Context) (V, error) { return b.Load(ctx, key) }
}

// Close releases pending callers with ErrBatcherClosed and cancels active callbacks.
// It is idempotent and does not wait for callbacks that ignore their context.
func (b *Batcher[K, V]) Close() {
	b.shutdown(ErrBatcherClosed)
}

// lifecycleErrorLocked gives explicit closure precedence over the scheduler's canceled context.
func (b *Batcher[K, V]) lifecycleErrorLocked() error {
	if b.err != nil {
		return b.err
	}
	return b.ctx.Err()
}

// enqueueLocked registers a unique key and starts or fills its collection window.
func (b *Batcher[K, V]) enqueueLocked(key K) *batchLoad[K, V] {
	group := b.queued
	if group == nil {
		group = &batchGroup[K, V]{loads: make([]*batchLoad[K, V], 0)}
		b.queued = group
	}
	item := &batchLoad[K, V]{key: key, done: make(chan struct{}), waiters: 1, group: group}
	b.pending[key] = item
	group.loads = append(group.loads, item)
	group.remaining++
	if len(group.loads) >= b.options.MaxBatchSize {
		b.dispatchLocked(group)
	} else if group.stopTimer == nil {
		group.stopTimer = b.afterFunc(b.options.Wait, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.dispatchLocked(group)
		})
	}
	return item
}

// dispatchLocked detaches the current window before starting a callback outside the lock.
func (b *Batcher[K, V]) dispatchLocked(group *batchGroup[K, V]) {
	// Stopped timers can still fire after a replacement window has taken ownership.
	if b.queued != group || b.lifecycleErrorLocked() != nil {
		return
	}
	b.queued = nil
	group.started = true
	if group.stopTimer != nil {
		group.stopTimer()
	}
	// run releases this context; the last departing caller can cancel it sooner.
	ctx, cancel := context.WithCancel(b.ctx) // #nosec G118 -- run and finishLocked release the batch context.
	group.cancel = cancel
	go b.run(ctx, group)
}

// run matches outcomes to immutable load identities, ignoring results from abandoned callbacks.
func (b *Batcher[K, V]) run(ctx context.Context, group *batchGroup[K, V]) {
	defer group.cancel()
	keys := make([]K, 0, len(group.loads))
	for _, item := range group.loads {
		keys = append(keys, item.key)
	}
	results := make(map[K]BatchResult[V])
	err := ctx.Err()
	if err == nil {
		results, err = b.load(ctx, keys)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	for _, item := range group.loads {
		if item.finished {
			continue
		}
		result := BatchResult[V]{Err: err}
		if err == nil {
			var exists bool
			result, exists = results[item.key]
			if !exists {
				result.Err = fmt.Errorf("%w: %v", ErrBatchResultMissing, item.key)
			}
		}
		b.finishLocked(item, result)
	}
}

// releaseWaiter abandons a key only when its last caller leaves before completion.
func (b *Batcher[K, V]) releaseWaiter(item *batchLoad[K, V]) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if item.finished {
		return
	}
	item.waiters--
	if item.waiters == 0 {
		b.finishLocked(item, BatchResult[V]{Err: context.Canceled})
	}
}

// finishLocked publishes once and releases the key's share of its group's resources.
func (b *Batcher[K, V]) finishLocked(item *batchLoad[K, V], result BatchResult[V]) {
	item.finished = true
	item.result = result
	delete(b.pending, item.key)
	group := item.group
	group.remaining--
	if !group.started {
		group.loads = slices.DeleteFunc(group.loads, func(queued *batchLoad[K, V]) bool { return queued == item })
	}
	if group.remaining == 0 {
		if group.started {
			group.cancel()
		} else {
			b.queued = nil
			if group.stopTimer != nil {
				group.stopTimer()
			}
		}
	}
	close(item.done)
}

// shutdown records the terminal reason, stops collection, and releases all queued or running callers.
func (b *Batcher[K, V]) shutdown(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return
	}
	b.err = err
	b.stopLifetime()
	b.cancel()
	for _, item := range b.pending {
		b.finishLocked(item, BatchResult[V]{Err: err})
	}
}
