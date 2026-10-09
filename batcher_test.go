package cacheq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// batchTestTimer exposes a collection deadline without depending on elapsed wall-clock time.
type batchTestTimer struct {
	// delay records the requested collection window.
	delay time.Duration
	// fire invokes the scheduled callback, even if it has already been stopped.
	fire func()
}

// batchWaitContext signals when Load reaches its wait after registering the caller.
type batchWaitContext struct {
	// Context supplies the caller's actual cancellation and values.
	context.Context
	// ready closes when Load has registered this wait.
	ready chan struct{}
	// once permits repeated Done calls without closing ready twice.
	once sync.Once
}

// Done exposes registration before returning the wrapped cancellation channel.
func (c *batchWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.ready) })
	return c.Context.Done()
}

// receiveBatchValue bounds a test harness wait without using time to decide business behavior.
func receiveBatchValue[V any](t *testing.T, values <-chan V) V {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for batch operation")
		var zero V
		return zero
	}
}

// newTestBatcher injects a manually fired collection timer before admitting any callers.
func newTestBatcher[K comparable, V any](
	t *testing.T,
	ctx context.Context,
	load BatchFunc[K, V],
	options BatchOptions,
) (*Batcher[K, V], <-chan batchTestTimer) {
	t.Helper()
	b, err := NewBatcher(ctx, load, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	timers := make(chan batchTestTimer, 1)
	b.afterFunc = func(delay time.Duration, run func()) func() bool {
		timers <- batchTestTimer{delay: delay, fire: run}
		return func() bool { return true }
	}
	return b, timers
}

// startBatchLoad admits a caller deterministically and returns its eventual public outcome.
func startBatchLoad[K comparable, V any](
	t *testing.T,
	b *Batcher[K, V],
	ctx context.Context,
	key K,
) <-chan BatchResult[V] {
	t.Helper()
	wait := &batchWaitContext{Context: ctx, ready: make(chan struct{})}
	result := make(chan BatchResult[V], 1)
	go func() {
		value, err := b.Load(wait, key)
		result <- BatchResult[V]{Data: value, Err: err}
	}()
	receiveBatchValue(t, wait.ready)
	return result
}

// TestBatcherValidation rejects invalid callbacks, policy, and an expired owner before allocating work.
func TestBatcherValidation(t *testing.T) {
	load := func(context.Context, []int) (map[int]BatchResult[int], error) { return nil, nil }
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		// name identifies the invalid constructor argument.
		name string
		// ctx supplies the scheduler lifetime.
		ctx context.Context
		// load is the callback under validation.
		load BatchFunc[int, int]
		// options contains the collection policy under validation.
		options BatchOptions
		// err is the expected public error identity.
		err error
	}{
		{name: "nil callback", ctx: context.Background(), options: BatchOptions{MaxBatchSize: 2}, err: ErrNoFetcher},
		{name: "negative wait", ctx: context.Background(), load: load,
			options: BatchOptions{Wait: -1, MaxBatchSize: 2}, err: ErrInvalidBatchOptions},
		{name: "zero size", ctx: context.Background(), load: load, err: ErrInvalidBatchOptions},
		{name: "negative size", ctx: context.Background(), load: load,
			options: BatchOptions{MaxBatchSize: -1}, err: ErrInvalidBatchOptions},
		{name: "canceled owner", ctx: canceled, load: load,
			options: BatchOptions{MaxBatchSize: 2}, err: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := NewBatcher(tc.ctx, tc.load, tc.options)
			if b != nil || !errors.Is(err, tc.err) {
				t.Fatalf("constructor = %v, %v; want nil, %v", b, err, tc.err)
			}
		})
	}
}

// TestBatcherCollection groups different keys at the window boundary and does not cache completed results.
func TestBatcherCollection(t *testing.T) {
	var calls atomic.Int32
	batches := make(chan []int, 1)
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, keys []int) (map[int]BatchResult[int], error) {
			calls.Add(1)
			batches <- keys
			results := make(map[int]BatchResult[int], len(keys))
			for _, key := range keys {
				results[key] = BatchResult[int]{Data: key * 10}
			}
			return results, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 3},
	)
	first := startBatchLoad(t, b, context.Background(), 1)
	timer := receiveBatchValue(t, timers)
	second := startBatchLoad(t, b, context.Background(), 2)
	if timer.delay != time.Hour || calls.Load() != 0 {
		t.Fatalf("collection delay = %v; calls = %d", timer.delay, calls.Load())
	}
	timer.fire()
	if keys := receiveBatchValue(t, batches); !reflect.DeepEqual(keys, []int{1, 2}) {
		t.Fatalf("batch keys = %v", keys)
	}
	for i, result := range []<-chan BatchResult[int]{first, second} {
		if got := receiveBatchValue(t, result); got.Err != nil || got.Data != (i+1)*10 {
			t.Fatalf("key %d = %+v", i+1, got)
		}
	}
	again := startBatchLoad(t, b, context.Background(), 1)
	receiveBatchValue(t, timers).fire()
	if keys := receiveBatchValue(t, batches); !reflect.DeepEqual(keys, []int{1}) {
		t.Fatalf("next batch keys = %v", keys)
	}
	if got := receiveBatchValue(t, again); got.Data != 10 || got.Err != nil || calls.Load() != 2 {
		t.Fatalf("uncached repeat = %+v; calls = %d", got, calls.Load())
	}
}

// TestBatcherSizeDispatch bounds callbacks and permits another batch while an earlier one is blocked.
func TestBatcherSizeDispatch(t *testing.T) {
	started := make(chan []int, 2)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(ctx context.Context, keys []int) (map[int]BatchResult[int], error) {
			started <- keys
			if keys[0] == 1 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-release:
				}
			}
			results := make(map[int]BatchResult[int], len(keys))
			for _, key := range keys {
				results[key] = BatchResult[int]{Data: key}
			}
			return results, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	first := startBatchLoad(t, b, context.Background(), 1)
	oldTimer := receiveBatchValue(t, timers)
	second := startBatchLoad(t, b, context.Background(), 2)
	if keys := receiveBatchValue(t, started); !reflect.DeepEqual(keys, []int{1, 2}) {
		t.Fatalf("first full batch = %v", keys)
	}
	third := startBatchLoad(t, b, context.Background(), 3)
	receiveBatchValue(t, timers)
	oldTimer.fire()
	fourth := startBatchLoad(t, b, context.Background(), 4)
	if keys := receiveBatchValue(t, started); !reflect.DeepEqual(keys, []int{3, 4}) {
		t.Fatalf("second full batch = %v", keys)
	}
	for i, result := range []<-chan BatchResult[int]{third, fourth} {
		if got := receiveBatchValue(t, result); got.Data != i+3 || got.Err != nil {
			t.Fatalf("unblocked key = %+v", got)
		}
	}
	b.Close()
	for _, result := range []<-chan BatchResult[int]{first, second} {
		if got := receiveBatchValue(t, result); !errors.Is(got.Err, ErrBatcherClosed) {
			t.Fatalf("closed blocked batch = %+v", got)
		}
	}
}

// TestBatcherZeroWait dispatches through the timer source without adding a collection delay.
func TestBatcherZeroWait(t *testing.T) {
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, keys []int) (map[int]BatchResult[int], error) {
			return map[int]BatchResult[int]{keys[0]: {Data: 7}}, nil
		},
		BatchOptions{MaxBatchSize: 2},
	)
	result := startBatchLoad(t, b, context.Background(), 1)
	timer := receiveBatchValue(t, timers)
	if timer.delay != 0 {
		t.Fatalf("zero wait scheduled %v", timer.delay)
	}
	timer.fire()
	if got := receiveBatchValue(t, result); got.Data != 7 || got.Err != nil {
		t.Fatalf("immediate result = %+v", got)
	}
}

// TestBatcherResults separates per-key failures, omissions, extra keys, and whole-batch errors.
func TestBatcherResults(t *testing.T) {
	missing := errors.New("product not found")
	unavailable := errors.New("database unavailable")
	for _, tc := range []struct {
		// name identifies the outcome shape supplied by the application.
		name string
		// results is the callback's map, which need not follow request order.
		results map[int]BatchResult[int]
		// err fails the whole callback when non-nil.
		err error
		// want is each requested key's expected public outcome.
		want []BatchResult[int]
	}{
		{name: "zero and extra", results: map[int]BatchResult[int]{2: {Data: 22}, 1: {}, 99: {Data: 99}},
			want: []BatchResult[int]{{}, {Data: 22}}},
		{name: "partial failure", results: map[int]BatchResult[int]{1: {Data: 11}, 2: {Data: -1, Err: missing}},
			want: []BatchResult[int]{{Data: 11}, {Data: -1, Err: missing}}},
		{name: "omitted key", results: map[int]BatchResult[int]{1: {Data: 11}},
			want: []BatchResult[int]{{Data: 11}, {Err: ErrBatchResultMissing}}},
		{name: "nil map", want: []BatchResult[int]{{Err: ErrBatchResultMissing}, {Err: ErrBatchResultMissing}}},
		{name: "whole failure", results: map[int]BatchResult[int]{1: {Data: 11}}, err: unavailable,
			want: []BatchResult[int]{{Err: unavailable}, {Err: unavailable}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, timers := newTestBatcher(t, context.Background(),
				func(context.Context, []int) (map[int]BatchResult[int], error) { return tc.results, tc.err },
				BatchOptions{Wait: time.Hour, MaxBatchSize: 2})
			first := startBatchLoad(t, b, context.Background(), 1)
			receiveBatchValue(t, timers)
			second := startBatchLoad(t, b, context.Background(), 2)
			for i, result := range []<-chan BatchResult[int]{first, second} {
				got := receiveBatchValue(t, result)
				if got.Data != tc.want[i].Data || !errors.Is(got.Err, tc.want[i].Err) {
					t.Fatalf("key %d = %+v; want %+v", i+1, got, tc.want[i])
				}
			}
		})
	}
}

// TestBatcherKeys rejects runtime-unsafe map keys while accepting ordinary zero and structured keys.
func TestBatcherKeys(t *testing.T) {
	// compositeKey exercises runtime comparability through an interface field.
	type compositeKey struct {
		// Value supplies the dynamically typed portion of the key.
		Value any
	}
	// productKey represents an ordinary comparable business identifier.
	type productKey struct {
		// ID selects the product to load.
		ID int
	}
	b, _ := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, keys []any) (map[any]BatchResult[int], error) {
			return map[any]BatchResult[int]{keys[0]: {Data: 7}}, nil
		},
		BatchOptions{MaxBatchSize: 1},
	)
	for _, key := range []any{nil, []int{1}, compositeKey{Value: []int{1}}, math.NaN()} {
		if value, err := b.Load(context.Background(), key); value != 0 || !errors.Is(err, ErrInvalidKey) {
			t.Fatalf("invalid key %v = %d, %v", key, value, err)
		}
	}
	for _, key := range []any{0, "", productKey{ID: 42}} {
		if value, err := b.Load(context.Background(), key); value != 7 || err != nil {
			t.Fatalf("valid key %v = %d, %v", key, value, err)
		}
	}
}

// TestBatcherWaiterCancellation preserves duplicate waiters and other keys when one caller leaves.
func TestBatcherWaiterCancellation(t *testing.T) {
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(ctx context.Context, keys []int) (map[int]BatchResult[int], error) {
			if !reflect.DeepEqual(keys, []int{1, 2}) {
				return nil, fmt.Errorf("unexpected keys: %v", keys)
			}
			started <- ctx
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
				return map[int]BatchResult[int]{1: {Data: 11}, 2: {Data: 22}}, nil
			}
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startBatchLoad(t, b, ctx, 1)
	receiveBatchValue(t, timers)
	joined := startBatchLoad(t, b, context.Background(), 1)
	second := startBatchLoad(t, b, context.Background(), 2)
	batchCtx := receiveBatchValue(t, started)
	cancel()
	if got := receiveBatchValue(t, first); !errors.Is(got.Err, context.Canceled) || batchCtx.Err() != nil {
		t.Fatalf("canceled caller = %+v; batch error = %v", got, batchCtx.Err())
	}
	unblock()
	for i, result := range []<-chan BatchResult[int]{joined, second} {
		if got := receiveBatchValue(t, result); got.Data != (i+1)*11 || got.Err != nil {
			t.Fatalf("remaining caller = %+v", got)
		}
	}
}

// TestBatcherQueuedCancellation removes abandoned keys and ignores an obsolete collection callback.
func TestBatcherQueuedCancellation(t *testing.T) {
	var calls atomic.Int32
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, keys []int) (map[int]BatchResult[int], error) {
			calls.Add(1)
			if !reflect.DeepEqual(keys, []int{2}) {
				return nil, fmt.Errorf("unexpected queued keys: %v", keys)
			}
			return map[int]BatchResult[int]{2: {Data: 22}}, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	ctx, cancel := context.WithCancel(context.Background())
	first := startBatchLoad(t, b, ctx, 1)
	oldTimer := receiveBatchValue(t, timers)
	cancel()
	if got := receiveBatchValue(t, first); !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("queued cancellation = %+v", got)
	}
	second := startBatchLoad(t, b, context.Background(), 2)
	newTimer := receiveBatchValue(t, timers)
	oldTimer.fire()
	if calls.Load() != 0 {
		t.Fatal("abandoned window dispatched replacement keys")
	}
	newTimer.fire()
	if got := receiveBatchValue(t, second); got.Data != 22 || got.Err != nil || calls.Load() != 1 {
		t.Fatalf("replacement window = %+v; calls = %d", got, calls.Load())
	}
}

// TestBatcherAllWaitersLeave cancels the shared callback and isolates a replacement load from its late result.
func TestBatcherAllWaitersLeave(t *testing.T) {
	var calls atomic.Int32
	started := make(chan context.Context, 1)
	abandoned := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	b, _ := newTestBatcher(
		t,
		context.Background(),
		func(ctx context.Context, keys []int) (map[int]BatchResult[int], error) {
			if calls.Add(1) == 1 {
				started <- ctx
				<-ctx.Done()
				close(abandoned)
				<-release
				defer close(finished)
				return map[int]BatchResult[int]{keys[0]: {Data: -1}}, nil
			}
			return map[int]BatchResult[int]{keys[0]: {Data: 7}}, nil
		},
		BatchOptions{MaxBatchSize: 1},
	)
	ctx, cancel := context.WithCancel(context.Background())
	first := startBatchLoad(t, b, ctx, 1)
	receiveBatchValue(t, started)
	cancel()
	if got := receiveBatchValue(t, first); !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("departing caller = %+v", got)
	}
	receiveBatchValue(t, abandoned)
	if value, err := b.Load(context.Background(), 1); value != 7 || err != nil {
		t.Fatalf("replacement = %d, %v", value, err)
	}
	b.Close()
	unblock()
	receiveBatchValue(t, finished)
}

// TestBatcherActiveKeyCancellation preserves other keys and cancels only after every key is abandoned.
func TestBatcherActiveKeyCancellation(t *testing.T) {
	for _, abandonBoth := range []bool{false, true} {
		t.Run(fmt.Sprintf("abandonBoth=%t", abandonBoth), func(t *testing.T) {
			started := make(chan context.Context, 1)
			stopped := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			b, timers := newTestBatcher(
				t,
				context.Background(),
				func(ctx context.Context, _ []int) (map[int]BatchResult[int], error) {
					started <- ctx
					select {
					case <-ctx.Done():
						close(stopped)
						return nil, ctx.Err()
					case <-release:
						return map[int]BatchResult[int]{1: {Data: 11}, 2: {Data: 22}}, nil
					}
				},
				BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
			)
			firstCtx, cancelFirst := context.WithCancel(context.Background())
			defer cancelFirst()
			secondCtx, cancelSecond := context.WithCancel(context.Background())
			defer cancelSecond()
			first := startBatchLoad(t, b, firstCtx, 1)
			receiveBatchValue(t, timers)
			second := startBatchLoad(t, b, secondCtx, 2)
			batchCtx := receiveBatchValue(t, started)
			cancelFirst()
			if got := receiveBatchValue(t, first); !errors.Is(got.Err, context.Canceled) || batchCtx.Err() != nil {
				t.Fatalf("one key abandoned = %+v; batch error = %v", got, batchCtx.Err())
			}
			if abandonBoth {
				cancelSecond()
				if got := receiveBatchValue(t, second); !errors.Is(got.Err, context.Canceled) {
					t.Fatalf("last key abandoned = %+v", got)
				}
				receiveBatchValue(t, stopped)
				return
			}
			unblock()
			if got := receiveBatchValue(t, second); got.Data != 22 || got.Err != nil {
				t.Fatalf("surviving key = %+v", got)
			}
		})
	}
}

// TestBatcherQueuedKeyCancellation removes one key without abandoning the remaining collection window.
func TestBatcherQueuedKeyCancellation(t *testing.T) {
	keys := make(chan []int, 1)
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, ids []int) (map[int]BatchResult[int], error) {
			keys <- ids
			return map[int]BatchResult[int]{2: {Data: 22}}, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 3},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := startBatchLoad(t, b, ctx, 1)
	timer := receiveBatchValue(t, timers)
	second := startBatchLoad(t, b, context.Background(), 2)
	cancel()
	if got := receiveBatchValue(t, first); !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("canceled queued key = %+v", got)
	}
	timer.fire()
	if got := receiveBatchValue(t, keys); !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("remaining queued keys = %v", got)
	}
	if got := receiveBatchValue(t, second); got.Data != 22 || got.Err != nil {
		t.Fatalf("remaining queued result = %+v", got)
	}
}

// TestBatcherLifetime releases queued and active work with the correct lifecycle error.
func TestBatcherLifetime(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("active=%t/close=%t", active, explicit), func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				started := make(chan context.Context, 1)
				b, timers := newTestBatcher(
					t,
					parent,
					func(ctx context.Context, _ []int) (map[int]BatchResult[int], error) {
						started <- ctx
						<-ctx.Done()
						return nil, ctx.Err()
					},
					BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
				)
				result := startBatchLoad(t, b, context.Background(), 1)
				timer := receiveBatchValue(t, timers)
				var batchCtx context.Context
				if active {
					timer.fire()
					batchCtx = receiveBatchValue(t, started)
				}
				want := error(context.Canceled)
				if explicit {
					want = ErrBatcherClosed
					b.Close()
					b.Close()
				} else {
					cancel()
				}
				if got := receiveBatchValue(t, result); !errors.Is(got.Err, want) {
					t.Fatalf("lifetime outcome = %+v; want %v", got, want)
				}
				if batchCtx != nil && batchCtx.Err() == nil {
					t.Fatal("lifetime did not cancel active callback")
				}
				if _, err := b.Load(context.Background(), 2); !errors.Is(err, want) {
					t.Fatalf("load after lifetime = %v; want %v", err, want)
				}
				timer.fire()
			})
		}
	}
}

// TestBatcherContext uses the owner values and deadline while respecting a caller canceled before enqueue.
func TestBatcherContext(t *testing.T) {
	// scopeKey identifies a request-independent service value in the test lifetime.
	type scopeKey struct{}
	owner := context.WithValue(context.Background(), scopeKey{}, "service")
	owner, cancel := context.WithDeadline(owner, time.Now().Add(time.Hour))
	defer cancel()
	b, _ := newTestBatcher(t, owner, func(ctx context.Context, keys []int) (map[int]BatchResult[string], error) {
		gotDeadline, ok := ctx.Deadline()
		wantDeadline, _ := owner.Deadline()
		if !ok || !gotDeadline.Equal(wantDeadline) {
			return nil, errors.New("batch did not inherit owner deadline")
		}
		return map[int]BatchResult[string]{keys[0]: {Data: ctx.Value(scopeKey{}).(string)}}, nil
	}, BatchOptions{MaxBatchSize: 1})
	caller := context.WithValue(context.Background(), scopeKey{}, "caller")
	if value, err := b.Load(caller, 1); value != "service" || err != nil {
		t.Fatalf("owner context = %q, %v", value, err)
	}
	canceled, stop := context.WithCancel(caller)
	stop()
	if _, err := b.Load(canceled, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled caller = %v", err)
	}
	expired, expire := context.WithDeadline(caller, time.Time{})
	defer expire()
	if _, err := b.Load(expired, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired caller = %v", err)
	}
}
