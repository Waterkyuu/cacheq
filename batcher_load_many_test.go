package cacheq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newLoadManyTestBatcher disables timer dispatch so only size limits and LoadMany can start work.
func newLoadManyTestBatcher[K comparable, V any](
	t *testing.T,
	ctx context.Context,
	load BatchFunc[K, V],
	options BatchOptions,
) *Batcher[K, V] {
	t.Helper()
	b, err := NewBatcher(ctx, load, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	b.afterFunc = func(time.Duration, func()) func() bool { return func() bool { return true } }
	return b
}

// startBatchLoadMany waits for registration before exposing an asynchronous call's result channel.
func startBatchLoadMany[K comparable, V any](
	t *testing.T,
	b *Batcher[K, V],
	ctx context.Context,
	keys []K,
) <-chan []BatchResult[V] {
	t.Helper()
	wait := &batchWaitContext{Context: ctx, ready: make(chan struct{})}
	result := make(chan []BatchResult[V], 1)
	go func() { result <- b.LoadMany(wait, keys) }()
	receiveBatchValue(t, wait.ready)
	return result
}

// TestBatcherLoadManyDispatch preserves positions while splitting unique keys and flushing the final window.
func TestBatcherLoadManyDispatch(t *testing.T) {
	for _, size := range []int{1, 2, 10} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			calls := make(chan []int, 3)
			b := newLoadManyTestBatcher(t, context.Background(),
				func(_ context.Context, keys []int) (map[int]BatchResult[int], error) {
					calls <- slices.Clone(keys)
					values := make(map[int]BatchResult[int], len(keys))
					for _, key := range keys {
						values[key] = BatchResult[int]{Data: key * 10}
					}
					return values, nil
				}, BatchOptions{Wait: time.Hour, MaxBatchSize: size})
			out := make(chan []BatchResult[int], 1)
			go func() { out <- b.LoadMany(context.Background(), []int{3, 1, 3, 2, 1}) }()
			results := receiveBatchValue(t, out)
			for index, want := range []int{30, 10, 30, 20, 10} {
				if results[index].Err != nil || results[index].Data != want {
					t.Fatalf("result[%d] = %+v, want %d", index, results[index], want)
				}
			}
			var dispatched []int
			for range (3 + size - 1) / size {
				keys := receiveBatchValue(t, calls)
				if len(keys) > size {
					t.Fatalf("callback exceeded size limit: %v", keys)
				}
				dispatched = append(dispatched, keys...)
			}
			slices.Sort(dispatched)
			if !slices.Equal(dispatched, []int{1, 2, 3}) {
				t.Fatalf("dispatched keys = %v", dispatched)
			}
		})
	}
}

// TestBatcherLoadManyResults isolates per-key failures and propagates whole-callback failures to duplicate positions.
func TestBatcherLoadManyResults(t *testing.T) {
	failure := errors.New("backend failure")
	for _, wholeFailure := range []bool{false, true} {
		t.Run(map[bool]string{false: "per-key", true: "whole-callback"}[wholeFailure], func(t *testing.T) {
			b := newLoadManyTestBatcher(t, context.Background(),
				func(context.Context, []int) (map[int]BatchResult[int], error) {
					values := map[int]BatchResult[int]{1: {Data: 10}, 2: {Data: 20, Err: failure}}
					if wholeFailure {
						return values, failure
					}
					return values, nil
				}, BatchOptions{MaxBatchSize: 10})
			results := b.LoadMany(context.Background(), []int{1, 2, 3, 2})
			if wholeFailure {
				for _, result := range results {
					if result.Data != 0 || !errors.Is(result.Err, failure) {
						t.Fatalf("whole failure = %+v", results)
					}
				}
				return
			}
			if results[0].Data != 10 || results[0].Err != nil ||
				results[1].Data != 20 || !errors.Is(results[1].Err, failure) ||
				!errors.Is(results[2].Err, ErrBatchResultMissing) || results[3] != results[1] {
				t.Fatalf("partial outcomes = %+v", results)
			}
		})
	}
}

// TestBatcherLoadManyInvalidKeys rejects invalid positions without blocking valid keys or invoking empty batches.
func TestBatcherLoadManyInvalidKeys(t *testing.T) {
	var calls atomic.Int32
	b := newLoadManyTestBatcher(t, context.Background(),
		func(_ context.Context, keys []any) (map[any]BatchResult[int], error) {
			calls.Add(1)
			if !slices.Equal(keys, []any{1}) {
				t.Errorf("callback keys = %v", keys)
			}
			return map[any]BatchResult[int]{1: {Data: 10}}, nil
		}, BatchOptions{MaxBatchSize: 10})
	for _, keys := range [][]any{nil, {}, {nil, []int{1}, math.NaN()}, {nil, 1, []int{1}, math.NaN(), 1}} {
		results := b.LoadMany(context.Background(), keys)
		if results == nil || len(results) != len(keys) {
			t.Fatalf("result shape = %+v for %v", results, keys)
		}
		for index, result := range results {
			if len(keys) == 5 && (index == 1 || index == 4) {
				if result.Data != 10 || result.Err != nil {
					t.Fatalf("valid result[%d] = %+v", index, result)
				}
			} else if !errors.Is(result.Err, ErrInvalidKey) {
				t.Fatalf("invalid result[%d] = %+v", index, result)
			}
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("callbacks = %d, want 1", calls.Load())
	}
}

// TestBatcherLoadManyCancellation retains a completed later key when an earlier key is still blocked.
func TestBatcherLoadManyCancellation(t *testing.T) {
	entered := make(chan int, 2)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	b := newLoadManyTestBatcher(t, context.Background(),
		func(ctx context.Context, keys []int) (map[int]BatchResult[int], error) {
			entered <- keys[0]
			if keys[0] == 2 {
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			} else {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return map[int]BatchResult[int]{2: {Data: 20}}, nil
		}, BatchOptions{MaxBatchSize: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := startBatchLoadMany(t, b, ctx, []int{1, 2, 2})
	for range 2 {
		receiveBatchValue(t, entered)
	}
	joined := startBatchLoad(t, b, context.Background(), 2)
	// Publishing through a surviving Load proves key 2 completed before cancellation of the group wait.
	unblock()
	if result := receiveBatchValue(t, joined); result.Data != 20 || result.Err != nil {
		t.Fatalf("joined result = %+v", result)
	}
	cancel()
	results := receiveBatchValue(t, out)
	if !errors.Is(results[0].Err, context.Canceled) || results[1].Data != 20 ||
		results[1].Err != nil || results[2] != results[1] {
		t.Fatalf("canceled results = %+v", results)
	}
}

// TestBatcherLoadManySharedWaiter flushes queued Load work and releases only its own interest on cancellation.
func TestBatcherLoadManySharedWaiter(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var canceled atomic.Bool
	b := newLoadManyTestBatcher(t, context.Background(),
		func(ctx context.Context, keys []int) (map[int]BatchResult[int], error) {
			close(entered)
			select {
			case <-ctx.Done():
				canceled.Store(true)
				return nil, ctx.Err()
			case <-release:
			}
			results := make(map[int]BatchResult[int], len(keys))
			for _, key := range keys {
				results[key] = BatchResult[int]{Data: key * 10}
			}
			return results, nil
		}, BatchOptions{Wait: time.Hour, MaxBatchSize: 10})
	first := startBatchLoad(t, b, context.Background(), 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	many := startBatchLoadMany(t, b, ctx, []int{1, 1, 2})
	receiveBatchValue(t, entered)
	cancel()
	for _, result := range receiveBatchValue(t, many) {
		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf("canceled result = %+v", result)
		}
	}
	close(release)
	if result := receiveBatchValue(t, first); result.Data != 10 || result.Err != nil || canceled.Load() {
		t.Fatalf("surviving caller = %+v, callback canceled = %v", result, canceled.Load())
	}
}

// TestBatcherLoadManyAdmission rejects expired lifetimes and recursive budgets without starting callbacks.
func TestBatcherLoadManyAdmission(t *testing.T) {
	for _, action := range []string{"caller canceled", "deadline", "owner canceled", "closed", "reentry"} {
		t.Run(action, func(t *testing.T) {
			owner, stop := context.WithCancel(context.Background())
			defer stop()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			executor, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
			if err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			b := newLoadManyTestBatcher(t, owner,
				func(context.Context, []int) (map[int]BatchResult[int], error) {
					calls.Add(1)
					return nil, nil
				}, BatchOptions{MaxBatchSize: 10, Executor: executor})
			want := context.Canceled
			switch action {
			case "caller canceled":
				cancel()
			case "deadline":
				var expire context.CancelFunc
				ctx, expire = context.WithDeadline(ctx, time.Unix(1, 0))
				defer expire()
				want = context.DeadlineExceeded
			case "owner canceled":
				stop()
			case "closed":
				b.Close()
				want = ErrBatcherClosed
			case "reentry":
				var release func()
				ctx, release, err = executor.acquireContext(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer release()
				want = ErrExecutionReentry
			}
			for _, result := range b.LoadMany(ctx, []int{1, 2, 1}) {
				if !errors.Is(result.Err, want) {
					t.Fatalf("admission result = %+v, want %v", result, want)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("rejected call invoked the backend")
			}
		})
	}
}

// TestBatcherLoadManyBudget applies admission limits to callbacks rather than input positions.
func TestBatcherLoadManyBudget(t *testing.T) {
	executor, err := NewExecutor(ExecutionOptions{MaxConcurrent: 1})
	if err != nil {
		t.Fatal(err)
	}
	release, err := executor.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var calls atomic.Int32
	b := newLoadManyTestBatcher(t, context.Background(),
		func(context.Context, []int) (map[int]BatchResult[int], error) {
			calls.Add(1)
			return nil, nil
		}, BatchOptions{MaxBatchSize: 1, Executor: executor})
	for _, result := range b.LoadMany(context.Background(), []int{1, 2, 1}) {
		if !errors.Is(result.Err, ErrQueueFull) {
			t.Fatalf("budget rejection = %+v", result)
		}
	}
	if calls.Load() != 0 || executor.Stats().Rejected != 2 {
		t.Fatalf("callbacks = %d; budget = %+v", calls.Load(), executor.Stats())
	}
}

// ExampleBatcher_LoadMany demonstrates ordered outcomes without per-key goroutine orchestration.
func ExampleBatcher_LoadMany() {
	var calls atomic.Int32
	products, err := NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]BatchResult[string], error) {
			calls.Add(1)
			results := make(map[int]BatchResult[string], len(ids))
			for _, id := range ids {
				if id == 99 {
					results[id] = BatchResult[string]{Err: errors.New("product not found")}
					continue
				}
				results[id] = BatchResult[string]{Data: fmt.Sprintf("product %d", id)}
			}
			return results, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		panic(err)
	}
	defer products.Close()

	ids := []int{43, 42, 43, 99}
	results := products.LoadMany(context.Background(), ids)
	for index, result := range results {
		if result.Err != nil {
			fmt.Printf("%d: %v\n", ids[index], result.Err)
			continue
		}
		fmt.Printf("%d: %s\n", ids[index], result.Data)
	}
	fmt.Printf("bulk calls: %d\n", calls.Load())
	// Output:
	// 43: product 43
	// 42: product 42
	// 43: product 43
	// 99: product not found
	// bulk calls: 2
}
