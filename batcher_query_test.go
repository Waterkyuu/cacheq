package cacheq

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// batchProductKey separates product results from other cache entries using the same numeric ID.
type batchProductKey struct {
	// ID identifies a product in this test's single authorization scope.
	ID int
}

// TestBatcherFetcherClosedStopsRetries treats closure as terminal before admission and during a callback.
func TestBatcherFetcherClosedStopsRetries(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, customPredicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("active=%t/predicate=%t", active, customPredicate), func(t *testing.T) {
				var attempts, predicates, delays atomic.Int32
				options := Options{Retry: 3, RetryDelay: func(int) time.Duration {
					delays.Add(1)
					return 0
				}}
				if customPredicate {
					options.RetryIf = func(error) bool { predicates.Add(1); return true }
				}
				client := NewClient(options)
				t.Cleanup(client.Close)
				started := make(chan struct{})
				b, _ := newTestBatcher(
					t,
					context.Background(),
					func(ctx context.Context, _ []int) (map[int]BatchResult[int], error) {
						close(started)
						<-ctx.Done()
						return nil, ctx.Err()
					},
					BatchOptions{MaxBatchSize: 1},
				)
				if !active {
					b.Close()
				}
				result := make(chan error, 1)
				go func() {
					_, err := Fetch(context.Background(), client, "a", func(ctx context.Context) (int, error) {
						attempts.Add(1)
						value, err := b.Fetcher(1)(ctx)
						if err != nil {
							return value, fmt.Errorf("load batch: %w", err)
						}
						return value, nil
					})
					result <- err
				}()
				if active {
					receiveBatchValue(t, started)
					b.Close()
				}
				if err := receiveBatchValue(t, result); !errors.Is(err, ErrBatcherClosed) {
					t.Fatalf("closed batch outcome = %v", err)
				}
				if attempts.Load() != 1 || predicates.Load() != 0 || delays.Load() != 0 {
					t.Fatalf("closed batch attempts=%d predicates=%d delays=%d",
						attempts.Load(), predicates.Load(), delays.Load())
				}
				if stats := client.Stats(); stats.Retries != 0 || stats.LoadFailures != 1 {
					t.Fatalf("closed batch stats = %+v", stats)
				}
			})
		}
	}
}

// batchReleaseContext pauses a departing caller before its deferred batch cleanup.
type batchReleaseContext struct {
	// Context supplies the real cancellation state, including to later joining callers.
	context.Context
	// departed signals that the first canceled Err call is waiting for release.
	departed chan struct{}
	// release lets the canceled caller finish after replacement work is admitted.
	release <-chan struct{}
	// paused ensures only the departing caller waits, leaving later Err checks available.
	paused atomic.Bool
}

// Err exposes cancellation while delaying the original caller's deferred waiter release.
func (c *batchReleaseContext) Err() error {
	err := c.Context.Err()
	if err != nil && c.paused.CompareAndSwap(false, true) {
		close(c.departed)
		<-c.release
	}
	return err
}

// TestBatcherFetcherReplacement prevents detached work from supplying a replacement Client load.
func TestBatcherFetcherReplacement(t *testing.T) {
	for _, action := range []string{"remove", "clear", "cancel", "set"} {
		t.Run(action, func(t *testing.T) {
			client := NewClient(Options{StaleTime: time.Hour})
			t.Cleanup(client.Close)
			started, releaseBatch := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(releaseBatch) }) }
			t.Cleanup(unblock)
			releaseCaller := make(chan struct{})
			defer close(releaseCaller)
			departed, admitted := make(chan struct{}), make(chan struct{})
			var calls, attempts atomic.Int32
			b, _ := newTestBatcher(t, context.Background(),
				func(context.Context, []int) (map[int]BatchResult[int], error) {
					version := int(calls.Add(1))
					if version == 1 {
						close(started)
						<-releaseBatch
					}
					return map[int]BatchResult[int]{1: {Data: version}}, nil
				}, BatchOptions{MaxBatchSize: 1})
			fetch := func(ctx context.Context) (int, error) {
				if attempts.Add(1) == 1 {
					return b.Load(&batchReleaseContext{
						Context: ctx, departed: departed, release: releaseCaller,
					}, 1)
				}
				return b.Load(&batchWaitContext{Context: ctx, ready: admitted}, 1)
			}
			handle := Query(client, "product:1", fetch)
			t.Cleanup(handle.Close)
			receiveBatchValue(t, started)
			switch action {
			case "remove":
				client.Remove("product:1")
			case "clear":
				client.Clear()
			case "cancel":
				client.Cancel("product:1")
			case "set":
				Set(client, "product:1", 99)
			}
			receiveBatchValue(t, departed)
			result := make(chan BatchResult[int], 1)
			go func() {
				value, err := handle.Refetch(context.Background())
				result <- BatchResult[int]{Data: value, Err: err}
			}()
			receiveBatchValue(t, admitted)
			unblock()
			if got := receiveBatchValue(t, result); got.Data != 2 || got.Err != nil {
				t.Fatalf("replacement reused detached batch: %+v; callbacks = %d", got, calls.Load())
			}
			if state := handle.Snapshot(); state.Data != 2 || state.Stale || state.Fetching {
				t.Fatalf("replacement cache = %+v", state)
			}
		})
	}
}

// TestBatcherFetcherIntegration combines Query and Fetch misses while keeping fresh cache hits outside the scheduler.
func TestBatcherFetcherIntegration(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	var calls atomic.Int32
	keys := make(chan []int, 1)
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, ids []int) (map[int]BatchResult[int], error) {
			calls.Add(1)
			keys <- ids
			return map[int]BatchResult[int]{1: {Data: 11}, 2: {Data: 22}}, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	firstFetcher := b.Fetcher(1)
	if calls.Load() != 0 {
		t.Fatal("binding a fetcher started a callback")
	}
	first := Query(client, batchProductKey{ID: 1}, firstFetcher)
	t.Cleanup(first.Close)
	receiveBatchValue(t, timers)
	second := make(chan BatchResult[int], 1)
	go func() {
		value, err := Fetch(context.Background(), client, batchProductKey{ID: 2}, b.Fetcher(2))
		second <- BatchResult[int]{Data: value, Err: err}
	}()
	if ids := receiveBatchValue(t, keys); !reflect.DeepEqual(ids, []int{1, 2}) {
		t.Fatalf("combined query and fetch keys = %v", ids)
	}
	state := awaitState(t, first.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if state.Data != 11 || state.Err != nil {
		t.Fatalf("query state = %+v", state)
	}
	if got := receiveBatchValue(t, second); got.Data != 22 || got.Err != nil {
		t.Fatalf("fetch state = %+v", got)
	}
	// A closed scheduler would fail if a fresh cache hit accidentally invoked the fetcher.
	b.Close()
	for _, id := range []int{1, 2} {
		if value, err := Fetch(
			context.Background(),
			client,
			batchProductKey{ID: id},
			b.Fetcher(id),
		); value != id*11 ||
			err != nil {
			t.Fatalf("cached product %d = %d, %v", id, value, err)
		}
	}
	fresh := Query(client, batchProductKey{ID: 1}, b.Fetcher(1))
	t.Cleanup(fresh.Close)
	if state := fresh.Snapshot(); state.Data != 11 || state.Err != nil || state.Fetching || calls.Load() != 1 {
		t.Fatalf("fresh query = %+v; callbacks = %d", state, calls.Load())
	}
	if err := Prefetch(context.Background(), client, batchProductKey{ID: 1}, b.Fetcher(1)); err != nil {
		t.Fatalf("cached prefetch = %v", err)
	}
	if _, err := Fetch(
		context.Background(),
		client,
		batchProductKey{ID: 3},
		b.Fetcher(3),
	); !errors.Is(
		err,
		ErrBatcherClosed,
	) {
		t.Fatalf("uncached fetch with closed scheduler = %v", err)
	}
}

// TestBatcherFetcherInvalidation refreshes separately cached products together without replacing their subscriptions.
func TestBatcherFetcherInvalidation(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	var calls atomic.Int32
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, ids []int) (map[int]BatchResult[int], error) {
			version := int(calls.Add(1))
			results := make(map[int]BatchResult[int], len(ids))
			for _, id := range ids {
				results[id] = BatchResult[int]{Data: id*10 + version}
			}
			return results, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	first := Query(client, batchProductKey{ID: 1}, b.Fetcher(1))
	t.Cleanup(first.Close)
	receiveBatchValue(t, timers)
	second := Query(client, batchProductKey{ID: 2}, b.Fetcher(2))
	t.Cleanup(second.Close)
	for i, handle := range []*QueryHandle[int]{first, second} {
		state := awaitState(t, handle.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
		if state.Data != (i+1)*10+1 || state.Err != nil {
			t.Fatalf("initial query = %+v", state)
		}
	}
	if err := client.Invalidate([]any{batchProductKey{ID: 1}, batchProductKey{ID: 2}}); err != nil {
		t.Fatal(err)
	}
	receiveBatchValue(t, timers)
	for i, handle := range []*QueryHandle[int]{first, second} {
		want := (i+1)*10 + 2
		state := awaitState(t, handle.Updates(), func(s Snapshot[int]) bool { return s.Data == want && !s.Fetching })
		if state.Err != nil || state.Stale {
			t.Fatalf("refreshed query = %+v", state)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("bulk callbacks = %d; want initial and refresh batches", calls.Load())
	}
}

// TestBatcherFetcherRetries retries only the failed key through the Client's existing request policy.
func TestBatcherFetcherRetries(t *testing.T) {
	failure := errors.New("temporarily unavailable")
	client := NewClient(Options{StaleTime: time.Hour, Retry: 1, RetryDelay: func(int) time.Duration { return 0 }})
	t.Cleanup(client.Close)
	var calls atomic.Int32
	keys := make(chan []int, 1)
	b, timers := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, ids []int) (map[int]BatchResult[int], error) {
			keys <- ids
			if calls.Add(1) == 1 {
				return map[int]BatchResult[int]{1: {Data: 11}, 2: {Err: failure}}, nil
			}
			return map[int]BatchResult[int]{2: {Data: 22}}, nil
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	first := Query(client, batchProductKey{ID: 1}, b.Fetcher(1))
	t.Cleanup(first.Close)
	receiveBatchValue(t, timers)
	second := Query(client, batchProductKey{ID: 2}, b.Fetcher(2))
	t.Cleanup(second.Close)
	if ids := receiveBatchValue(t, keys); !reflect.DeepEqual(ids, []int{1, 2}) {
		t.Fatalf("initial keys = %v", ids)
	}
	awaitState(t, first.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	receiveBatchValue(t, timers).fire()
	if ids := receiveBatchValue(t, keys); !reflect.DeepEqual(ids, []int{2}) {
		t.Fatalf("retry keys = %v", ids)
	}
	state := awaitState(t, second.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if state.Data != 22 || state.Err != nil || calls.Load() != 2 || client.Stats().Retries != 1 {
		t.Fatalf("retry outcome = %+v; callbacks = %d; stats = %+v", state, calls.Load(), client.Stats())
	}
}

// TestBatcherFetcherCancellation preserves another query when an imperative load's owner leaves the batch.
func TestBatcherFetcherCancellation(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	started := make(chan context.Context, 1)
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
				return nil, ctx.Err()
			case <-release:
				return map[int]BatchResult[int]{1: {Data: 11}, 2: {Data: 22}}, nil
			}
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := Fetch(ctx, client, batchProductKey{ID: 1}, b.Fetcher(1))
		first <- err
	}()
	receiveBatchValue(t, timers)
	second := Query(client, batchProductKey{ID: 2}, b.Fetcher(2))
	t.Cleanup(second.Close)
	batchCtx := receiveBatchValue(t, started)
	cancel()
	if err := receiveBatchValue(t, first); !errors.Is(err, context.Canceled) || batchCtx.Err() != nil {
		t.Fatalf("fetch cancellation = %v; callback error = %v", err, batchCtx.Err())
	}
	unblock()
	state := awaitState(t, second.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if state.Data != 22 || state.Err != nil {
		t.Fatalf("surviving query = %+v", state)
	}
}

// TestBatcherFetcherLocalWrite preserves a local replacement while the bulk callback serves another key.
func TestBatcherFetcherLocalWrite(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	started := make(chan context.Context, 1)
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
				return nil, ctx.Err()
			case <-release:
				return map[int]BatchResult[int]{1: {Data: 11}, 2: {Data: 22}}, nil
			}
		},
		BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	first := Query(client, batchProductKey{ID: 1}, b.Fetcher(1))
	t.Cleanup(first.Close)
	receiveBatchValue(t, timers)
	second := Query(client, batchProductKey{ID: 2}, b.Fetcher(2))
	t.Cleanup(second.Close)
	batchCtx := receiveBatchValue(t, started)
	if err := Set(client, batchProductKey{ID: 1}, 99); err != nil {
		t.Fatal(err)
	}
	state := awaitState(t, first.Updates(), func(s Snapshot[int]) bool { return s.Data == 99 && !s.Fetching })
	if state.Err != nil || batchCtx.Err() != nil {
		t.Fatalf("local replacement = %+v; batch error = %v", state, batchCtx.Err())
	}
	unblock()
	state = awaitState(t, second.Updates(), func(s Snapshot[int]) bool { return s.HasData && !s.Fetching })
	if state.Data != 22 || state.Err != nil || first.Snapshot().Data != 99 {
		t.Fatalf("bulk completion = %+v; replaced query = %+v", state, first.Snapshot())
	}
}

// TestBatcherFetcherConsumerOptions preserves independent freshness decisions through FetchWithOptions.
func TestBatcherFetcherConsumerOptions(t *testing.T) {
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	var calls atomic.Int32
	b, _ := newTestBatcher(
		t,
		context.Background(),
		func(_ context.Context, keys []int) (map[int]BatchResult[int], error) {
			return map[int]BatchResult[int]{keys[0]: {Data: int(calls.Add(1))}}, nil
		},
		BatchOptions{MaxBatchSize: 1},
	)
	fetch := b.Fetcher(1)
	key := batchProductKey{ID: 1}
	for range 2 {
		if value, err := Fetch(context.Background(), client, key, fetch); value != 1 || err != nil {
			t.Fatalf("default cache decision = %d, %v", value, err)
		}
	}
	var immediate time.Duration
	value, err := FetchWithOptions(
		context.Background(),
		client,
		key,
		fetch,
		FetchOptions{StaleTime: &immediate},
	)
	if value != 2 || err != nil || calls.Load() != 2 || Get[int](client, key).Stale {
		t.Fatalf("consumer cache decision = %d, %v; callbacks = %d", value, err, calls.Load())
	}
}

// TestBatcherFetcherScopedKeys keeps authorization scopes independent even when product IDs match.
func TestBatcherFetcherScopedKeys(t *testing.T) {
	// tenantProductKey identifies a product within its authorization scope.
	type tenantProductKey struct {
		// Tenant selects the identity scope used by the business callback.
		Tenant string
		// ID identifies the product within that scope.
		ID int
	}
	client := NewClient(Options{StaleTime: time.Hour})
	t.Cleanup(client.Close)
	for _, tenant := range []string{"alice", "bob"} {
		b, _ := newTestBatcher(
			t,
			context.Background(),
			func(_ context.Context, ids []int) (map[int]BatchResult[string], error) {
				return map[int]BatchResult[string]{ids[0]: {Data: tenant}}, nil
			},
			BatchOptions{MaxBatchSize: 1},
		)
		value, err := Fetch(context.Background(), client, tenantProductKey{Tenant: tenant, ID: 42}, b.Fetcher(42))
		if value != tenant || err != nil {
			t.Fatalf("tenant %s = %q, %v", tenant, value, err)
		}
	}
}
