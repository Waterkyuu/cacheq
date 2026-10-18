# Automatic batch loading

[简体中文](batching.zh-CN.md)

`Batcher[K, V]` collects concurrent loads for different keys and invokes a business-supplied `BatchFunc`. It supports data sources with bulk operations, such as SQL `WHERE id IN (...)` queries and bulk HTTP endpoints.

The application implements the bulk operation. soroi does not discover backend capabilities or generate SQL. A callback that loops over single-object requests does not reduce backend call counts.

## Quick start

This example loads two products through separate `Query` handles with one shared bulk callback. Save it as `main.go` in a module importing soroi, then run `go run .`.

The example uses `Wait: time.Hour` and `MaxBatchSize: 2` so the size limit triggers dispatch without relying on a short timing window. Application settings should use a collection duration that fits the required load latency.

```go
package main

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main groups different query keys and reuses their individually cached results.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer client.Close()
	var calls atomic.Int32
	products, err := soroi.NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]soroi.BatchResult[string], error) {
			calls.Add(1)
			sort.Ints(ids)
			fmt.Printf("bulk load: %v\n", ids)
			results := make(map[int]soroi.BatchResult[string], len(ids))
			for _, id := range ids {
				results[id] = soroi.BatchResult[string]{Data: fmt.Sprintf("product %d", id)}
			}
			return results, nil
		},
		soroi.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		panic(err)
	}
	defer products.Close()

	first := soroi.Query(client, "product:42", products.Fetcher(42))
	defer first.Close()
	second := soroi.Query(client, "product:43", products.Fetcher(43))
	defer second.Close()
	for _, handle := range []*soroi.QueryHandle[string]{first, second} {
		for state := range handle.Updates() {
			if state.Err != nil {
				panic(state.Err)
			}
			if state.HasData && !state.Fetching {
				fmt.Println(state.Data)
				break
			}
		}
	}
	value, err := soroi.Fetch(context.Background(), client, "product:42", products.Fetcher(42))
	if err != nil {
		panic(err)
	}
	fmt.Printf("cached: %s; bulk calls: %d\n", value, calls.Load())
}
```

Output:

```text
bulk load: [42 43]
product 42
product 43
cached: product 42; bulk calls: 1
```

The two `Query` handles load in the background, placing IDs 42 and 43 into one batch. The final `Fetch` hits product 42's cache without another bulk callback.

## Load a known set of keys

Use `LoadMany(ctx, keys)` when the complete key list is already available. It returns `[]BatchResult[V]` with the same length and order as the input, so each position carries its own data or error. No per-key goroutines or outer error handling are required.

Save this example as `main.go` in a module importing soroi and run `go run .`:

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main loads known product IDs and prints outcomes in input order.
func main() {
	var calls atomic.Int32
	products, err := soroi.NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]soroi.BatchResult[string], error) {
			calls.Add(1)
			results := make(map[int]soroi.BatchResult[string], len(ids))
			for _, id := range ids {
				if id == 99 {
					results[id] = soroi.BatchResult[string]{Err: errors.New("product not found")}
					continue
				}
				results[id] = soroi.BatchResult[string]{Data: fmt.Sprintf("product %d", id)}
			}
			return results, nil
		},
		soroi.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
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
}
```

Output:

```text
43: product 43
42: product 42
43: product 43
99: product not found
bulk calls: 2
```

The three unique IDs are split into two callbacks by `MaxBatchSize: 2`. Both positions for ID 43 share one result. ID 99 fails individually, while the other positions retain their data.

### LoadMany contract

- All valid keys are registered before waiting. Duplicate keys within the call share one load, even when they cross a batch boundary.
- Newly queued work is dispatched immediately, including the final partial batch. `Wait` continues to control independent `Load` calls; `LoadMany` may also dispatch keys already queued by those calls.
- Queued or running work for the same key is shared with other callers. Separate batches may execute concurrently and finish out of order. `MaxBatchSize` and `Executor` retain their existing responsibilities.
- `nil` and empty input return a non-nil empty slice without starting or flushing work. Invalid keys receive `ErrInvalidKey` in their positions; valid keys still load. An already-canceled caller receives `ctx.Err()` in every position before key validation.
- Cancellation preserves outcomes already published by the batcher, including data and per-key errors. Unfinished positions receive `ctx.Err()`, and this call releases its interest in all keys. Other callers can keep shared work alive. No atomic snapshot across batches is promised.
- Closure, lifetime cancellation, budget admission failures, and whole-callback errors are returned in the affected positions. There is no separate aggregate error.
- Completed results are not cached. To retain Client caching and query subscriptions, use `Fetcher` with the existing query APIs.

## Query API integration

`products.Fetcher(42)` returns a `Fetcher[V]` bound to batch key `42`. Creating the function starts no work. Pass it to `Fetch`, `Query`, or `Prefetch`.

The Client first decides whether cached data is reusable. Only required loads invoke the Fetcher and enter the collection window. Each Client key retains independent cached data and subscriptions.

| Key type | Example | Responsibility |
| --- | --- | --- |
| Client cache key | `"product:42"` | Identifies cached data, subscriptions, and invalidation scope |
| Batch key | `42` | Identifies the object passed to the bulk callback and matches its result |

Use `products.Load(ctx, id)` without Client caching. The batcher shares queued and active work without retaining completed results.

## API and options

| API | Contract |
| --- | --- |
| `NewBatcher(ctx, load, options)` | Creates a batcher whose lifetime is owned by `ctx`, using the supplied bulk callback |
| `Load(ctx, key)` | Creates or joins a key's load and waits for its result |
| `LoadMany(ctx, keys)` | Registers known keys, dispatches queued work immediately, and returns ordered per-position outcomes |
| `Fetcher(key)` | Returns a loader for existing query APIs without starting work |
| `Close()` | Releases waiters and cancels active batches; repeated calls are harmless |

`BatchFunc[K, V]` has the signature `func(context.Context, []K) (map[K]BatchResult[V], error)`. Construction, `Load`, and `LoadMany` require non-nil contexts.

| `BatchOptions` field | Default or constraint | Contract |
| --- | --- | --- |
| `Wait` | Zero means 0; cannot be negative | Starts with the first unique key in an empty window; zero adds no intentional collection delay |
| `MaxBatchSize` | Must be positive; no default | Maximum unique keys per batch; reaching it dispatches immediately |
| `Executor` | nil by default | Optional shared limit for active and queued bulk callbacks |

`MaxBatchSize` is not a callback concurrency limit. Distinct batches can run concurrently; use `BatchOptions.Executor` to bound active and queued callbacks. See [execution budgets](execution.en-US.md) for placement and cancellation rules. Sequential requests that each wait for completion cannot form one batch.

## Results

The callback returns `map[K]BatchResult[V]`, matching outcomes by key independently of response order.

| `BatchResult[V]` field | Contract |
| --- | --- |
| `Data` | Per-key data; zero-valued data is valid |
| `Err` | Per-key error affecting only that key |

A whole-callback error is returned to every requested key, ignoring the map. On whole-callback success, omitted keys receive a wrapped `ErrBatchResultMissing`; extra keys are ignored.

Missing business objects should return explicit per-key application errors. Omitting an entry indicates an incomplete callback response rather than an absent object.

## Errors

| Condition | Error or outcome |
| --- | --- |
| Nil bulk callback | `ErrNoFetcher` |
| Negative `Wait` or non-positive `MaxBatchSize` | `ErrInvalidBatchOptions` |
| Parent context canceled before construction | Parent context error |
| Nil, dynamically non-comparable, or non-reflexive key such as NaN | `ErrInvalidKey` |
| Successful response omits a requested key | Wrapped `ErrBatchResultMissing` |
| Caller cancellation or deadline | `context.Canceled` or `context.DeadlineExceeded` |
| Explicitly closed batcher | `ErrBatcherClosed` |
| Full execution budget and queue | `ErrQueueFull` for every key in the rejected batch |
| Whole-callback or per-key application failure | Corresponding application error |

Use `errors.Is` to inspect library and application errors. Passing a single-object callback instead of a `BatchFunc` produces a Go compile-time type mismatch. There is no runtime discovery or automatic single-object fallback.

## Cancellation and lifecycle

The constructor context supplies callback values, deadlines, and cancellation. Individual callers control their own wait without passing their values or deadlines into the shared callback.

- Canceling one caller preserves other callers. A queued key is removed once all its waiters leave.
- An active callback is canceled when every key in its batch loses its waiters.
- `Close()` releases waiters with `ErrBatcherClosed` and cancels active callbacks without waiting for callbacks that ignore cancellation.
- Parent cancellation releases waiters with the parent's context error.
- Results arriving after abandonment or closure are discarded.

Use separate batchers for data sources and authorization scopes. Client and batch keys must include identity and query parameters that affect results, rather than relying on an individual caller's private context. Treat shared results as immutable.

## Cache policy interaction

The Client owns freshness, retries, and per-key load timeouts. Collection waiting counts toward the key's timeout. Failed keys can enter later retry batches without reloading successful keys.

`ErrBatcherClosed` is terminal for Client loads, including wrapped errors. Closing a batcher ends affected loads without retry attempts, backoff, or calls to `RetryIf`. Further backend work requires a new batcher; retrying a closed scheduler cannot recover.

Invalidation can refresh several observed keys through one batcher. Invalidating one key does not invalidate others that shared its batch. `Set`, `Remove`, and cancellation retain protection against late results overwriting newer state.

`Stats()` and [diagnostic events](events.en-US.md) count per-key loads, not bulk callbacks or database calls. Count actual bulk invocations inside the business callback when needed.

`ErrQueueFull` is also terminal for the current Client load, including wrapped errors, and bypasses `RetryIf`. Batch admission failure does not invoke the bulk callback.

When the Client and Batcher share an Executor, use `QueryOptions{DisableExecutor: true}` for batched queries. Ordinary queries retain the Client budget, while bulk callbacks acquire the Batcher budget. Without this override, an outer callback holding the same budget receives `ErrExecutionReentry` before batch collection; the Client does not retry it. See [execution budgets](execution.en-US.md) for context propagation requirements.
