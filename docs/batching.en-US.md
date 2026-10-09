# Automatic batch loading

[简体中文](batching.zh-CN.md)

`Batcher[K, V]` collects concurrent loads for different keys and invokes a business-supplied `BatchFunc`. It supports data sources with bulk operations, such as SQL `WHERE id IN (...)` queries and bulk HTTP endpoints.

The application implements the bulk operation. cacheq does not discover backend capabilities or generate SQL. A callback that loops over single-object requests does not reduce backend call counts.

## Quick start

This example loads two products through separate `Query` handles with one shared bulk callback. Save it as `main.go` in a module importing cacheq, then run `go run .`.

The example uses `Wait: time.Hour` and `MaxBatchSize: 2` so the size limit triggers dispatch without relying on a short timing window. Application settings should use a collection duration that fits the required load latency.

```go
package main

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main groups different query keys and reuses their individually cached results.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	var calls atomic.Int32
	products, err := cacheq.NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]cacheq.BatchResult[string], error) {
			calls.Add(1)
			sort.Ints(ids)
			fmt.Printf("bulk load: %v\n", ids)
			results := make(map[int]cacheq.BatchResult[string], len(ids))
			for _, id := range ids {
				results[id] = cacheq.BatchResult[string]{Data: fmt.Sprintf("product %d", id)}
			}
			return results, nil
		},
		cacheq.BatchOptions{Wait: time.Hour, MaxBatchSize: 2},
	)
	if err != nil {
		panic(err)
	}
	defer products.Close()

	first := cacheq.Query(client, "product:42", products.Fetcher(42))
	defer first.Close()
	second := cacheq.Query(client, "product:43", products.Fetcher(43))
	defer second.Close()
	for _, handle := range []*cacheq.QueryHandle[string]{first, second} {
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
	value, err := cacheq.Fetch(context.Background(), client, "product:42", products.Fetcher(42))
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

## Query API integration

`products.Fetcher(42)` returns a `Fetcher[V]` bound to batch key `42`. Creating the function starts no work. Pass it to `Fetch`, `FetchWithOptions`, `Query`, or `Prefetch`.

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
| `Fetcher(key)` | Returns a loader for existing query APIs without starting work |
| `Close()` | Releases waiters and cancels active batches; repeated calls are harmless |

`BatchFunc[K, V]` has the signature `func(context.Context, []K) (map[K]BatchResult[V], error)`. Construction and `Load` require non-nil contexts.

| `BatchOptions` field | Default or constraint | Contract |
| --- | --- | --- |
| `Wait` | Zero means 0; cannot be negative | Starts with the first unique key in an empty window; zero adds no intentional collection delay |
| `MaxBatchSize` | Must be positive; no default | Maximum unique keys per batch; reaching it dispatches immediately |

`MaxBatchSize` is not a callback concurrency limit. Distinct batches can run concurrently, so callbacks must support concurrent execution. Sequential requests that each wait for completion cannot form one batch.

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

Invalidation can refresh several observed keys through one batcher. Invalidating one key does not invalidate others that shared its batch. `Set`, `Remove`, and cancellation retain protection against late results overwriting newer state.

`Stats()` and [diagnostic events](events.en-US.md) count per-key loads, not bulk callbacks or database calls. Count actual bulk invocations inside the business callback when needed.
