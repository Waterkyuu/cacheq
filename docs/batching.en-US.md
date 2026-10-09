# Automatic batch loading

[简体中文](batching.zh-CN.md)

`Batcher[K, V]` collects concurrent loads for different keys and calls a business-supplied `BatchFunc` with a list of unique keys. For example, a callback can use one SQL `WHERE id IN (...)` query or a bulk HTTP endpoint. cacheq does not discover backend capabilities or generate SQL. A callback that loops over single-object requests still makes multiple backend calls.

`products.Fetcher(id)` binds one batch key into an existing `Fetcher[V]` without starting a query. Pass that function to `Fetch`, `FetchWithOptions`, `Query`, or `Prefetch`. The Client decides whether to reuse fresh data or invoke the function; only required loads enter the batcher's collection window. Each Client key retains its own cache entry and subscriptions. Existing API signatures remain unchanged.

## Complete runnable example

This program needs no server or credentials. The long collection window deliberately makes the example independent of machine speed: two concurrent keys reach `MaxBatchSize` and dispatch immediately. In an application, choose a short window appropriate for the backend latency budget.

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

Expected output:

```text
bulk load: [42 43]
product 42
product 43
cached: product 42; bulk calls: 1
```

The two `Query` handles start loads in the background, allowing IDs 42 and 43 to enter the same batch. The final `Fetch` returns product 42 from the Client's fresh cache without another callback or collection delay. `"product:42"` is the Client cache key; `42` is the key passed to the bulk function.

For a standalone load without the Client's cache, use `products.Load(ctx, id)`. Binding a fetcher is equivalent to `func(ctx context.Context) (V, error) { return products.Load(ctx, id) }`.

## Scheduling and ownership

- `Wait` starts when the first unique key enters an empty collection window. Zero adds no intentional delay. Sequential calls that each wait for their result cannot form a batch together.
- `MaxBatchSize` must be positive and counts unique keys. Reaching it dispatches immediately. Distinct batches may run concurrently; this option is not a global concurrency limit.
- Duplicate keys share queued or running work. A completed result is not cached by the batcher, so a subsequent `Load` starts another operation.
- Cache freshness, retries, and per-key load timeouts remain Client policies. A failed key may re-enter a later batch through the Client's retry loop; successful keys are not retried with it. Batch collection time is included in that key's timeout. Client statistics count per-key shared loads, not bulk callback invocations.
- Existing invalidation can refresh multiple observed keys through the same batcher. `Set`, `Remove`, and cancellation retain their existing late-result protection. Invalidating one key does not invalidate other keys merely because they shared a bulk callback.
- The constructor context owns callback values, deadlines, and cancellation. Individual callers control their own wait, without contributing context values or deadlines to the shared callback.
- Canceling one caller preserves other callers. A queued key with no remaining callers is removed; an active callback is canceled when all of its keys lose their callers.
- `Close` is idempotent, releases waiters with `ErrBatcherClosed`, and signals active callbacks to stop. It does not wait for callbacks that ignore their context. Parent cancellation releases waiters with the parent context's error.
- Use separate batchers for different data sources and authorization scopes. Include all identity and query parameters in keys rather than depending on an individual caller's context. Callbacks must be safe for concurrent batches and treat their returned data as immutable once published.

## Results and errors

Return `map[K]BatchResult[V]` to match values by key, independently of response order. Present map entries with zero-valued `Data` are valid results.

| Condition | Outcome |
| --- | --- |
| A single-object function is passed as the callback | Go compile-time type mismatch |
| Nil callback | Constructor returns `ErrNoFetcher` |
| Negative `Wait` or non-positive `MaxBatchSize` | Constructor returns `ErrInvalidBatchOptions` |
| Whole callback returns an error | Every requested key receives that error; the map is ignored |
| One map entry contains `Err` | Only that key receives the entry's data and error |
| A requested key is absent from a successful map | Its caller receives a wrapped `ErrBatchResultMissing` |
| An extra key is returned | It is ignored |
| A caller is canceled or its deadline expires | `context.Canceled` or `context.DeadlineExceeded` |
| A callback finishes after abandonment or closure | Its result is discarded |

Use `errors.Is` to inspect library and application errors. Missing business objects should have explicit per-key application errors; an omitted map entry represents an incomplete callback response. Nil, dynamically non-comparable, and non-reflexive keys (such as NaN) return `ErrInvalidKey`.

## Verification

```sh
go test -race ./... -run TestBatcher -count=1
```

[Behavior tests](../batcher_test.go) inject a controlled timer and synchronize callers through channels, covering collection deadlines, size limits, shared work, partial errors, missing results, cancellation, ownership, closure, and late callbacks. [Client integration tests](../batcher_query_test.go) cover mixed Query/Fetch batching, cache-hit bypass, invalidation, per-key retries, cancellation, and scoped cache keys. The [executable API example](../batcher_example_test.go) verifies the output above.
