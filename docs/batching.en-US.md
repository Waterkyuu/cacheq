# Automatic batch loading

[简体中文](batching.zh-CN.md)

`Batcher[K, V]` collects concurrent loads for different keys and calls a business-supplied `BatchFunc` with a list of unique keys. For example, a callback can use one SQL `WHERE id IN (...)` query or a bulk HTTP endpoint. cacheq does not discover backend capabilities or generate SQL. A callback that loops over single-object requests still makes multiple backend calls.

## Complete runnable example

This program needs no server or credentials. The long collection window deliberately makes the example independent of machine speed: two concurrent keys reach `MaxBatchSize` and dispatch immediately. In an application, choose a short window appropriate for the backend latency budget.

```go
package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main demonstrates two concurrent callers sharing one bulk callback.
func main() {
	products, err := cacheq.NewBatcher(
		context.Background(),
		func(_ context.Context, ids []int) (map[int]cacheq.BatchResult[string], error) {
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

	ids := []int{42, 43}
	values := make([]string, len(ids))
	var callers sync.WaitGroup
	for i, id := range ids {
		callers.Add(1)
		go func() {
			defer callers.Done()
			value, err := products.Load(context.Background(), id)
			if err != nil {
				panic(err)
			}
			values[i] = value
		}()
	}
	callers.Wait()
	fmt.Println(values)
}
```

Expected output:

```text
bulk load: [42 43]
[product 42 product 43]
```

## Scheduling and ownership

- `Wait` starts when the first unique key enters an empty collection window. Zero adds no intentional delay. Sequential calls that each wait for their result cannot form a batch together.
- `MaxBatchSize` must be positive and counts unique keys. Reaching it dispatches immediately. Distinct batches may run concurrently; this option is not a global concurrency limit.
- Duplicate keys share queued or running work. A completed result is not cached by the batcher, so a subsequent `Load` starts another operation.
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

[Behavior tests](../batcher_test.go) inject a controlled timer and synchronize callers through channels, covering collection deadlines, size limits, shared work, partial errors, missing results, cancellation, ownership, closure, and late callbacks.
