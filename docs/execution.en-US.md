# Execution concurrency budgets

[简体中文](execution.zh-CN.md) · [Batch loading](batching.en-US.md) · [Mutations](mutations.en-US.md)

`Executor` limits concurrently executing callbacks and queued work. Share an instance across clients, mutation handles, or batchers that access the same backend. It does not impose a requests-per-second rate, persist jobs, or create a background worker pool.

## Runnable example

Save this program in a Go module that imports cacheq and run `go run .`.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main shares a bounded executor with cached query loads.
func main() {
	budget, err := cacheq.NewExecutor(cacheq.ExecutionOptions{
		MaxConcurrent: 4,
		MaxQueued:     16,
	})
	if err != nil {
		panic(err)
	}
	client := cacheq.NewClient(cacheq.Options{
		Executor:  budget,
		StaleTime: time.Minute,
	})
	defer client.Close()
	value, err := cacheq.Fetch(
		context.Background(),
		client,
		"answer",
		func(context.Context) (int, error) { return 42, nil },
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	fmt.Println(budget.Stats().Active, budget.Stats().Queued)
}
```

Expected output:

```text
42
0 0
```

## Configuration and ownership

| Configuration | Contract |
| --- | --- |
| `ExecutionOptions.MaxConcurrent` | Positive maximum concurrently executing callbacks |
| `ExecutionOptions.MaxQueued` | Non-negative waiting capacity; zero rejects work immediately when all slots are occupied |
| `Options.Executor` | Optional shared query budget; nil preserves unrestricted execution |
| `BatchOptions.Executor` | Optional budget around each actual bulk callback, after collection |
| `MutationOptions.Executor` | Optional write budget; nil inherits the Client budget |

Invalid capacities return `ErrInvalidExecutionOptions` from `NewExecutor`. Construct budgets with this constructor; the zero value is not a configured budget. Options are copied and capacities are immutable. An Executor owns no timers or goroutines and needs no `Close`. Clients and caller contexts retain lifecycle ownership.

## Admission, cancellation, and retries

- Fresh cache hits and same-key request joiners consume no additional slot.
- Waiting callers receive slots in FIFO order. If both running and queue capacity are exhausted, admission returns `ErrQueueFull` without invoking the callback. Applications can detect it with `errors.Is`.
- Client retries do not retry `ErrQueueFull`, including wrapped errors returned by batchers or application fetchers. `RetryIf` is not called for this error.
- Each query loader attempt acquires a slot. The slot is released before retry predicates and backoff; a subsequent attempt competes for capacity again.
- Queue time counts toward the query's existing overall timeout or caller deadline. Queued caller cancellation, `Client.Cancel`, and Client shutdown remove waiting query work.
- Cancellation does not release a running slot until its callback actually returns. Otherwise an uncooperative callback could let actual concurrency exceed the budget.
- Mutation holds capacity only for its write callback. It releases capacity before local synchronization and invalidation. Caller context deadlines apply; Client query timeout and retry defaults do not apply to writes.
- Waiting work is visible as `Fetching` for queries and `Pending` for mutations. No additional queue status is introduced into existing snapshots.

## Batch placement and nested calls

When Client fetchers call `Batcher.Fetcher`, prefer configuring only `BatchOptions.Executor`. This allows independent keys to enter the collection window while limiting actual backend batch calls. `MaxBatchSize` continues to limit unique keys per batch, not callback concurrency.

Multiple batchers can share one Executor. A rejected batch returns `ErrQueueFull` to all its keys. Closing a batcher or canceling all of a batch's waiters cancels that batch's queue wait.

Do not install the same Executor on both the outer Client and its Batcher: outer loads could hold all slots while waiting for an inner callback to acquire one. The same rule applies to recursive fetches inside a loader or mutator using the same budget. Put the budget at the backend boundary, or use independent budgets with an explicit dependency order. Limiting outer per-key fetchers can also reduce batch sizes.

## Statistics and resource boundaries

`Executor.Stats()` reports `Active`, `Queued`, and cumulative `Rejected` counts. Active includes acquired slots handed to queued callers and callbacks that have not returned after cancellation. Queued can briefly include canceled callers until cleanup executes. Counts belong to the shared Executor, not an individual Client.

Client `Stats().Loads` still counts logical shared loads, including those waiting or rejected. `Retries` and diagnostic `Attempts` count callbacks actually entered at the Client boundary; a query rejected before its first attempt has zero attempts. A batch-fetcher invocation is not a bulk backend call. Mutation execution is not included in Client query statistics.

The budget bounds callback concurrency and its admission queue. It does not bound cached bytes, subscriptions, the batch collection window, transient goroutines, or the cost of an individual callback. Continue configuring cache retention and honoring context cancellation.

## Verification

```sh
go test -race ./... -run 'Test(Executor|Execution|BatcherExecution|MutationExecution)' -count=1
```

[Executor tests](../executor_test.go), [query integration](../execution_test.go), [batch integration](../batcher_execution_test.go), and [mutation integration](../mutation_execution_test.go) verify overload, FIFO order, cancellation races, shared budgets, batch placement, and release before synchronization or retry backoff.
