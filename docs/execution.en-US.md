# Execution concurrency budgets

[简体中文](execution.zh-CN.md) · [Batch loading](batching.en-US.md) · [Mutations](mutations.en-US.md)

`Executor` limits concurrent loads or writes and bounds the queue of work waiting to execute. Use it to protect databases or HTTP services with limited concurrency. Multiple Clients, Mutations, or Batchers accessing one data source can share an Executor.

## Quick start

This example permits one running load and no queued work. While product 42's load holds the slot, product 43's load is rejected. Completing the first load releases the slot.

Save the complete program as `main.go` in a module importing soroi, then run `go run .`. Channels control when the first load completes, requiring no external service or short timing-dependent waits.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main rejects a second product load while the only execution slot is occupied.
func main() {
	budget, err := soroi.NewExecutor(soroi.ExecutionOptions{
		MaxConcurrent: 1,
		MaxQueued:     0,
	})
	if err != nil {
		panic(err)
	}
	client := soroi.NewClient(soroi.Options{
		Executor:  budget,
		StaleTime: time.Minute,
	})
	defer client.Close()

	started := make(chan struct{})
	release := make(chan struct{})
	loadFirst := func(ctx context.Context) (string, error) {
		close(started)
		select {
		case <-release:
			return "product 42", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	first := soroi.Query(client, "product:42", loadFirst)
	defer first.Close()
	<-started

	_, err = soroi.Fetch(
		context.Background(),
		client,
		"product:43",
		func(context.Context) (string, error) { return "product 43", nil },
	)
	fmt.Println("second rejected:", errors.Is(err, soroi.ErrQueueFull))
	fmt.Println("active:", budget.Stats().Active)

	close(release)
	value, err := soroi.Fetch(context.Background(), client, "product:42", loadFirst)
	if err != nil {
		panic(err)
	}
	fmt.Println("first:", value)
	stats := budget.Stats()
	fmt.Printf("finished: active=%d queued=%d rejected=%d\n", stats.Active, stats.Queued, stats.Rejected)
}
```

Expected output:

```text
second rejected: true
active: 1
first: product 42
finished: active=0 queued=0 rejected=1
```

`Options.Executor` applies the budget to Client loaders. The first `Query` loads in the background; `started` ensures it already holds the only slot. Another key then needs loading, but `MaxQueued: 0` permits no waiting. `Fetch` returns `ErrQueueFull` without invoking product 43's loader.

Closing `release` allows the first load to complete. The final `Fetch` shares product 42's request or cached data without another loader invocation. `Rejected` retains the cumulative count, so it remains 1 afterward.

## Snippet prerequisites

Place these snippets inside a business function returning `error`, importing the standard library packages they use. `budget` is an Executor created by `NewExecutor`, and `ctx` is a non-nil caller context. The batch example also needs an application-supplied `loadProducts` function whose signature is described in that section. Configuration snippets are independent.

## `NewExecutor`: concurrency and queue capacity

Set a positive `MaxQueued` to allow some requests to wait:

```go
budget, err := soroi.NewExecutor(soroi.ExecutionOptions{
	MaxConcurrent: 4,
	MaxQueued:     16,
})
if err != nil {
	return err
}
```

This permits up to 4 executing callbacks and 16 waiting tasks. When running slots are occupied, new work enters the queue. A full queue immediately returns `ErrQueueFull`. Waiters receive released slots in the order they entered the queue.

| `ExecutionOptions` field | Constraint | Purpose |
| --- | --- | --- |
| `MaxConcurrent` | Must be positive | Maximum callbacks executing at once |
| `MaxQueued` | Must be non-negative | Waiting capacity; zero rejects work when no slot is free |

Construct budgets with `NewExecutor`; do not use an unconfigured zero value. Capacity cannot change after construction. An Executor owns no background goroutines or timers and needs no `Close`.

## Sharing a budget across Clients

Pass the same `budget` to multiple Clients:

```go
first := soroi.NewClient(soroi.Options{Executor: budget})
second := soroi.NewClient(soroi.Options{Executor: budget})
defer first.Close()
defer second.Close()
```

Each Client keeps its own cache, but both acquire slots from the same budget. With the quick start's one-slot budget, both Clients together can execute only one callback. Creating two Executors instead gives two independent limits.

Create and inject budgets per data source at the application boundary. Closing one Client cancels work it owns without closing the shared Executor or other Clients.

## Configuration placement

| Configuration field | Scope | Default behavior |
| --- | --- | --- |
| `Options.Executor` | Each Client loader attempt | nil, unrestricted execution |
| `QueryOptions.Executor` | Loads initiated by this call or handle | nil, inherits the Client Executor |
| `QueryOptions.DisableExecutor` | Skip the outer Client loader budget | false, retain inheritance or override |
| `MutationOptions.Executor` | Mutation business write callback | nil, inherits the Client Executor |
| `BatchOptions.Executor` | Actual bulk callback after collection | nil, unrestricted execution |

Cache hits invoke no loader and consume no slot. Multiple same-key calls share one load and one slot. Different-key loads acquire slots independently.

`DisableExecutor: true` takes precedence over `Executor`. It affects only the outer loader; a Batcher's own budget remains active. Shared loads retain their initiator's budget, including when a later caller disables its own outer budget. Query handles retain this configuration for automatic loading, enablement, invalidation, and manual refresh.

Mutation releases capacity when its business callback returns, before local cache synchronization and associated invalidation. Set `MutationOptions.Executor` explicitly to give writes a separate budget. Nil means inheritance and cannot disable an already-configured Client budget.

## Using budgets with batch loading

When a Client loads through `Batcher.Fetcher`, prefer configuring `BatchOptions.Executor`. Different keys can then collect into batches before actual backend calls acquire capacity.

The snippet's application-supplied `loadProducts` has the signature `func(context.Context, []int) (map[int]soroi.BatchResult[string], error)`. See [automatic batch loading](batching.en-US.md) for a complete bulk callback example.

```go
products, err := soroi.NewBatcher(ctx, loadProducts, soroi.BatchOptions{
	Wait:         2 * time.Millisecond,
	MaxBatchSize: 100,
	Executor:     budget,
})
if err != nil {
	return err
}
defer products.Close()

client := soroi.NewClient(soroi.Options{StaleTime: time.Minute, Executor: budget})
defer client.Close()
value, err := soroi.Fetch(ctx, client, "product:42", products.Fetcher(42), soroi.QueryOptions{
	DisableExecutor: true,
})
if err != nil {
	return err
}
fmt.Println(value)
```

`MaxBatchSize` limits unique keys per batch; Executor limits concurrently executing bulk callbacks. A rejected batch returns `ErrQueueFull` to every key without invoking the bulk callback.

This Client can use the same budget for ordinary queries and actual bulk callbacks. The batched Fetch skips outer acquisition, allowing keys to collect without holding per-key slots. Ordinary queries without overrides still acquire the Client budget.

For subscribed batched queries, configure the corresponding handle:

```go
query := soroi.Query(client, "product:43", products.Fetcher(43), soroi.QueryOptions{
	DisableExecutor: true,
})
defer query.Close()
```

If an outer callback already holds the Batcher's Executor, `Load` returns `ErrExecutionReentry` before collecting or joining a key. The Client does not retry this error, even when wrapped, and does not call `RetryIf`. Nested query and mutation acquisitions through the callback context are also rejected. Rejections do not increase `Stats().Rejected`, which counts overload only.

`Fetch`, `Prefetch`, and `QueryHandle.Refetch` also reject synchronous joins when the callback context holds the existing load's Executor. The check uses the initiator's budget, so overriding or disabling the joining caller's budget does not bypass it. Fresh cache hits remain available without acquiring capacity; joins to loads using a different or disabled Executor remain permitted.

Detection depends on propagating the supplied callback context. Replacing it with `context.Background()` or starting independently owned background work loses that ownership information. Avoid nested dependencies on a held budget; configure capacity at the actual backend boundary. Separate budgets remain independently enforced and can still form dependency cycles if configured at multiple waiting layers.

## Overload and retries

Use `errors.Is(err, soroi.ErrQueueFull)` to detect a full budget. The callback at this boundary has not executed. The application can report that it is busy or arrange a later call at a higher layer.

The Client does not retry `ErrQueueFull` or call `RetryIf` for it, including wrapped errors returned by batch or application loaders.

Other retryable query errors retain the existing retry policy. Each attempt acquires capacity again. Capacity is released after the callback, before retry decisions and delays, so backoff consumes no execution slot.

## Cancellation and timeouts

Queue time counts toward the query's overall timeout. Cancellation by the caller owning the load, `Client.Cancel`, or Client closure cancels the corresponding queued work. A caller that only joined an existing shared load cancels its own wait without canceling that shared load.

Once a callback starts, it retains its slot until it returns. Context cancellation cannot force an uncooperative callback to stop, so business loaders and mutators must still honor context cancellation.

Mutation uses the caller's context deadline and does not inherit Client query timeouts. Batch loading keeps its cancellation rules: closing the Batcher or canceling all waiters for a batch cancels that batch's queue wait. Canceling one waiter preserves other waiters.

Queued queries still report `Fetching`; Mutations report `Pending`. Snapshots do not expose a separate queued status.

## `Stats`: active and queued work

`budget.Stats()` reports the entire shared budget rather than one Client's usage.

| Field | Meaning |
| --- | --- |
| `Active` | Allocated slots, including callbacks that have not returned after cancellation |
| `Queued` | Current waiters; briefly includes canceled work until cleanup finishes |
| `Rejected` | Cumulative arrivals rejected with `ErrQueueFull` because capacity was full |

A slot handed to a waiter already counts as `Active`. In the quick start, `Active` is 1 while the first load is running and 0 after it finishes.

Client `Stats().Loads` counts logical per-key loads, including waiting or rejected loads. `Retries` and diagnostic `Attempts` count Client loader callbacks actually entered; rejection before the first invocation has zero attempts. Per-key Fetcher calls do not equal bulk backend calls. Mutations are not included in Client query statistics.

## Errors and limits

| Condition | Error |
| --- | --- |
| Non-positive `MaxConcurrent` or negative `MaxQueued` | `ErrInvalidExecutionOptions` |
| Running and queue capacity both full | `ErrQueueFull` |
| Callback context already holds the requested budget | `ErrExecutionReentry` |
| Context canceled or expired while queued | `context.Canceled` or `context.DeadlineExceeded` |

Executor limits simultaneous callbacks and waiting work, not requests per second. It provides no persistent job queue and does not bound cached bytes, subscriptions, keys in batch collection windows, or transient goroutines. Client settings still control cache capacity and retention.

## Related guides

- [Cache and lifecycle](cache.en-US.md): load timeouts, retries, cache capacity, and cancellation.
- [Automatic batch loading](batching.en-US.md): collect different keys into bulk backend calls.
- [Mutations](mutations.en-US.md): apply write budgets and synchronize associated queries after success.
