# Shared query options and reusable definitions

[简体中文](query-options.zh-CN.md)

`QueryOptions` is the shared configuration accepted by `Query`, `Fetch`, and `Prefetch`. It overrides Client defaults for freshness, retries, timeouts, and execution budgets. `Disable` controls only Query's automatic loading; explicit `Fetch`, `Prefetch`, and handle `Refetch` still execute.

`QueryDefinition[V]` groups a key, typed loader, and `QueryOptions` for reuse across these entry points. Constructing a definition performs no loading and creates no cache or subscription.

## Quick start: reuse one configuration

This complete example gives all three entry points the same one-hour freshness policy. The disabled subscription receives data loaded explicitly by `Prefetch`, and `Fetch` reuses that fresh data. Client defaults remain unchanged. Save the program as `main.go` in a module importing cacheq and run `go run .`.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main reuses one loading policy across subscription, prefetch, and explicit reads.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	ctx := context.Background()
	load := func(context.Context) (string, error) { return "hello", nil }
	hour := time.Hour
	options := cacheq.QueryOptions{Disable: true, StaleTime: &hour}
	query := cacheq.Query(client, "greeting", load, options)
	defer query.Close()
	if err := cacheq.Prefetch(ctx, client, "greeting", load, options); err != nil {
		panic(err)
	}
	value, err := cacheq.Fetch(ctx, client, "greeting", load, options)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	fmt.Println("observed:", query.Snapshot().Data)
}
```

Expected output:

```text
hello
observed: hello
```

## Automatic loading and explicit reads

`Query(client, key, load, options)` copies its policy at construction. Automatic loads, `SetEnabled`, and manual `Refetch` retain that policy.

| Entry point | Effect of `Disable: true` |
| --- | --- |
| `Query` | Subscribe to shared updates without starting automatic loads |
| `Fetch` | Perform an explicit read; fresh cache data can satisfy it |
| `Prefetch` | Warm the cache explicitly using the same loading policy |
| Handle `Refetch` | Permit explicit refreshing even while disabled |

Omitting options or leaving `Disable` false permits automatic loading. Disabling a handle does not cancel shared work or disable other handles. Later changes to the original options do not change an existing handle; use `SetEnabled(bool)` instead.

`Fetch(ctx, client, key, load, options)` and `Prefetch(ctx, client, key, load, options)` apply configuration only to that call. Both ignore `Disable`. They neither change Client defaults nor register a policy for later calls using the same key. `Prefetch` returns loading errors without returning data or creating an ongoing subscription.

## `QueryDefinition`: define a typed query once

The definition has five fields:

| Field | Responsibility |
| --- | --- |
| `Key` | Identify the query using a non-nil comparable key; include all query parameters and identity scope |
| `Fetcher` | Load `V` using the application's HTTP, database, or other dependencies |
| `Shared` | Optionally reuse serialized results through a [shared result cache](shared-cache.en-US.md); nil keeps caching local |
| `Tags` | Declare labels for grouping related queries; registration and lifetime follow the [query tag contract](query-tags.en-US.md) |
| `Options` | Supply the same `QueryOptions` to reading, subscribing, and prefetching |

Each operation receives a Client explicitly. The Client owns cached data, shared requests, and shutdown. The same definition can be used with multiple Clients, whose caches remain independent. The returned query handle must be closed by its caller.

This complete example demonstrates definition construction, prefetching, cache reuse, and explicit invalidation:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main reuses a typed definition without creating a second cache or automatic loading.
func main() {
	client := cacheq.NewClient(cacheq.Options{})
	defer client.Close()
	ctx := context.Background()
	hour := time.Hour
	calls := 0
	definition := cacheq.QueryDefinition[string]{
		Key: "greeting",
		Fetcher: func(context.Context) (string, error) {
			calls++
			return "hello", nil
		},
		Options: cacheq.QueryOptions{Disable: true, StaleTime: &hour},
	}
	query := definition.Query(client)
	defer query.Close()
	fmt.Println("initial calls:", calls)
	if err := definition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	value, err := definition.Fetch(ctx, client)
	if err != nil {
		panic(err)
	}
	fmt.Println(value, "calls:", calls)
	if err := client.Invalidate(definition.Key); err != nil {
		panic(err)
	}
	if _, err := definition.Fetch(ctx, client); err != nil {
		panic(err)
	}
	fmt.Println("refreshed calls:", calls)
}
```

Expected output:

```text
initial calls: 0
hello calls: 1
refreshed calls: 2
```

| Method | Result |
| --- | --- |
| `definition.Fetch(ctx, client)` | `(V, error)` with the same contract as `Fetch` |
| `definition.Query(client)` | `*QueryHandle[V]` with the same contract as `Query` |
| `definition.Prefetch(ctx, client)` | Loading error with the same contract as `Prefetch` |

The methods use the definition's key, fetcher, and options directly. To customize one consumer, copy the definition and change the copy's options before calling a method. Changes to Key, Fetcher, or Options apply to subsequent operations without reconfiguring existing handles. Tags declared by subsequent operations are merged into the shared query state. Each method reads option pointer values when its operation starts. Applications must synchronize shared definitions, pointer values, and mutable state captured by loaders.

Definitions do not infer keys from a loader, add authorization checks, or serialize business operations. Build parameterized definitions in ordinary business functions so the key and loader capture matching parameters. Result-type binding, cancellation, errors, and in-flight request sharing follow the existing [query contracts](queries.en-US.md). Use `client.Invalidate(definition.Key)` or `client.InvalidateAndWait(ctx, definition.Key)` for explicit invalidation.

## Shared fields

These loading-policy fields apply to `Query`, `Fetch`, and `Prefetch`.

| Field | Type | Default | When set |
| --- | --- | --- | --- |
| `StaleTime` | `*time.Duration` | Inherit Client freshness | Calculate freshness from the data's `UpdatedAt`; zero means immediately stale |
| `Retry` | `*int` | Inherit Client retry count | Additional attempts after the first failure; zero or negative disables retries |
| `RetryIf` | `func(error) bool` | Inherit Client error filter | True permits an additional attempt, subject to the retry count and terminal errors |
| `RetryDelay` | `func(int) time.Duration` | Inherit Client retry delay | Return the delay before an additional attempt; numbering starts at 1 |
| `Timeout` | `*time.Duration` | Inherit Client load timeout | Bound the entire load, including queuing, execution, and retry delays; zero or negative adds no timeout |
| `Executor` | `*Executor` | nil, inherit Client budget | Use this shared budget for new loader attempts |
| `DisableExecutor` | `bool` | false | Skip the outer Client budget; takes precedence over `Executor` |

Pointer fields distinguish inheritance from explicit zero values. For example, `Retry: nil` inherits the default retry count; a pointer to 0 disables retries. Caller context deadlines and cancellation remain effective even with a zero `Timeout`.

Cancellation, deadline errors, `ErrQueueFull`, `ErrBatcherClosed`, and `ErrExecutionReentry` never retry, including wrapped errors, and bypass `RetryIf`. The predicate runs only when attempts remain and the error is retryable. A predicate always returning true can override the Client filter but cannot bypass terminal errors.

`RetryIf` and `RetryDelay` run outside the Client lock and may call Client APIs. Applications must synchronize shared mutable state captured by callbacks.

## Execution budget overrides

Use `Executor` to select a different backend budget for one query, or `DisableExecutor: true` when its Batcher already budgets the actual bulk callback. A nil Executor inherits the Client budget unless disabling is explicit. Disabling outer acquisition does not disable the Batcher's own budget.

```go
options := cacheq.QueryOptions{
	DisableExecutor: true,
}
```

Pass this same configuration to `Query`, `Fetch`, or `Prefetch` when using `Batcher.Fetcher`. Applying the same budget at both layers returns `ErrExecutionReentry` when the callback context is propagated. See [execution budgets](execution.en-US.md) for a complete configuration example and detection limitations.

## Defaults and overrides

- Omitting options or passing an empty value uses Client defaults. `QueryOptions.Disable` defaults to false.
- If several options values are supplied, only the last applies; their fields are not merged. Nil fields in the last value inherit Client defaults.
- Duration and retry pointer values are copied when creating `Query` or calling `Fetch` or `Prefetch`. A definition retains option pointers until one of these operations applies them. Later changes to the original variables do not alter existing configuration. Executor references are shared; capacity is not copied.
- The same key still shares data and requests. Different options do not create separate caches or replace query parameters and identity scope in the key.

## Configuration for shared requests

A new load uses its initiator's loader, retry settings, timeout, and execution budget. Later same-key calls do not replace them. A joiner's context can end its own wait, but its `Timeout` setting applies only to new loads it initiates.

Automatic refresh after invalidation uses the oldest still-enabled handle's configuration. After that handle closes or is disabled, the next enabled handle supplies future automatic refreshes. Manual `Refetch` uses the current handle's configuration when starting a new load.

## Cache policy limits

`StaleTime` changes ordinary cache freshness decisions without enabling polling. Passing time does not automatically publish notifications or start requests; `Snapshot()` reevaluates state when read. `Get` and calls to `Fetch` or `Prefetch` without options use Client freshness defaults.

Explicit invalidation and failed refreshes that install no new data make ordinary cached data stale for all queries. Absolute deadlines returned through `FetchWithExpiry` are unaffected by `StaleTime` overrides, including explicitly cached fallback data and errors.

The Client owns cache capacity, `MaxAge`, GC, and the clock. A longer `StaleTime` cannot extend data availability beyond `MaxAge`.

## Related guides

- [Queries and conditions](queries.en-US.md): `Query`, `Fetch`, state subscriptions, and enablement.
- [Cache and lifecycle](cache.en-US.md): Client defaults, expiration, and resource management.
- [Invalidation](invalidation.en-US.md): invalidate and refresh associated queries.
- [Execution budgets](execution.en-US.md): share backend capacity and configure batched query overrides.
