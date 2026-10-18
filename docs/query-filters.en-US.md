# Unified query selection

[简体中文](query-filters.zh-CN.md) · [Invalidation and refresh](invalidation.en-US.md) · [Cache and lifecycle](cache.en-US.md)

`QueryFilter` selects a group of queries within one Client. `Invalidate`, `InvalidateAndWait`, `Cancel`, and `Remove` accept the same selection to invalidate data, await refreshes, cancel loads, or remove cached state.

Typical uses include invalidating detail and list queries after a resource changes, canceling a tenant's loads when switching tenants, and removing associated cached data on sign-out. A filter describes targets; the called method determines loading, cancellation, and removal behavior.

## Quick start

This complete program selects tenant 42's detail and list queries, marks them stale, and removes their cached data. Tenant 99's data remains available. `RefetchNone` prevents this invalidation from starting refreshes.

Save the program as `main.go` in a Go module with the soroi dependency and run `go run .`. No HTTP service or database is required.

```go
package main

import (
	"fmt"
	"time"

	"github.com/Waterkyuu/soroi"
)

// ResourceKey identifies a resource within a tenant.
type ResourceKey struct {
	// TenantID separates data belonging to different tenants.
	TenantID int
	// Resource identifies the kind of query result.
	Resource string
}

// main invalidates and removes tenant 42's cached results without affecting tenant 99.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	defer client.Close()

	detailKey := ResourceKey{TenantID: 42, Resource: "profile"}
	listKey := ResourceKey{TenantID: 42, Resource: "users"}
	otherKey := ResourceKey{TenantID: 99, Resource: "profile"}
	if err := soroi.Set(client, detailKey, "Alice"); err != nil {
		panic(err)
	}
	if err := soroi.Set(client, listKey, []string{"Alice"}); err != nil {
		panic(err)
	}
	if err := soroi.Set(client, otherKey, "Carol"); err != nil {
		panic(err)
	}

	filter := soroi.QueryFilter{
		Predicate: func(key any) bool {
			resource, ok := key.(ResourceKey)
			return ok && resource.TenantID == 42
		},
	}
	if err := client.Invalidate(filter, soroi.InvalidateOptions{
		Refetch: soroi.RefetchNone,
	}); err != nil {
		panic(err)
	}
	fmt.Println("detail stale:", soroi.Get[string](client, detailKey).Stale)
	fmt.Println("list stale:", soroi.Get[[]string](client, listKey).Stale)
	fmt.Println("other tenant stale:", soroi.Get[string](client, otherKey).Stale)

	if err := client.Remove(filter); err != nil {
		panic(err)
	}
	fmt.Println("detail retained:", soroi.Get[string](client, detailKey).HasData)
	fmt.Println("list retained:", soroi.Get[[]string](client, listKey).HasData)
	fmt.Println("other tenant data:", soroi.Get[string](client, otherKey).Data)
}
```

Expected output:

```text
detail stale: true
list stale: true
other tenant stale: false
detail retained: false
list retained: false
other tenant data: Carol
```

`Predicate` receives a query key and returns true to select it. This predicate first checks the application's `ResourceKey` type, then matches `TenantID`; keys of other types are excluded. Tenant data is separated by `TenantID` in the query key. The filter does not replace identity scope in keys.

## `target`: specify operation targets

The four methods accept these forms through their `target any` parameter:

| Argument form | Example | Selection rule |
| --- | --- | --- |
| One query key | `"users"` | Address that exact key |
| `[]any` | `[]any{"users", "user:42"}` | Address multiple keys in first-occurrence order, without duplicates |
| `func(any) bool` | `func(key any) bool { return key == "users" }` | Select existing Client queries for which the predicate returns true |
| A `QueryFilter` value | `soroi.QueryFilter{Keys: []any{"users"}}` | Combine keys and a predicate for reuse across operations |

Query keys must satisfy soroi's comparable-key requirements. Matching uses exact Go equality, including dynamic types; strings and arrays are not matched by prefix. Only `[]any` denotes a batch argument. Other slice types such as `[]string` cannot be passed directly as batches.

An existing query is a key for which the Client still retains cached state, an active load, or a subscription. This includes subscribed queries without usable data. Calling `QueryDefinition` alone does not register a query with a Client.

Filters and predicates select existing queries only and skip missing keys. Passing an exact key or `[]any` directly addresses the specified keys; `Invalidate` can establish stale state for missing keys in these forms. Empty and nil `[]any` batches both perform no operations.

## `QueryFilter`: combine selection criteria

| Field | Type | Default | Contract |
| --- | --- | --- | --- |
| `Keys` | `[]any` | nil | Restrict candidate keys; nil is unrestricted, while a non-nil empty slice selects nothing |
| `Tags` | `[]string` | nil | Require any matching registered label; nil is unrestricted, while a non-nil empty slice selects nothing |
| `Predicate` | `func(key any) bool` | nil | Further restrict candidates; nil accepts all candidates |

Configured fields are combined with AND: a key must satisfy `Keys`, match any requested `Tags`, and satisfy `Predicate`. Duplicate keys are processed once. After the complete key list has been validated, candidates satisfying key and tag restrictions are passed to the predicate at most once. Declare labels through `Definition.Tags`; see [query tags](query-tags.en-US.md) for registration and lifetime rules.

This local fragment uses the `ResourceKey` type from the quick start and can be placed inside an application function:

```go
filter := soroi.QueryFilter{
	Keys: []any{
		ResourceKey{TenantID: 42, Resource: "profile"},
		ResourceKey{TenantID: 99, Resource: "profile"},
	},
	Predicate: func(key any) bool {
		resource, ok := key.(ResourceKey)
		return ok && resource.TenantID == 42
	},
}
```

The filter selects only tenant 42's `profile` query, provided the Client already retains that query.

| Filter value | Selected queries |
| --- | --- |
| `soroi.QueryFilter{}` | All existing queries |
| `soroi.QueryFilter{Keys: nil}` | All existing queries |
| `soroi.QueryFilter{Keys: []any{}}` | No queries |
| `soroi.QueryFilter{Keys: []any{"users"}}` | The existing `"users"` query |
| `soroi.QueryFilter{Predicate: func(key any) bool { return key == "users" }}` | The existing `"users"` query |

Order is unspecified without an explicit key list. A filter retains criteria, not previously matched results; every operation selects queries again.

## Operation behavior and return values

| Method | Return value | Effect on selected queries |
| --- | --- | --- |
| `client.Invalidate(target, options...)` | `error` | Retain data, mark it stale, and schedule refreshes according to options; return without awaiting refresh completion |
| `client.InvalidateAndWait(ctx, target, options...)` | `([]RefreshResult, error)` | Mark data stale and await the refresh outcomes associated with this invalidation |
| `client.Cancel(target)` | `error` | Cancel active shared loads while retaining completed data and subscriptions |
| `client.Remove(target)` | `error` | Cancel active shared loads, clear cached data, errors, and associated cleanup timers, and retain subscriptions |

`Invalidate` and `InvalidateAndWait` use the same refresh conditions as exact-key calls: automatic refresh requires an enabled subscription. By default, `Invalidate` does not append a refresh to an active load; `RefetchAfterLoad` enables a bounded follow-up. `InvalidateAndWait` enables it automatically. `RefetchNone` prevents starting refreshes without canceling existing requests. See [invalidation and refresh](invalidation.en-US.md) for the complete policy.

`Cancel` and `Remove` affect every waiter of a selected shared load, not only the application function invoking them. Both signal cancellation and update query state without waiting for loaders to exit; loaders must honor context.

Neither operation automatically reloads data. Subsequent `Fetch`, `Refetch`, or a transition from disabled to enabled can load data again. Close a subscription with its handle's `Close()` method. If subscriptions remain after `Remove`, the key's result-type binding remains in effect; see [cache and lifecycle](cache.en-US.md).

## Await refresh completion

This complete program prefetches data and creates an enabled subscription. After changing the simulated backend value, `InvalidateAndWait` waits for the query to refresh before the program reads Bob.

Save the program as `main.go` and run `go run .`. The loader can run in the background, so `atomic.Value` synchronizes access to the backend value.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main waits for an enabled query to reload a changed backend value.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	defer client.Close()
	ctx := context.Background()

	var backend atomic.Value
	backend.Store("Alice")
	load := func(context.Context) (string, error) {
		return backend.Load().(string), nil
	}
	if err := soroi.Prefetch(ctx, client, "profile", load); err != nil {
		panic(err)
	}
	query := soroi.Query(client, "profile", load)
	defer query.Close()

	backend.Store("Bob")
	filter := soroi.QueryFilter{Keys: []any{"profile"}}
	results, err := client.InvalidateAndWait(ctx, filter)
	if err != nil {
		panic(err)
	}
	fmt.Println("refreshed:", results[0].Refreshed)
	fmt.Println("data:", query.Snapshot().Data)
}
```

Expected output:

```text
refreshed: true
data: Bob
```

Each selected key has one `RefreshResult`:

| Field | Meaning |
| --- | --- |
| `Key` | The selected query key, retaining its dynamic type |
| `Refreshed` | A post-invalidation load completed successfully without its result being invalidated again or replaced by a local write |
| `Err` | The key's refresh failure, supersession error, or wait cancellation error |

Queries without an enabled subscription, or calls using `RefetchNone`, return `Refreshed == false` and `Err == nil` to indicate a skipped refresh. Missing filter keys do not appear in the results. An empty selection returns a zero-length results slice and a nil error.

Selected queries can refresh concurrently. Results follow the first-occurrence order of an explicit key list. Per-key errors are wrapped with key context and joined in the returned `error`; use `errors.Is` and `errors.As` to inspect them. One failed refresh does not roll back other completed refreshes. Successful completion does not establish a simultaneous, consistent snapshot across multiple keys.

## Errors and context

`InvalidateAndWait` requires a non-nil `context.Context` as `ctx`.

| Condition | Returned behavior |
| --- | --- |
| Nil target, non-comparable key, or an invalid key within a key list | `ErrInvalidKey`; no partial operation-owned changes or predicate calls |
| A nil `func(any) bool` passed directly | `ErrInvalidKey` |
| A nil `QueryFilter.Predicate` field | Valid; no predicate restriction |
| Client closed before invocation | `ErrClosed`; the predicate is not evaluated |
| Client closed during predicate evaluation | `ErrClosed` before applying the operation |
| `InvalidateAndWait` context canceled before invocation | Context error; no selection or invalidation |
| Context canceled while awaiting refreshes | End this call's unfinished waits; Client-owned refreshes continue |

Call `Cancel` or `Remove` explicitly to stop the actual refreshes. See [invalidation and refresh](invalidation.en-US.md) for per-key refresh errors and execution-budget errors from `InvalidateAndWait`.

## Concurrency and lifecycle

Predicates run outside the Client lock and may call Client APIs. They evaluate a candidate-key snapshot captured at the start of the operation. Newly added keys are excluded, and keys no longer retained by the Client before application are skipped.

Selection and application are not a backend transaction and do not form a transaction with other business writes. Predicate side effects are not rolled back. Prefer predicates that only inspect keys.

The caller owns filter slices, mutable data captured by predicates, and the business scope represented in query keys. Applications must synchronize concurrent mutation of these values. Selection does not change `QueryOptions.Disable`, introduce polling, or load queries without subscriptions.

## Related guides

- [Shared options and query definitions](query-options.en-US.md): reuse keys, loaders, and configuration.
- [Invalidation and refresh](invalidation.en-US.md): refresh policy, per-key results, and bounded follow-up loads.
- [Cache and lifecycle](cache.en-US.md): cache reads and writes, cancellation, removal, and resource release.

The selection contract is covered by [unit tests](../query_filter_test.go) and [HTTP end-to-end tests](../e2e/query_filter_test.go).
