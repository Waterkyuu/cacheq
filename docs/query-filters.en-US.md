# Unified query selection

[简体中文](query-filters.zh-CN.md)

`QueryFilter` defines a reusable selection of queries for `Invalidate`, `InvalidateAndWait`, `Cancel`, and `Remove`. It groups queries by exact keys or a business predicate without creating another cache or owning subscriptions.

## Replace per-key orchestration

When cancellation accepts only an exact key, an application must maintain candidate keys and repeat matching and iteration around each operation. Invalidation already accepts a predicate, so the duplicated work is enumeration and orchestration rather than an unavoidable repetition of the predicate itself:

```go
matches := func(key any) bool {
    resource, ok := key.(ResourceKey)
    return ok && resource.TenantID == tenantID
}
if err := client.Invalidate(matches); err != nil {
    return err
}
for _, key := range knownKeys {
    if matches(key) {
        if err := client.Cancel(key); err != nil {
            return err
        }
    }
}
```

This fragment belongs in a function returning `error`; `ResourceKey` is defined in the complete program below, and `knownKeys` is an application-maintained list. With unified selection, pass the same filter directly to each operation:

```go
filter := cacheq.QueryFilter{Predicate: matches}
if err := client.Cancel(filter); err != nil {
    return err
}
```

The Client enumerates its own query keys; the application no longer maintains `knownKeys` for this operation. A predicate can also be passed directly to any of the four operations. Use `QueryFilter` when selection should be named, reused, or narrowed with both keys and a predicate.

## Complete tenant example

Save this program as `main.go` in a module importing cacheq and run `go run .`. It prefetches typed queries, observes them, changes backend data, and waits for tenant 42's detail and list to refresh. Cancellation retains cached data; removal clears only selected results.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// ResourceKey identifies a result within a tenant.
type ResourceKey struct {
	// TenantID separates otherwise identical resource queries.
	TenantID int
	// Resource identifies the kind of result.
	Resource string
}

// main reuses a tenant filter to refresh, cancel, and remove related queries.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()
	ctx := context.Background()
	var backend atomic.Value
	backend.Store("Alice")
	detailDefinition := cacheq.QueryDefinition[string]{
		Key: ResourceKey{TenantID: 42, Resource: "user"},
		Fetcher: func(context.Context) (string, error) {
			return backend.Load().(string), nil
		},
	}
	listDefinition := cacheq.QueryDefinition[[]string]{
		Key: ResourceKey{TenantID: 42, Resource: "users"},
		Fetcher: func(context.Context) ([]string, error) {
			return []string{backend.Load().(string)}, nil
		},
	}
	if err := detailDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	if err := listDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	detail := detailDefinition.Query(client)
	list := listDefinition.Query(client)
	defer detail.Close()
	defer list.Close()
	otherKey := ResourceKey{TenantID: 99, Resource: "user"}
	if err := cacheq.Set(client, otherKey, "Carol"); err != nil {
		panic(err)
	}
	filter := cacheq.QueryFilter{Predicate: func(key any) bool {
		resource, ok := key.(ResourceKey)
		return ok && resource.TenantID == 42
	}}
	backend.Store("Bob")
	results, err := client.InvalidateAndWait(ctx, filter)
	if err != nil {
		panic(err)
	}
	fmt.Println("refreshed:", len(results))
	fmt.Println("detail:", detail.Snapshot().Data)
	fmt.Println("list:", list.Snapshot().Data)
	if err := client.Cancel(filter); err != nil {
		panic(err)
	}
	fmt.Println("after cancel:", detail.Snapshot().Data)
	if err := client.Remove(filter); err != nil {
		panic(err)
	}
	fmt.Println("removed:", !detail.Snapshot().HasData && !list.Snapshot().HasData)
	fmt.Println("other tenant:", cacheq.Get[string](client, otherKey).Data)
}
```

Expected output:

```text
refreshed: 2
detail: Bob
list: [Bob]
after cancel: Bob
removed: true
other tenant: Carol
```

Tenant isolation comes from `ResourceKey.TenantID`, not the filter. Every query parameter and identity scope affecting a result must remain part of its key.

## Selection contract

| Field | Meaning |
| --- | --- |
| `Keys []any` | Restrict candidates to exact comparable keys; nil imposes no key restriction |
| `Predicate func(any) bool` | Further restrict candidates; nil accepts all candidates |

- `QueryFilter{}` selects all existing cached, loading, or subscribed keys.
- A non-nil empty `Keys` slice selects nothing. Missing keys are skipped rather than created.
- Keys and predicate are combined with AND. Each distinct existing candidate is passed to the predicate once, after validating all supplied keys.
- Explicit key order follows first occurrence. Without `Keys`, order is unspecified.
- Keys use exact Go equality with their dynamic types. There is no implicit string prefix, array prefix, or tenant matching.
- Predicate evaluation runs outside the Client lock and may call Client APIs. It uses an initial snapshot; newly added keys are excluded and keys removed before application are skipped. Selection and application do not form a transaction with concurrent business writes.
- The caller owns the filter's slices and predicate state and must synchronize concurrent mutation. Predicate side effects are caller-owned and are not rolled back.

For a key subset further restricted to a tenant:

```go
filter := cacheq.QueryFilter{
    Keys: []any{detailDefinition.Key, listDefinition.Key},
    Predicate: func(key any) bool {
        resource, ok := key.(ResourceKey)
        return ok && resource.TenantID == 42
    },
}
if err := client.Remove(filter); err != nil {
    return err
}
```

The fragment belongs in a function returning `error` and uses the definitions from the complete example.

## Supported operations

| Operation | Effect on selected queries |
| --- | --- |
| `client.Invalidate(filter, options...)` | Retain data, mark it stale, and schedule enabled observed refreshes according to options |
| `client.InvalidateAndWait(ctx, filter, options...)` | Apply the same selection and wait for post-invalidation refresh outcomes |
| `client.Cancel(filter)` | Cancel active shared loads while retaining completed data and subscriptions |
| `client.Remove(filter)` | Cancel active shared loads and discard completed state; subscriptions remain usable |

All four accept an exact key, `[]any`, `func(any) bool`, or a `QueryFilter` value. Only `[]any` denotes a direct batch; other slice types are invalid. Raw exact-key invalidation may create stale metadata for a missing key; filter and predicate selection operate only on existing keys. A nil direct target or direct predicate is invalid, whereas a nil predicate field in a filter means no restriction.

Invalid keys return `ErrInvalidKey` before any operation-owned changes or predicate calls. Closed clients return `ErrClosed` without evaluating predicates. Closure during predicate evaluation returns `ErrClosed` before applying the selection. `InvalidateAndWait` checks a pre-canceled context before selection; canceling its wait does not cancel Client-owned refreshes. Calling `Cancel(filter)` or `Remove(filter)` explicitly cancels the selected shared loads, affecting all their waiters.

Filters choose targets; they do not change `QueryOptions.Disable`, start unobserved loaders, or add polling. Canceling or removing a query does not close its handles or prevent subsequent explicit reads and refreshes. Tags are not part of this API yet.

## Related guides

- [Query definitions and shared options](query-options.en-US.md): group keys, loaders, and consumer configuration.
- [Invalidation and refresh](invalidation.en-US.md): refresh policy, waiting outcomes, and bounded follow-up loads.
- [Cache and lifecycle](cache.en-US.md): cancellation, removal, and Client ownership.

Public behavior is covered by [unit tests](../query_filter_test.go) and [HTTP end-to-end tests](../e2e/query_filter_test.go).
