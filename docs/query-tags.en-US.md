# Query tags

[简体中文](query-tags.zh-CN.md) · [Query filters](query-filters.en-US.md) · [Query definitions](query-options.en-US.md)

Query tags declare relationships between queries. Detail, list, and aggregate queries can use different keys and result types while sharing a tag, allowing a `QueryFilter` to invalidate them, await refreshes, cancel their loads, or remove cached data as a group.

`Key` identifies a query; `Tags` groups related queries. Sharing a tag does not merge caches for different keys or change result-type constraints or loading policies.

This guide specifies local tag registration and QueryFilter matching. Optional remote registration through `ShareTags` and cross-instance invalidation through `WatchShared` are covered by [shared invalidation and tags](shared-invalidation.en-US.md).

## Quick start

This complete program assigns the same tag to tenant 42's user detail and list queries. It prefetches data, creates enabled subscriptions, changes the simulated backend value, and awaits both refreshes by tag. Tenant 99's data remains unchanged.

Save the program as `main.go` in a Go module with the cacheq dependency and run `go run .`. The example uses `atomic.Value` to synchronize data read by background loads and requires no external service.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// User is the user data returned by detail and list queries.
type User struct {
	// Name contains the user's display name.
	Name string
}

// main refreshes tagged detail and list queries without touching another tenant's data.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()
	ctx := context.Background()
	var backend atomic.Value
	backend.Store(User{Name: "Alice"})
	tags := []string{"tenant:42:users"}

	detailDefinition := cacheq.QueryDefinition[User]{
		Key:  "tenant:42:user:7",
		Tags: tags,
		Fetcher: func(context.Context) (User, error) {
			return backend.Load().(User), nil
		},
	}
	listDefinition := cacheq.QueryDefinition[[]User]{
		Key:  "tenant:42:users",
		Tags: tags,
		Fetcher: func(context.Context) ([]User, error) {
			return []User{backend.Load().(User)}, nil
		},
	}
	otherDefinition := cacheq.QueryDefinition[User]{
		Key:  "tenant:99:user:7",
		Tags: []string{"tenant:99:users"},
		Fetcher: func(context.Context) (User, error) {
			return User{Name: "Carol"}, nil
		},
	}
	if err := detailDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	if err := listDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	if err := otherDefinition.Prefetch(ctx, client); err != nil {
		panic(err)
	}
	detail := detailDefinition.Query(client)
	list := listDefinition.Query(client)
	defer detail.Close()
	defer list.Close()

	backend.Store(User{Name: "Bob"})
	filter := cacheq.QueryFilter{Tags: []string{"tenant:42:users"}}
	results, err := client.InvalidateAndWait(ctx, filter)
	if err != nil {
		panic(err)
	}
	fmt.Println("refreshed:", len(results))
	fmt.Println("detail:", detail.Snapshot().Data.Name)
	fmt.Println("list:", list.Snapshot().Data[0].Name)
	fmt.Println("other tenant:", cacheq.Get[User](client, otherDefinition.Key).Data.Name)

	if err := client.Remove(filter); err != nil {
		panic(err)
	}
	fmt.Println("selected removed:", !detail.Snapshot().HasData && !list.Snapshot().HasData)
}
```

Expected output:

```text
refreshed: 2
detail: Bob
list: Bob
other tenant: Carol
selected removed: true
```

`Prefetch` registers tags and populates the cache; `Query` creates enabled subscriptions. `InvalidateAndWait` refreshes queries with enabled subscriptions only, so this call returns two results. `Remove` clears selected data while leaving the caller responsible for closing subscriptions.

## Declaration and registration

| Field | Type | Default | Responsibility |
| --- | --- | --- | --- |
| `QueryDefinition.Tags` | `[]string` | nil | Declare labels for this query key; nil and empty slices declare no labels |
| `QueryFilter.Tags` | `[]string` | nil | Select existing queries matching at least one requested label; nil is unrestricted, while a non-nil empty slice selects nothing |

Constructing a `QueryDefinition` does not register tags. Its `Fetch`, `Prefetch`, and `Query` methods register tags after validation when reusing cached data, starting or joining a load, or creating a subscription. Fresh cache hits and disabled subscriptions also register tags without requiring a request.

Tags are copied when an operation starts. Later changes to the definition's `Tags` slice do not change registered labels. Using the modified definition adds its new labels to the same key. Registration belongs to the supplied Client; another Client using the same definition registers its own labels independently.

Tags are declared on query definitions and are separate from the loading policy in `QueryOptions`. Direct `Fetch`, `Query`, and `Prefetch` calls do not add tags or clear labels already registered for the key. `Set` and `Update` preserve existing labels.

## Matching rules

- Multiple requested tags use OR: `Tags: []string{"users", "orders"}` selects queries containing `"users"` or `"orders"`.
- `Keys`, `Tags`, and `Predicate` use AND. A key must satisfy every configured restriction.
- Tags use exact, case-sensitive string matching without prefix matching or whitespace normalization. An empty string is an ordinary tag value.
- A query is processed once even if it has several matching labels or the filter repeats a label.
- Explicit `Keys` determine first-occurrence result order; order is unspecified without a key list.

This local fragment can be placed inside an application function. It restricts selection to two keys with either requested tag, then excludes the detail key:

```go
filter := cacheq.QueryFilter{
    Keys: []any{"tenant:42:user:7", "tenant:42:users"},
    Tags: []string{"tenant:42:users", "tenant:42:orders"},
    Predicate: func(key any) bool {
        return key != "tenant:42:user:7"
    },
}
```

Only registered queries still retained by the Client participate. Unused definitions, missing keys, and queries without a requested label are excluded. Predicates receive only candidates already satisfying key and tag restrictions. Other [query selection rules](query-filters.en-US.md) remain unchanged.

## Operations

| Operation | Behavior |
| --- | --- |
| `client.Invalidate(filter, options...)` | Retain data, mark it stale, and schedule refreshes according to options without awaiting completion |
| `client.InvalidateAndWait(ctx, filter, options...)` | Await post-invalidation refresh outcomes for selected queries |
| `client.Cancel(filter)` | Cancel selected shared loads while retaining completed data and subscriptions |
| `client.Remove(filter)` | Cancel selected shared loads and remove cached data while retaining subscriptions |

Tags affect target selection only. They do not load queries without subscriptions or change `QueryOptions.Disable`. Automatic refresh uses the existing subscription-selection rules. One failed load does not roll back other completed results; see [invalidation and refresh](invalidation.en-US.md) for per-key outcomes and error handling.

## Same-key merging and lifetime

When several definitions or consumers use the same key, registered labels are combined rather than replaced by the latest call. Closing a handle does not unregister its labels. Reading the same key through an entry point without tags also preserves them.

| State transition | Tag behavior |
| --- | --- |
| Load completion or failure, invalidation, cancellation, `Set`, or `Update` | Preserve registered labels |
| Data removal or eviction while subscriptions or query state remain | Retain labels with the remaining query state |
| Complete removal of an unsubscribed key by `Remove`, `Clear`, capacity eviction, or GC | Release its labels |
| Client closure | Release all labels owned by that Client |

Reaching `MaxAge` makes data unavailable but does not necessarily remove the entire query state. Label retention follows the existing state-cleanup rules.

Declaring new labels adds relationships without deleting previous labels. To replace a key's label set, close all subscriptions for that key, call `Remove(key)` to release its old state, then use the new definition. See [cache and lifecycle](cache.en-US.md) for retention policies.

## Validation, concurrency, and scope

Invalid keys, result-type conflicts, a nil Fetcher that cannot satisfy the operation, and contexts canceled before invocation do not register tags. Backend failures or wait cancellation after registration do not roll it back.

Selection reads registered labels under the Client lock; application predicates run outside it. Tag matching uses query state at the start of selection. Labels added during predicate evaluation do not expand that operation's selected keys. Selection and backend writes are not a transaction.

Applications must synchronize concurrent changes to definition slices and data captured by predicates. Query keys must still include parameters and identity scope affecting results. When tenants share a Client, use scoped labels such as `"tenant:42:users"` or combine tags with key predicates to restrict targets.

## Related guides

- [Query filters](query-filters.en-US.md): target forms, intersection rules, and operation return values.
- [Query definitions and options](query-options.en-US.md): definition reuse, loading policy, and Client ownership.
- [Invalidation and refresh](invalidation.en-US.md): awaited refreshes, per-key errors, and bounded follow-up loads.

Tag behavior is covered by [unit tests](../query_tags_test.go) and [HTTP end-to-end tests](../e2e/query_tags_test.go).
