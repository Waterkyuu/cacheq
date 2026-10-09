# Cache reads, writes, expiration, and lifecycle

[简体中文](cache.zh-CN.md) · [Queries and conditions](queries.en-US.md) · [Invalidation](invalidation.en-US.md)

A `Client` stores typed results, controls their freshness and retention, and owns shared loads. This guide covers local reads and writes, preloading, absolute expiry, eviction, cancellation, and resource release.

## API snippet prerequisites

Unless labeled as a complete program, Go blocks are partial snippets inside business functions. Snippets containing `return err` require a function returning `error`. `ctx` is a non-nil caller context; `client` is created at the application boundary and injected; `users` is a query handle from the [query guide](queries.en-US.md). Import `cacheq "github.com/Waterkyuu/cacheq"` and the standard-library packages used by each snippet.

For cache hits, shared requests, and cleanup counts, see [observability](observability.en-US.md).

## `NewClient` and `Options`

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime:  time.Minute,
	GCTime:     5 * time.Minute,
	MaxEntries: 1000,
	MaxAge:     10 * time.Minute,
	Retry:      2,
	RetryDelay: func(attempt int) time.Duration {
		return time.Duration(attempt) * 100 * time.Millisecond
	},
	Timeout: 10 * time.Second,
})
defer client.Close()
```

One application-owned client can store strings, details, lists, and configuration simultaneously. Each key keeps its own static result type.

| Option | Responsibility | Default |
| --- | --- | --- |
| `StaleTime` | Ordinary result freshness | 0: immediately stale |
| `GCTime` | Retention of unused state | 0: disabled; negative also disables |
| `MaxEntries` | Maximum retained results and errors, using LRU eviction | 0: unlimited; negative also disables |
| `MaxAge` | Maximum data availability since its last installation | 0: unlimited; negative also disables |
| `Retry` | Additional attempts after initial failure | 0 |
| `RetryIf` | Selects errors eligible for another attempt | nil: retry eligible errors while attempts remain |
| `RetryDelay` | Delay before additional attempt, numbered from 1 | Exponential from 1 second, capped at 30 seconds |
| `Timeout` | Bounds the entire load, including retries and backoff | 0: no additional timeout; caller cancellation still applies |
| `Clock` | Freshness and retention comparison clock | `time.Now` |

All duration fields use `time.Duration`. Zero or negative `StaleTime` makes ordinary data immediately stale; non-positive `Retry` disables additional attempts, and non-positive `Timeout` adds no timeout.

These policies provide client defaults. [Per-consumer options](query-options.en-US.md) can override freshness, retries, and timeout; capacity, GC, maximum age, and the clock remain client policies. Cancellation, deadline errors, and `ErrBatcherClosed` are not retried, even when `RetryIf` returns true. Timeout cancels the context; it cannot forcibly terminate a loader that ignores context.

## `Get[V]`: inspect without requesting

```go
state := cacheq.Get[string](client, "greeting")
if state.HasData {
	fmt.Println(state.Data)
}
```

No request or subscription is created. The explicit type is necessary because there is no fetcher to infer it from. An unknown key is `Idle`; reading it does not bind its type. Reads of cached state restart inactive retention. Use handle `Updates()` for ongoing notifications.

## `Set`: install local data

```go
if err := cacheq.Set(client, "greeting", "hello"); err != nil {
	return err
}
if err := cacheq.Set(client, "feature-flags", map[string]bool{"search": true}); err != nil {
	return err
}
```

The value supplies the static type. Data becomes fresh according to `StaleTime`, capped by `MaxAge` when enabled, and compatible handles are notified. Older requests are canceled and detached so late results cannot overwrite the write. Conflicting types are rejected before cancellation, leaving valid work intact.

Cached values are shared and immutable to readers. Copy slices, maps, and pointed-to values before modifying and installing them. Direct mutation neither publishes updates nor prevents data races.

## `Prefetch`: warm data for later use

```go
getGreeting := func(context.Context) (string, error) {
	return "hello", nil
}
if err := cacheq.Prefetch(
	ctx,
	client,
	"greeting",
	getGreeting,
); err != nil {
	return err
}
```

This waits for preparation and returns only an error. Subsequent `Fetch` or `Query` uses the same cache. Fresh data avoids a load; active same-key requests are shared. Replace the fixed fetcher with an HTTP loader in production.

## `FetchWithExpiry` and `Loader`: preserve an absolute deadline

```go
// In production, obtain both fields from the same persisted record.
savedValue := "restored"
savedExpiresAt := time.Now().Add(time.Minute)
load := func(context.Context) (string, time.Time, error) {
	return savedValue, savedExpiresAt, nil
}
value, err := cacheq.FetchWithExpiry(
	ctx,
	client,
	"restored-value",
	load,
)
if err != nil {
	return err
}
_ = value
```

`Loader[V]` is `func(context.Context) (V, time.Time, error)`. Its deadline preserves the original freshness deadline of restored data instead of restarting its freshness window. `MaxAge` still starts at installation into this client. Zero or elapsed deadlines make data immediately stale.

Ordinary failures do not install new data. Returning both an error and a future deadline explicitly caches the fallback value and that error until the deadline, with `HasData: true` even for a zero value. When `MaxAge` is enabled, it can shorten this deadline. Refresh errors retain earlier successful data only while it remains within `MaxAge`.

## `GCTime`: remove inactive data automatically

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
})
defer client.Close()
```

Freshness and retention are separate: data becomes stale after a minute but can still be displayed during refresh. Deletion requires no handles, no active load, and five minutes without use.

Reads and writes restart retention. Any handle, including a disabled handle, suspends cleanup; loads also suspend it. Cleanup resumes after the last handle closes or a load finishes. `Remove`, `Clear`, and `Close` stop corresponding timers. Capacity eviction and maximum data age are separate policies below.

`Clock` replaces comparison time, not real timer scheduling. Changing an injected clock alone does not trigger a GC timer.

## `MaxEntries`: limit retained results with LRU eviction

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime:  time.Minute,
	MaxEntries: 1000,
})
defer client.Close()
```

At most 1000 completed results or cached errors are retained. A new result beyond that limit evicts the least recently used one. Reads, writes, query construction, and invalidation update recency; closing a handle does not. Zero or negative limits preserve unlimited capacity.

Eviction clears data and errors, stops the key's GC timer, and notifies subscribed handles with an empty snapshot. It does not cancel active loads or automatically request data again. Handles can still `Refetch`; their static result type remains protected while handles or loads own the key. Without either owner, eviction also releases the key's type binding. Empty type metadata is released after its last owner leaves, even when timed GC is disabled.

The limit counts retained results, not bytes, subscriptions, active requests, or their type metadata. A single large value can still consume substantial memory.

## `MaxAge`: stop serving over-age data

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
	MaxAge:    10 * time.Minute,
})
defer client.Close()
```

Age starts when a successful load or `Set` installs data. In this example it is fresh for one minute, then may remain available while stale for another nine minutes. At ten minutes it is no longer returned: `HasData` becomes false and `Data` is the result type's zero value, including during refresh or for disabled handles. The last error and active type bindings remain; age expiration does not cancel an active load. Inactive metadata remains subject to GC and capacity cleanup.

Reads, invalidation, and ordinary failed refreshes do not renew this deadline. Installing a new successful result, a local value, or an explicit fallback from `FetchWithExpiry` starts a new age window. Loader deadlines earlier than the age limit are preserved; later freshness deadlines are shortened to it.

Age is checked on cache operations and state publication. Merely passing time does not send an update or start a request. A read that discovers expired data notifies existing handles; enabled `Query` construction or `Fetch` can load a replacement. Previously returned snapshots are ordinary values and do not change retroactively. Zero or negative `MaxAge` disables this policy.

## Complete example: capacity and maximum age

Copy this program into an application importing cacheq. The injected clock demonstrates the deadline without sleeping or starting requests.

```go
package main

import (
	"fmt"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// main demonstrates LRU eviction and maximum data age with a deterministic clock.
func main() {
	now := time.Unix(0, 0)
	client := cacheq.NewClient(cacheq.Options{
		StaleTime:  time.Minute,
		MaxEntries: 2,
		MaxAge:     10 * time.Minute,
		Clock:      func() time.Time { return now },
	})
	defer client.Close()
	if err := cacheq.Set(client, "a", "first"); err != nil {
		panic(err)
	}
	if err := cacheq.Set(client, "b", "second"); err != nil {
		panic(err)
	}
	_ = cacheq.Get[string](client, "a")
	if err := cacheq.Set(client, "c", "third"); err != nil {
		panic(err)
	}
	fmt.Println(
		"retained:",
		cacheq.Get[string](client, "a").HasData,
		cacheq.Get[string](client, "b").HasData,
		cacheq.Get[string](client, "c").HasData,
	)
	now = now.Add(10 * time.Minute)
	fmt.Println("over age:", cacheq.Get[string](client, "a").HasData,
		cacheq.Get[string](client, "c").HasData)
}
```

Expected output:

```text
retained: true false true
over age: false false
```

## `Cancel`: stop a key's active load

```go
if err := client.Cancel("greeting"); err != nil {
	return err
}
```

Previously completed data remains available. Consumers are notified that fetching stopped. Loaders must honor context. Canceling an idle key is safe and does not automatically restart work.

## `Remove`: discard one key

```go
if err := client.Remove("greeting"); err != nil {
	return err
}
```

This cancels work, discards data and timers, and notifies live handles of missing data. It does not automatically reload. A remaining handle can `Refetch`, or a new query can load again.

Type binding survives while old handles are alive. To use a different result type for a key, close all old handles, remove the key, then create the new query.

## `Clear`: discard data while keeping the client usable

```go
client.Clear()
```

Results, requests, and cleanup timers are cleared. Live handles and their type contracts remain, receive empty state, and can manually refresh. Clearing does not itself initiate requests.

## Closing handles and the client

```go
users.Close()  // Release one consumer without canceling shared work.
client.Close() // Cancel all work and close all query channels permanently.
```

Closing a handle does not delete data or cancel a shared load. Closing the client cancels requests, closes query updates and [event subscriptions](events.en-US.md), removes data and timers, and rejects further operations. Both are idempotent.

Released handle `Snapshot()` can still inspect a compatible shared cache; `SetEnabled` and `Refetch` return `ErrQueryClosed`. Client closure makes typed operations return `ErrClosed`.

## Error handling

```go
state := cacheq.Get[int](client, "greeting")
if errors.Is(state.Err, cacheq.ErrTypeMismatch) {
	// Use this key's declared result type or select a different key.
}
```

| Error | Trigger |
| --- | --- |
| `ErrClosed` | Operation on a closed client |
| `ErrQueryClosed` | Enablement or refresh through a released handle |
| `ErrTypeMismatch` | A key used with another static result type |
| `ErrInvalidKey` | Nil or non-comparable key |
| `ErrNoFetcher` | Query construction without a fetcher, or a required load without a loader |

`Query` always requires a non-nil fetcher. `Fetch[V]` and `FetchWithExpiry[V]` may accept nil when fresh data already exists; stale or missing data returns `ErrNoFetcher`. Prefer `Get[V]` for inspection.

Static and dynamic types differ: a `Query[any]` key is bound to `any`. Use `Set[any]` for that key; an inferred concrete string in `Set` binds as `string` and conflicts. Ordinary business queries should use concrete types.

## Verification

```sh
go test -race ./e2e -run 'Test(HTTPCapacityEviction|HTTPMaxAge|HTTPCancellation|HTTPRetriesAndRetainedData|PublicCacheOperations)' -count=1
go test -race . -run 'Test(Capacity|MaxAge|GC)' -count=1
```

[External e2e tests](../e2e/query_lifecycle_test.go) cover actual HTTP cancellation, retries, and public cache lifecycle. [Capacity e2e](../e2e/cache_capacity_test.go) verifies LRU eviction and replacement requests; [age e2e](../e2e/cache_age_test.go) verifies failed refreshes, subscriptions, and shared HTTP work at the deadline. [Cache tests](../cache_test.go) verify automatic deletion and exact time boundaries.
