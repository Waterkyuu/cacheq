# Cache reads, writes, expiration, and lifecycle

[简体中文](cache.zh-CN.md) · [Queries and conditions](queries.en-US.md) · [Invalidation](invalidation.en-US.md)

All operations share one `*cacheq.Client`. Snippets belong inside business functions: `ctx` is the caller's context and `client` is created at the application boundary and injected. Import `cacheq "github.com/Waterkyuu/cacheq"`, plus the standard-library packages used by each snippet.

## `NewClient` and `Options`

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
	Retry:     2,
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
| `Retry` | Additional attempts after initial failure | 0 |
| `RetryDelay` | Delay before additional attempt, numbered from 1 | Exponential from 1 second, capped at 30 seconds |
| `Timeout` | Bounds the entire load, including retries and backoff | 0: no additional timeout; caller cancellation still applies |
| `Clock` | Freshness and retention comparison clock | `time.Now` |

Policies apply to the client. Cancellation and deadline errors are not retried. Timeout cancels the context; it cannot forcibly terminate a loader that ignores context.

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

The value supplies the static type. Data becomes fresh according to `StaleTime` and compatible handles are notified. Older requests are canceled and detached so late results cannot overwrite the write. Conflicting types are rejected before cancellation, leaving valid work intact.

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

`Loader[V]` is `func(context.Context) (V, time.Time, error)`. Its deadline preserves the age of restored data instead of restarting its freshness window. Zero or elapsed deadlines make data immediately stale.

Ordinary failures do not install new data. Returning both an error and a future deadline explicitly caches the fallback value and that error until the deadline, with `HasData: true` even for a zero value. Refresh errors retain earlier successful data.

## `GCTime`: remove inactive data automatically

```go
client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
})
```

Freshness and retention are separate: data becomes stale after a minute but can still be displayed during refresh. Deletion requires no handles, no active load, and five minutes without use.

Reads and writes restart retention. Any handle, including a disabled handle, suspends cleanup; loads also suspend it. Cleanup resumes after the last handle closes or a load finishes. `Remove`, `Clear`, and `Close` stop corresponding timers. There is no LRU eviction or capacity limit.

`Clock` replaces comparison time, not real timer scheduling. Internal GC tests inject both comparison time and scheduling to test boundaries deterministically.

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

## Handle and client `Close`

```go
users.Close()  // Release one consumer without canceling shared work.
client.Close() // Cancel all work and close all query channels permanently.
```

Closing a handle does not delete data or cancel a shared load. Closing the client cancels requests, closes update channels, removes data and timers, and rejects further operations. Both are idempotent.

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
go test -race ./e2e -run 'Test(HTTPCancellation|HTTPRetriesAndRetainedData|PublicCacheOperations)' -count=1
go test -race . -run 'TestGC' -count=1
```

[External e2e tests](../e2e/query_lifecycle_test.go) cover actual HTTP cancellation, retries, and public cache lifecycle. [Cache tests](../cache_test.go) verify automatic deletion and exact time boundaries.
