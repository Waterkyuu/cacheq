# Invalidation and awaitable refresh

[简体中文](invalidation.zh-CN.md) · [Queries and conditions](queries.en-US.md) · [Cache and lifecycle](cache.en-US.md)

Invalidation retains data but marks it stale. After updating a user, detail and list results can become stale together even though they return `User` and `[]User` respectively.

`Invalidate` accepts an exact key, a `[]any` list, a `func(any) bool` predicate, or a reusable `QueryFilter`. [Query filters](query-filters.en-US.md) combine exact keys and predicates and also apply to waiting for refresh, cancellation, and removal. Configuration is optional and limited to one `InvalidateOptions` value; extra values return `ErrTooManyOptions` before invalidation. Only `[]any` is treated as a batch, so convert other slice types explicitly. Nil targets and invalid keys return `ErrInvalidKey`; empty or nil `[]any` batches do nothing.

## Quick start

Save this complete program as `main.go` in a module importing soroi and run
`go run .`. It changes a local backend value, invalidates the observed key,
and waits for fresh data.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main refreshes an observed value after a successful backend mutation.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Hour})
	defer client.Close()
	var backend atomic.Value
	backend.Store("initial")
	load := func(context.Context) (string, error) {
		return backend.Load().(string), nil
	}
	query := soroi.Query(client, "message", load)
	defer query.Close()
	ctx := context.Background()
	before, err := soroi.Fetch(
		ctx,
		client,
		"message",
		load,
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("before:", before)

	backend.Store("updated")
	if _, err := client.InvalidateAndWait(ctx, "message"); err != nil {
		panic(err)
	}
	after := query.Snapshot().Data
	fmt.Println("after:", after)
}
```

Expected output:

```text
before: initial
after: updated
```

## Targets and snippet prerequisites

| Target | Scope |
| --- | --- |
| One comparable key | Exact key match |
| `[]any` | Explicit keys, validated together and deduplicated |
| `func(any) bool` | Existing keys selected by the predicate |

The following partial snippets use the shared `client`, `getUser`, and `getUsers`
from the [query guide](queries.en-US.md), inside a function returning `error`.
`ctx` is a non-nil caller context; import the packages used by each snippet.

## Single-key invalidation

```go
if err := client.Invalidate("user:42"); err != nil {
	return err
}

```

Data remains available, `Stale` becomes true, and handles are notified. An enabled handle supplies its fetcher for background refresh; keys without enabled handles are only marked stale.

Invalidation does not wait for refresh completion. Observe results through `Updates()`, use handle `Refetch(ctx)` for a single query, or use `InvalidateAndWait` to wait for associated post-invalidation refreshes.

## Batch invalidation

Create queries with different result types:

```go
detail := soroi.Query(client, "user:42", getUser) // QueryHandle[User]
users := soroi.Query(client, "users", getUsers)   // QueryHandle[[]User]
defer detail.Close()
defer users.Close()

```

After the business mutation succeeds:

```go
// Call the application's user-update API successfully before invalidating.
if err := client.Invalidate([]any{"user:42", "users"}); err != nil {
	return err
}

```

One operation spans multiple result types in the shared client. Enabled consumers refresh by default; other keys simply become stale. Duplicate keys are processed once, avoiding duplicate work or invalidating a refresh just started by an earlier occurrence.

`[]any` contains keys, not untyped query data. It permits strings and comparable struct keys in the same batch. Each query's data remains typed.

All keys are validated before mutation. A non-comparable key returns `ErrInvalidKey` without partial invalidation. Empty batches have no effect. Closed clients return `ErrClosed`.

## `InvalidateOptions` and `RefetchMode`

| Mode | Behavior |
| --- | --- |
| `RefetchObserved` | Default and zero value: refresh keys with enabled handles |
| `RefetchNone` | Mark stale without starting new work |

Single-key invalidation accepts an optional configuration:

```go
if err := client.Invalidate("user:42", soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
}); err != nil {
	return err
}
```

Defer a group of related queries:

```go
if err := client.Invalidate(
	[]any{"user:42", "users"},
	soroi.InvalidateOptions{Refetch: soroi.RefetchNone},
); err != nil {
	return err
}
```

Even enabled consumers retain stale data without starting a request. A later `Fetch`, new `Query`, transition from disabled to enabled, or manual `Refetch` may refresh it. Existing enabled handles do not poll simply because time passes.

## Predicate invalidation

Import `strings` to select all user details and the list:

```go
if err := client.Invalidate(func(key any) bool {
	name, ok := key.(string)
	return ok && (name == "users" || strings.HasPrefix(name, "user:"))
}, soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
}); err != nil {
	return err
}
```

Only keys returning true become stale. The predicate selects targets; `QueryOptions.Disable` and handle `SetEnabled` control automatic loading independently.

The predicate receives a key and runs outside the client lock, so it may call `Get`, `Set`, and other client operations. It evaluates a snapshot of existing keys once each, with no ordering guarantee. Newly added keys are excluded; keys removed before invalidation are skipped. Nil predicates return `ErrInvalidKey`; closed clients return `ErrClosed` without evaluating the predicate.

When inspecting data within a predicate, use the correct static result type for that key. Not every key necessarily contains `User`.

## Structured keys: select a tenant and resource

```go
// ResourceKey identifies a query within a tenant.
type ResourceKey struct {
	// Resource selects the query family.
	Resource string
	// Tenant identifies the data owner.
	Tenant int
}

if err := client.Invalidate(func(key any) bool {
	resource, ok := key.(ResourceKey)
	return ok && resource.Resource == "users" && resource.Tenant == 42
}); err != nil {
	return err
}

```

String prefixes have no built-in semantics. Match prefixes yourself, or use comparable structs and inspect their fields. Structs containing slices or maps are not valid keys.

## Invalidation during a request

By default, an active same-key request is neither canceled nor duplicated. Its result remains stale, preventing pre-invalidation work from being treated as current. Completion does not automatically start a second request. A `Fetch` or `Refetch` made while that request is running shares its result; a call after completion can start a new load.

`RefetchNone` does not cancel work already running; it only suppresses new work. Use `client.Cancel` to cancel, and `client.Remove` to discard data.

## `RefetchAfterLoad`: bounded background follow-up

Opt in without waiting:

```go
if err := client.Invalidate("users", soroi.InvalidateOptions{
	RefetchAfterLoad: true,
}); err != nil {
	return err
}
```

An enabled observer is required. When no load is active, invalidation starts the ordinary background refresh. When a load is active, multiple opted-in invalidations coalesce into one follow-up after it finishes, including when the old load fails or its caller cancels. The follow-up is owned by Client, uses the oldest currently enabled observer's loader and policy, and applies the normal retry, timeout, and execution budget rules.

A load chain permits at most one automatic follow-up. Invalidating that follow-up leaves its result stale without scheduling a third load. Subsequent explicit operations may start another chain. This bound prevents continuous writes from causing an unlimited sequence of refreshes. The old result may appear as stale data while the follow-up runs.

If all observers close or become disabled before dispatch, the follow-up is skipped. `Set`, `Update`, `Cancel`, `Remove`, `Clear`, and Client closure discard scheduled work. `RefetchNone` takes precedence for the current call, but does not cancel a follow-up requested earlier.

## `InvalidateAndWait`: wait for associated refreshes

```go
results, err := client.InvalidateAndWait(ctx, []any{"user:42", "users"})
for _, result := range results {
	fmt.Printf("key=%v refreshed=%t err=%v\n", result.Key, result.Refreshed, result.Err)
}
if err != nil {
	return err
}

```

Import `fmt` for this snippet. This entry enables `RefetchAfterLoad` automatically and accepts the same single key, `[]any`, predicate, and optional configuration as `Invalidate`. Only keys with enabled observers and loaders refresh. Unobserved or disabled keys, and keys selected with `RefetchNone`, are only marked stale and return skipped results. It does not retain loaders for Fetch-only keys.

All selected refreshes are scheduled before waiting; individual failures do not stop other keys. Explicit batches retain first-occurrence order and remove duplicates. Predicate order is unspecified; keys removed during matching are excluded. Invalid selections cause no changes and return no results. Empty selections return an empty result slice.

| `RefreshResult` | Meaning |
| --- | --- |
| `Key` | Selected query key with its original dynamic type |
| `Refreshed: true`, `Err: nil` | A post-invalidation load completed successfully without further invalidation or replacement |
| `Refreshed: false`, `Err: nil` | Refresh skipped because no enabled observer remained, or `RefetchNone` was selected |
| `Refreshed: false`, non-nil `Err` | Load failure, further invalidation, local replacement, or waiting cancellation |

The returned error joins per-key errors with key context; `errors.Is` and `errors.As` retain the original causes. If the awaited load is invalidated again, its receipt reports `ErrRefreshInvalidated`. A later opted-in operation may independently schedule a follow-up, but does not change the earlier receipt. Coalesced waiters awaiting the same follow-up receive its outcome.

`Refreshed` describes completion, not a promise that data remains fresh at return time. For example, zero `StaleTime` makes successfully refreshed data immediately stale. Results are not an atomic multi-key snapshot, and no server-side consistency guarantee is added.

The caller context limits the overall wait, including the old request and follow-up. A pre-canceled context performs no invalidation. Cancellation after scheduling ends only this caller's wait; Client-owned loads and other waiters continue. Completed and skipped results are preserved; remaining targets report the context error. Explicit `Cancel`, `Remove`, or `Clear` returns `context.Canceled` for affected waits; `Set` or `Update` returns `ErrRefreshSuperseded`; Client closure returns `ErrClosed`, without waiting for detached callbacks to finish.

Waiting through a callback context that already holds the selected observer's Executor returns `ErrExecutionReentry` for that key without scheduling additional work. Avoid waiting on your own active query or circular backend dependencies. Detection relies on the supplied context and the selected Client-layer budget; it cannot infer hidden Batcher budgets or application dependency cycles.

## Errors

| Error | Condition |
| --- | --- |
| `ErrInvalidKey` | Nil target, nil predicate, or a nil or non-comparable key in a batch |
| `ErrClosed` | Client has been closed, including when the target would match no keys |
| `ErrRefreshInvalidated` | The awaited refresh was invalidated again while running |
| `ErrRefreshSuperseded` | A local `Set` or `Update` replaced the awaited refresh |
| `ErrExecutionReentry` | A waiting callback already holds the selected refresh budget |

The last three errors belong to `InvalidateAndWait`. Load and context errors are also returned through its per-key results. `Invalidate` remains non-waiting and reports selection errors only.

A nil or empty `[]any` batch succeeds on an open client. Other slice types are
not interpreted as batches and return `ErrInvalidKey`. Invalid batch keys are
rejected before any key is marked stale. Background refresh errors are reported
through query state, rather than returned by `Invalidate`.

## Verification

```sh
go test -race ./e2e -run 'Test(SharedClientMutation|DeferredPredicateInvalidation)' -count=1
```

[HTTP e2e tests](../e2e/query_lifecycle_test.go) perform a real PUT mutation, check mixed-type detail/list refresh and unrelated data isolation, and confirm deferred predicate invalidation issues no HTTP request.
