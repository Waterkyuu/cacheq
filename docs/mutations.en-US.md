# Mutations and associated invalidation

[简体中文](mutations.zh-CN.md) · [Atomic updates](updates.en-US.md) · [Invalidation](invalidation.en-US.md)

`Mutation` executes a business write, exposes its state, and synchronizes related cache entries after success. For example, changing a user's name can update the detail cache directly and reload the user list.

The application implements HTTP requests or database writes; cacheq coordinates cache synchronization afterward. Creating a Mutation starts no request. Calling `Mutate` begins execution.

## Quick start

This example changes a user's name, stores the returned user in the detail cache, and refreshes an observed user list. Save the complete program as `main.go` in a module importing cacheq, then run `go run .`.

An `atomic.Value` stores local backend data, so no external service is needed. Reads and writes can run concurrently with the list's background refresh.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// User contains the profile returned by the backend.
type User struct {
	// Name contains the user's display name.
	Name string
}

// main synchronizes a detail cache and an observed list after changing a user.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	ctx := context.Background()

	var backend atomic.Value
	backend.Store(User{Name: "Alice"})
	getUsers := func(context.Context) ([]User, error) {
		return []User{backend.Load().(User)}, nil
	}
	users := cacheq.Query(client, "users", getUsers)
	defer users.Close()
	before, err := cacheq.Fetch(ctx, client, "users", getUsers)
	if err != nil {
		panic(err)
	}
	fmt.Println("before:", before[0].Name)

	rename := cacheq.Mutation(
		client,
		func(ctx context.Context, name string) (User, error) {
			if err := ctx.Err(); err != nil {
				return User{}, err
			}
			user := User{Name: name}
			backend.Store(user)
			return user, nil
		},
		cacheq.MutationOptions[string, User]{
			OnSuccess: func(ctx context.Context, user User, _ string) error {
				updateErr := cacheq.Set(client, "user:42", user)
				_, refreshErr := client.InvalidateAndWait(ctx, "users")
				return errors.Join(updateErr, refreshErr)
			},
		},
	)
	defer rename.Close()
	result, err := rename.Mutate(ctx, "Bob")
	if err != nil {
		panic(err)
	}
	if result.SyncErr != nil {
		panic(result.SyncErr)
	}

	after := users.Snapshot().Data
	fmt.Println("detail:", cacheq.Get[User](client, "user:42").Data.Name)
	fmt.Println("list:", after[0].Name)
}
```

Expected output:

```text
before: Alice
detail: Bob
list: Bob
```

`rename.Mutate(ctx, "Bob")` first changes backend data. After success, `OnSuccess` stores the returned `User` under `"user:42"` and calls `client.InvalidateAndWait(ctx, "users")`. The list has an enabled `Query`, so it refreshes before the callback returns.

`Mutate` waits for the write and `OnSuccess` to finish. The callback chooses whether to refresh in the background through `Client.Invalidate` or wait through `Client.InvalidateAndWait`. This example waits, so the list snapshot contains Bob when `Mutate` returns without a synchronization error.

## Snippet prerequisites

The following partial snippets use `client`, `rename`, `User`, and a non-nil `ctx` from the quick start. Place snippets with `return` inside a business function returning `error`, and import the standard library packages they use. Configuration snippets replace the `MutationOptions` supplied when constructing a Mutation.

## `Mutation` and `Mutate`: creation and execution

`Mutation(client, write, options)` returns `MutationHandle[I, V]`. `I` is the input type and `V` the write result type; the example uses `string` and `User`. The business callback, `Mutator[I, V]`, has the signature `func(context.Context, I) (V, error)`.

`Mutate(ctx, input)` executes the business callback once, waits for cache synchronization after success, and returns `MutationResult[V]` and an error. Database operations must return success only after the transaction commits. cacheq does not retry or deduplicate writes. Repeating the same input after completion executes another write.

One handle accepts only one execution at a time, including cache synchronization. Overlapping calls return `ErrMutationBusy` without queuing or changing the active state. Different handles can execute concurrently; they do not automatically serialize by user ID or another business key.

## `OnSuccess`: caching the write result

The example's write returns a complete user detail, so `OnSuccess` calls `Set` without requesting that detail again. To change one item in an existing list, use [Update](updates.en-US.md) inside the callback.

The callback has the signature `func(ctx context.Context, result V, input I) error`. `result` is the successful return value of the business write; `input` is the original argument passed to `Mutate`. The context preserves the caller's values and deadline and is also canceled when the handle or Client closes. Go requires the declared callback signature; use `_` for unused parameters.

The callback runs once after a successful business write and never after a failed write. It runs outside the Client lock and may call cache APIs. The handle stays `Pending` until the callback returns. Returning an error reports a synchronization failure without undoing the committed business write.

This follows [TanStack Query's mutation invalidation model](https://tanstack.com/query/latest/docs/framework/react/guides/invalidations-from-mutations): call the query client's cache APIs inside the success callback and wait for the synchronization work there. In Go, the callback calls a blocking method and returns an error instead of returning a JavaScript Promise. cacheq retains its separate `SyncErr` contract for failures after a committed write.

## Refreshing related queries from `OnSuccess`

Call `Client.Invalidate` directly with a query key, `[]any`, or a predicate. The application declares relationships between details and lists; Mutation does not select targets automatically. For a fixed list key, no target-producing callback is needed:

```go
options := cacheq.MutationOptions[string, User]{
	OnSuccess: func(context.Context, User, string) error {
		return client.Invalidate("users", cacheq.InvalidateOptions{RefetchAfterLoad: true})
	},
}
```

This marks the list stale and schedules background refreshing, including one bounded follow-up if a request is already running. `Mutate` waits for the callback, but the callback returns before the HTTP refresh completes. Background errors appear in the corresponding query state.

To defer loading, pass `RefetchNone` at the Client call:

```go
options := cacheq.MutationOptions[string, User]{
	OnSuccess: func(context.Context, User, string) error {
		return client.Invalidate(
			[]any{"user:42", "users"},
			cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
		)
	},
}
```

This retains old detail and list data. A subsequent `Fetch`, new enabled `Query`, or manual `Refetch` can reload it. This callback does not install the write result.

The callback owns synchronization order and error handling. An early return stops its remaining steps. To attempt both a cache update and invalidation even when one fails, execute both and combine their errors with `errors.Join`, as in the quick start. Include tenant and user scope in selected keys.

## Waiting for refresh after a committed write

Call `Client.InvalidateAndWait` inside `OnSuccess` to wait for associated refreshes. A callback that only refreshes a fixed list is:

```go
options := cacheq.MutationOptions[string, User]{
	OnSuccess: func(ctx context.Context, _ User, _ string) error {
		_, err := client.InvalidateAndWait(ctx, "users")
		return err
	},
}
```

`Mutate` waits for the list's post-invalidation refresh, including the bounded follow-up if an earlier request is still running. Read `users.Snapshot().Data` after checking both `err` and `result.SyncErr`. Use the returned per-key results when the application needs to distinguish refreshed and skipped targets.

The handle remains `Pending` and rejects overlapping executions during this wait. Enabled observed queries refresh; unobserved or disabled keys are only marked stale, and `RefetchNone` suppresses refreshing. The waiting contract is the same as `Client.InvalidateAndWait`, including further-invalidation and local-replacement errors. Waiting does not establish business ordering across Mutation handles or server-side consistency.

The `Mutate` context and handle lifetime bound waits that use the supplied callback context. Cancellation after a committed write is reported through `SyncErr` when the callback returns that error, while the returned business error remains nil and status remains `Success`. Client closure can report `ErrClosed` or context cancellation, depending on which signal is observed first. Client-owned refreshes may continue after the caller stops waiting. Execution budget capacity is released before the callback starts.

A late successful write still invokes `OnSuccess`, even with a canceled context. `Client.InvalidateAndWait` rejects a pre-canceled context without invalidating. When a confirmed write must always mark cached data stale, call `Client.Invalidate` independently of cancellation before any cancellable waiting step. Mutation performs no implicit invalidation after the callback returns.

## Migrating previous Mutation options

This API change removes `MutationOptions.Invalidate`, `InvalidateOptions`, and `WaitForRefresh`, and changes the `OnSuccess` signature from `func(input I, result V) error` to `func(ctx context.Context, result V, input I) error`.

Move target selection and refresh options into explicit Client calls in `OnSuccess`. Replace background invalidation with `client.Invalidate(target, options)` and awaited invalidation with `client.InvalidateAndWait(ctx, target, options)`. Cache updates and invalidation now share one callback, which controls their ordering and combined errors.

## Distinguishing write failures from synchronization failures

The `error` returned by `Mutate` means the write did not complete successfully, including rejected calls and business callback errors. `result.SyncErr` means the business write succeeded but a subsequent cache update, invalidation, or opted-in refresh wait failed.

```go
result, err := rename.Mutate(ctx, "Bob")
if err != nil {
	return err
}
if result.SyncErr != nil {
	fmt.Printf("user saved; cache synchronization failed: %v\n", result.SyncErr)
}
fmt.Println(result.Data.Name)
return nil
```

**When `err == nil`, do not repeat a business write solely because `SyncErr` is non-nil.** For example, if a payment completes but its cache update fails, repair the cache or query the business state again.

The error returned by `OnSuccess` becomes `SyncErr`, preserving `errors.Is` and `errors.As`. Use `errors.Join` in the callback to retain errors from multiple synchronization steps. Result data remains available and the Mutation status remains `Success`. Applications can record the error and arrange cache repair or another query.

## `Snapshot` and `Updates`: execution state

`rename.Snapshot()` returns the current `MutationSnapshot[User]`. `rename.Updates()` supplies the initial state and subsequent latest states.

| Status | Meaning |
| --- | --- |
| `Idle` | Created but not yet executed |
| `Pending` | Waiting for capacity, writing, or synchronizing; the previous result is cleared |
| `Success` | Business write succeeded and `HasData` is true; still inspect `SyncErr` |
| `Error` | Execution failed, with the cause in `Err` |

Successful zero-valued data is valid. A snapshot is a value and does not update itself. Slow consumers may skip intermediate states; `Updates()` is not a retained event log. Rejected overlapping calls and calls canceled before starting do not change the current state.

## Options

| `MutationOptions` field | Default | Purpose |
| --- | --- | --- |
| `OnSuccess` | nil | Run post-commit cache updates and explicit invalidation; wait until the callback returns |
| `Executor` | nil, inherits Client configuration | Limit concurrent and queued write callbacks |

The last options value wins; fields are not merged. Options are copied, but captured application data still needs synchronization. Cache keys and invalidation predicates should include tenant and user scope that affects results.

An [execution budget](execution.en-US.md) holds capacity only during the business write, releasing it before cache synchronization. Client query retry and timeout settings do not apply to Mutation. Set write deadlines through the context passed to `Mutate`.

## Cancellation and closing

Caller cancellation, `rename.Close()`, or Client closure cancels the write context. Business callbacks must honor cancellation; `Close()` does not wait for callbacks that ignore it.

Explicit handle closure immediately closes `Updates()` and is idempotent. Client closure also closes its Mutation update channels asynchronously. `Mutate` may continue waiting until the business callback finishes.

Cancellation does not prove a remote write was rolled back. If the callback returns success after cancellation, cacheq still synchronizes the cache and retains the successful result. Closed handles send no further notifications, but `Snapshot()` records the eventual result. A closed Client can cause a separate `SyncErr` during synchronization.

## Errors

| Condition | Error or outcome |
| --- | --- |
| Nil business callback | `ErrNoMutator` |
| Same handle already executing | `ErrMutationBusy` |
| Closed handle | `ErrMutationClosed` |
| Closed Client | `ErrClosed` |
| Full execution and queue capacity | `ErrQueueFull`, without invoking the business callback |
| Context canceled before starting | Context error, without invoking the business callback |
| Business callback error | Reported through the returned `error` and snapshot `Err` |
| Cache synchronization fails after write success | Reported through result and snapshot `SyncErr` |

Construction errors are exposed through `Snapshot().Err`, the initial `Updates()` state, and subsequent `Mutate` calls. This feature provides no multi-key transaction, backend rollback, or automatic idempotency handling.

## Related guides

- [Atomic cache updates](updates.en-US.md): change a list or merge partial data using existing cache contents.
- [Invalidation](invalidation.en-US.md): single-key, batch, and predicate invalidation and background refresh behavior.
- [Execution budgets](execution.en-US.md): limit backend concurrency for queries, batches, and writes.
