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
			OnSuccess: func(_ string, user User) error {
				return cacheq.Set(client, "user:42", user)
			},
			Invalidate: func(string, User) any { return "users" },
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

	after, err := cacheq.Fetch(ctx, client, "users", getUsers)
	if err != nil {
		panic(err)
	}
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

`rename.Mutate(ctx, "Bob")` first changes backend data. After success, `OnSuccess` stores the returned `User` under `"user:42"`, and `Invalidate` marks `"users"` stale. The list has an enabled `Query`, so invalidation starts a background refresh.

`Mutate` waits for the write and synchronization callbacks, but not for the list's background refresh. The final `Fetch` joins that refresh or reuses its completed data, returning a list containing Bob.

## Snippet prerequisites

The following partial snippets use `client`, `rename`, `User`, and a non-nil `ctx` from the quick start. Place snippets with `return` inside a business function returning `error`, and import the standard library packages they use. Configuration snippets replace the `MutationOptions` supplied when constructing a Mutation.

## `Mutation` and `Mutate`: creation and execution

`Mutation(client, write, options)` returns `MutationHandle[I, V]`. `I` is the input type and `V` the write result type; the example uses `string` and `User`. The business callback, `Mutator[I, V]`, has the signature `func(context.Context, I) (V, error)`.

`Mutate(ctx, input)` executes the business callback once, waits for cache synchronization after success, and returns `MutationResult[V]` and an error. Database operations must return success only after the transaction commits. cacheq does not retry or deduplicate writes. Repeating the same input after completion executes another write.

One handle accepts only one execution at a time, including cache synchronization. Overlapping calls return `ErrMutationBusy` without queuing or changing the active state. Different handles can execute concurrently; they do not automatically serialize by user ID or another business key.

## `OnSuccess`: caching the write result

The example's write returns a complete user detail, so `OnSuccess` calls `Set` without requesting that detail again. To change one item in an existing list, use [Update](updates.en-US.md) inside the callback.

The callback runs once after a successful business write, receiving the original input and result. It does not run after a failed write. It runs outside the Client lock and may call cache APIs. Returning an error does not undo the committed business write.

## `Invalidate`: refreshing related queries

The `Invalidate` callback returns query keys to invalidate. A single key, `[]any`, and `func(any) bool` select an exact key, explicit keys, and matching keys respectively. Returning nil skips invalidation. The application must declare relationships between details and lists; cacheq does not infer them.

By default, keys with enabled query handles refresh in the background; other keys are only marked stale. Use this configuration to defer loading:

```go
options := cacheq.MutationOptions[string, User]{
	Invalidate: func(string, User) any {
		return []any{"user:42", "users"}
	},
	InvalidateOptions: cacheq.InvalidateOptions{
		Refetch: cacheq.RefetchNone,
	},
}
```

This retains old detail and list data. A subsequent `Fetch`, new enabled `Query`, or manual `Refetch` can reload it. The configuration has no `OnSuccess`, so it does not directly install the write result.

Execution proceeds through the business write, `OnSuccess`, and then `Invalidate`. Invalidation still runs when `OnSuccess` returns an error. Invalidation itself does not wait for refreshes; background load errors are reported through the corresponding query state.

Invalidating an active load does not duplicate it. Its result remains stale, and no follow-up load starts automatically. To wait for fresh data, use `Fetch` or `Refetch` according to the [invalidation rules](invalidation.en-US.md).

## Distinguishing write failures from synchronization failures

The `error` returned by `Mutate` means the write did not complete successfully, including rejected calls and business callback errors. `result.SyncErr` means the business write succeeded but a subsequent cache update or invalidation failed.

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

Errors from `OnSuccess` and invalidation are combined in `SyncErr`, preserving `errors.Is` and `errors.As`. Result data remains available and the Mutation status remains `Success`. The snippet reports synchronization failure separately and continues using the confirmed write result. Applications can record the error and arrange cache repair or another query.

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
| `OnSuccess` | nil | Update local cache after write success |
| `Invalidate` | nil | Select queries to invalidate after write success |
| `InvalidateOptions` | `RefetchObserved` | Refresh observed queries in the background or only mark stale |
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
