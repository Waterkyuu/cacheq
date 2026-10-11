# Mutations and associated invalidation

[简体中文](mutations.zh-CN.md) · [Atomic updates](updates.en-US.md) · [Invalidation](invalidation.en-US.md)

`Mutation` creates a typed handle for a business write, its execution state, and cache synchronization after success. Writes are never retried or deduplicated. The application remains responsible for backend transactions, authorization, and idempotency.

## Runnable example

Save this program in a Go module that imports cacheq and run `go run .`. The write uses a local value and needs no external service.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main separates a committed write from subsequent cache synchronization.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	if err := cacheq.Set(client, "users", []string{"Alice"}); err != nil {
		panic(err)
	}
	change := cacheq.Mutation(
		client,
		func(_ context.Context, name string) (string, error) { return name, nil },
		cacheq.MutationOptions[string, string]{
			OnSuccess: func(_ string, name string) error {
				return cacheq.Set(client, "user:42", name)
			},
			Invalidate:        func(string, string) any { return []any{"users"} },
			InvalidateOptions: cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
		},
	)
	defer change.Close()
	result, err := change.Mutate(context.Background(), "Bob")
	if err != nil {
		panic(err)
	}
	if result.SyncErr != nil {
		panic(result.SyncErr)
	}
	fmt.Println(result.Data)
	fmt.Println(cacheq.Get[[]string](client, "users").Stale)
}
```

Expected output:

```text
Bob
true
```

## Execution and state

`Mutator[I, V]` is `func(context.Context, I) (V, error)`. `Mutation` starts no work; `Mutate(ctx, input)` executes once and waits for the write and synchronization callbacks. The last `MutationOptions` wins. Options are copied; captured application state still needs synchronization.

`Snapshot()` and `Updates()` expose `MutationSnapshot[V]`: `Status`, `Data`, `HasData`, `Err`, and `SyncErr`. Initial status is `Idle`; execution publishes `Pending`, clearing the previous outcome. Success includes valid zero-valued data. Slow subscribers may skip intermediate states.

Only one call, including synchronization, may run per handle. Overlapping calls return `ErrMutationBusy` without queuing or modifying the active state. Repeating the same input after completion performs another write. Different handles may execute concurrently; they do not serialize writes by resource key.

## Synchronization contract

After the write returns success:

1. Run `OnSuccess(input, result)` when provided. Use `Set` for authoritative data or [Update](updates.en-US.md) for a transformation.
2. Evaluate `Invalidate(input, result)` and pass a non-nil target to `Client.Invalidate`. The target may be a key, `[]any`, or `func(any) bool`. Nil skips invalidation.
3. Combine synchronization errors into `MutationResult.SyncErr` and `MutationSnapshot.SyncErr`, preserving `errors.Is` and `errors.As`.

Invalidation still runs if `OnSuccess` returns an error. Both callbacks run outside internal locks and may use Client APIs. A backend error prevents both callbacks.

The `Mutate` error describes admission or business execution failure. **A nil error means the write succeeded, even when `SyncErr` is non-nil.** Repair or invalidate cache state instead of repeating the committed write solely because synchronization failed. Synchronization is not transactional across keys or with the backend.

Return write success only after a database transaction commits. Include tenant/user scope in cache keys and invalidation predicates. No tags or data relationships are inferred automatically.

Invalidation uses existing semantics: enabled observed queries refresh in the background by default; `RefetchNone` only marks data stale. Mutation completion does not wait for those refreshes. An already-running load stays stale after in-flight invalidation and is not automatically followed by another load. See the [invalidation contract](invalidation.en-US.md).

## Cancellation and execution budgets

Caller cancellation, `Close`, and Client closure cancel the write context. Callbacks must cooperate; closing does not wait for callbacks that ignore cancellation. Client closure asynchronously closes mutation update channels. Explicit `Close` closes the channel immediately and is idempotent.

Cancellation does not prove a remote write was rolled back. If the callback returns success despite concurrent cancellation, synchronization still runs, the returned write error is nil, and the snapshot records success. Closed handles send no further notifications. A closed Client can then cause a separate `SyncErr`.

`MutationOptions.Executor` optionally overrides the Client's [execution budget](execution.en-US.md); nil inherits it. Capacity is held only around the write callback, released before cache synchronization. Queue admission failure returns `ErrQueueFull` without executing the write. Client query retry and timeout defaults do not apply to mutations; use a caller context deadline.

## Errors and verification

Nil writes return `ErrNoMutator`; released handles return `ErrMutationClosed`; closed clients return `ErrClosed`. Construction errors are available through state and execution. Already-canceled calls do not change state or invoke callbacks.

```sh
go test -race ./... -run TestMutation -count=1
```

[Tests](../mutation_test.go) cover successful synchronization, associated queries, repeated inputs, write failures, synchronization failures, overlapping calls, cancellation, closure, and late confirmed writes.
