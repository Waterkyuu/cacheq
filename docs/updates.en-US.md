# Atomic cache updates

[简体中文](updates.zh-CN.md) · [Cache and lifecycle](cache.en-US.md) · [Mutations](mutations.en-US.md)

`Update` computes a new value from existing cached data. Use it to change an item in a list or merge a partial result. If another operation changes the same key during the computation, `Update` returns a conflict error and preserves that operation's result.

It changes only the current Client's cache and sends no backend request. Use [Mutation](mutations.en-US.md) to commit a business write before synchronizing cached data.

## Quick start

This example changes user 42's cached name to Bob while preserving the previously read list. Save it as `main.go` in a module importing cacheq, then run `go run .`.

```go
package main

import (
	"fmt"
	"slices"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// User contains the fields displayed in a user list.
type User struct {
	// ID identifies the user within the list.
	ID int
	// Name contains the user's display name.
	Name string
}

// main updates one cached list item while preserving previously read data.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	if err := cacheq.Set(client, "users", []User{{ID: 42, Name: "Alice"}}); err != nil {
		panic(err)
	}
	before := cacheq.Get[[]User](client, "users")

	err := cacheq.Update(client, "users", func(users []User) ([]User, error) {
		next := slices.Clone(users)
		for i := range next {
			if next[i].ID == 42 {
				next[i].Name = "Bob"
			}
		}
		return next, nil
	})
	if err != nil {
		panic(err)
	}
	after := cacheq.Get[[]User](client, "users")
	fmt.Println("before:", before.Data[0].Name)
	fmt.Println("after:", after.Data[0].Name)
}
```

Expected output:

```text
before: Alice
after: Bob
```

`Update` passes the current `[]User` to the callback and attempts to store the returned list. `slices.Clone` copies the slice's backing array, so changing the new list leaves `before.Data` unchanged. A successful write notifies query handles for this key.

## Choosing `Set`, `Update`, or `Invalidate`

| Available information | Operation |
| --- | --- |
| A complete new value for the key, or data to initialize the cache | `Set(client, key, value)` |
| A transformation that depends on the key's current data | `Update(client, key, update)` |
| Knowledge that data changed and must be fetched again | `client.Invalidate(key, cacheq.InvalidateOptions{})` |

With separate `Get` and `Set` calls, another write can occur between them and be overwritten by the final `Set`. `Update` checks for changes during computation to prevent an older value from replacing a newer result.

## `Update`: read, compute, and write

`Update[V]` accepts a Client, a query key, and a `func(V) (V, error)` callback. The callback determines the result type, which must match the key's existing type.

The call first reads existing data. Missing data or data beyond `MaxAge` returns `ErrNoData` without invoking the callback. Stale data still within `MaxAge` can be updated.

The callback runs outside the Client lock and may call other Client APIs. Returning an error prevents the computed result from being written and returns that error unchanged. After a successful callback, the key's revision is checked again; only an unchanged revision permits the write. Each `Update` invokes its callback at most once, with no automatic recomputation after conflicts.

A successful update has `Set` semantics: it recalculates freshness using Client defaults, renews `UpdatedAt` and the `MaxAge` starting point, clears the previous error, cancels older loads, and notifies query handles. A zero `StaleTime` makes the new value immediately stale. Atomicity applies to one key in one Client, not backend transactions or multi-key commits.

## Slices, maps, and pointers

Multiple readers share cached values; the callback receives no deep copy. Mutating a shared slice, map, or pointed-to object changes cached data before revision validation and may introduce data races.

The example's `User` contains only integer and string fields, so copying the slice is sufficient. For elements containing maps, slices, or pointers, also copy nested data that will be changed. Both `slices.Clone` and `maps.Clone` make shallow copies.

Keep callbacks focused on computing a new value. Conflict detection can prevent a cache write, but it cannot undo HTTP requests, database writes, or other external side effects performed by a callback.

## Handling missing data and conflicts

This snippet uses `client` and `User` from the quick start inside a function returning `error`. Import `errors` and `slices`. It treats list synchronization as optional: skip missing cached data, and invalidate a conflicting list so a later query can fetch backend data.

```go
err := cacheq.Update(client, "users", func(users []User) ([]User, error) {
	next := slices.Clone(users)
	for i := range next {
		if next[i].ID == 42 {
			next[i].Name = "Bob"
		}
	}
	return next, nil
})
switch {
case errors.Is(err, cacheq.ErrNoData):
	return nil
case errors.Is(err, cacheq.ErrUpdateConflict):
	return client.Invalidate("users", cacheq.InvalidateOptions{})
default:
	return err
}

```

`ErrUpdateConflict` means the computed result was not written. The example's `Invalidate` retains current data and refreshes an enabled observed list by default. Without subscribers, it only marks data stale. See [invalidation](invalidation.en-US.md).

If an operation must be recomputed from the current cached value, retry the entire `Update` after a conflict and bound the number of attempts. Client `Retry` settings do not apply to local updates.

A same-key `Set`, another `Update`, load completion (including failure), explicit invalidation, forced refresh, removal, capacity eviction, or `MaxAge` expiration can cause a pending update to conflict. Removing and recreating a key is also detected, even when the recreated value is equal. Changes to unrelated keys do not conflict.

## Errors

| Condition | Error |
| --- | --- |
| Missing data or data beyond `MaxAge` | `ErrNoData` |
| Same-key state changes while the callback computes | `ErrUpdateConflict` |
| Nil callback | `ErrNoUpdater` |
| Invalid query key | `ErrInvalidKey` |
| Callback data type differs from the key's existing type | `ErrTypeMismatch` |
| Client closed, including during the callback | `ErrClosed` |
| Application callback fails | Original callback error |

Use `errors.Is` to inspect library errors. Missing data, validation failures, and an already-closed Client prevent the callback from running. Closing the Client after a callback starts prevents its result from being written.

## Queries and diagnostics

After success, `Get` reads the new data and existing `Query` handles receive a new state through `Updates()`. Previously returned snapshots are not replaced; shared data must still be treated as immutable.

A successful update emits `EventLocalWrite` with `ReasonSet`. A conflict emits no successful write event. See [diagnostic events](events.en-US.md) for event fields.

## Related guides

- [Cache and lifecycle](cache.en-US.md): direct writes, freshness, maximum data age, and cancellation.
- [Mutations](mutations.en-US.md): synchronize details and lists after a business write succeeds.
- [Invalidation](invalidation.en-US.md): reload associated queries after an update conflict.
