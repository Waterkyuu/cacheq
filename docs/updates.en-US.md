# Conflict-safe cache updates

[简体中文](updates.zh-CN.md) · [Cache lifecycle](cache.en-US.md) · [Mutations](mutations.en-US.md)

`Update` transforms an existing cached value without silently overwriting concurrent changes. It runs the application callback once outside the Client lock, then commits only if the captured cache revision is still current. It does not load missing data or write to a backend.

## Runnable example

Save this program in a Go module that imports cacheq and run `go run .`.

```go
package main

import (
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main updates a cached counter without an unprotected read-modify-write sequence.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	if err := cacheq.Set(client, "counter", 1); err != nil {
		panic(err)
	}
	if err := cacheq.Update(client, "counter", func(value int) (int, error) {
		return value + 1, nil
	}); err != nil {
		panic(err)
	}
	fmt.Println(cacheq.Get[int](client, "counter").Data)
}
```

Expected output:

```text
2
```

## Commit contract

- The callback receives existing usable data, including stale data. Missing or over-age data returns `ErrNoData` without invoking the callback.
- The callback executes exactly once, outside internal locks. It may call Client APIs. A callback error is returned unchanged and prevents installation.
- Writes, completed refreshes (including failures), explicit invalidation, forced refresh, removal, eviction, and age expiration can invalidate the captured revision. Conflicts return `ErrUpdateConflict`; the callback is not automatically repeated.
- Removing and recreating the key cannot hide a conflict, even if the new value equals the old one. Unrelated keys do not conflict.
- Success has `Set` semantics: it installs ordinary fresh data using Client defaults, renews `UpdatedAt` and `MaxAge`, clears the previous error, cancels older loads, and notifies observers. Diagnostics report an `EventLocalWrite` with `ReasonSet`.
- Updates are atomic for one key. They are not multi-key or backend transactions. Revision validation does not roll back application side effects.

## Shared values and retries

Cached values are shared and immutable. Copy slices, maps, and pointed-to objects before modifying them; mutating the callback argument in place bypasses conflict protection and may race with readers. The library does not deep-copy application data.

When `errors.Is(err, cacheq.ErrUpdateConflict)` is true, the application may retry the entire `Update` call with a bounded retry policy. Keep transformation callbacks free of external side effects if they might be invoked again by the application. `Options.Retry` does not apply to local updates.

Use `Set` to install authoritative complete data or initialize a missing key. Use [Mutation](mutations.en-US.md) for remote writes followed by cache synchronization.

## Errors and verification

`ErrNoUpdater` rejects a nil callback. Invalid keys, incompatible result types, and closed clients return `ErrInvalidKey`, `ErrTypeMismatch`, and `ErrClosed`. Closing the Client during a callback prevents its result from being installed.

```sh
go test -race ./... -run TestUpdate -count=1
```

[Tests](../update_test.go) cover overlapping transformations, explicit retries, reentrant callbacks, invalidation, removal and recreation, age and capacity limits, callback errors, and late loader results.
