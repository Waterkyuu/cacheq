# Per-consumer query options

[简体中文](query-options.zh-CN.md)

`QueryOptions` configures one `Query` handle; `FetchOptions` configures one `Fetch` call. Both override Client defaults for freshness, retries, and timeouts. `QueryOptions` additionally provides `Disable` to control automatic loading.

## Quick start

Append options to the call. This complete example gives `Query` and `Fetch` a one-hour freshness window while leaving the Client's one-minute default unchanged.

Save the program as `main.go` in a module importing cacheq, then run `go run .`.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main applies the same freshness override to a query handle and a Fetch call.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	load := func(context.Context) (string, error) {
		return "hello", nil
	}
	hour := time.Hour

	query := cacheq.Query(
		client,
		"greeting",
		load,
		cacheq.QueryOptions{StaleTime: &hour},
	)
	defer query.Close()

	value, err := cacheq.Fetch(
		context.Background(),
		client,
		"greeting",
		load,
		cacheq.FetchOptions{StaleTime: &hour},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
}
```

Expected output:

```text
hello
```

`Query` accepts `QueryOptions`, and `Fetch` accepts `FetchOptions`. The example's calls share the cache and any active request for `"greeting"`. The `StaleTime` overrides change only their respective freshness decisions. Unset retry and timeout fields continue to use Client defaults.

## QueryOptions

Pass options through `Query(client, key, load, options)`. Configuration is captured when the handle is created and remains in effect for automatic loading, `SetEnabled`, and manual `Refetch`.

In addition to the shared fields below, `QueryOptions` supports:

| Field | Type | Default | Behavior |
| --- | --- | --- | --- |
| `Disable` | `bool` | false | True prevents automatic loading while retaining cache reads and state updates |

For example, pass this configuration to create a query without automatic loading:

```go
options := cacheq.QueryOptions{
	Disable: true,
}
```

While disabled, construction and invalidation do not trigger automatic loading for this handle. Calling `query.SetEnabled(true)` permits missing or stale data to load. Changing the original `options.Disable` does not reconfigure the handle. Disabling does not cancel active shared requests.

## FetchOptions

Pass options through `Fetch(ctx, client, key, load, options)`. Configuration applies only to this call. It neither changes Client defaults nor becomes the configuration for later calls using the same key.

For example, disable retries and set a three-second load timeout:

```go
retries := 0
timeout := 3 * time.Second
options := cacheq.FetchOptions{
	Retry:   &retries,
	Timeout: &timeout,
}
```

Pass `options` as the final argument to `Fetch`. `FetchOptions` has no `Disable` field: calling `Fetch` requests data, and cache state determines whether loading is necessary.

## Shared fields

The following fields apply to both `QueryOptions` and `FetchOptions`.

| Field | Type | When nil | When set |
| --- | --- | --- | --- |
| `StaleTime` | `*time.Duration` | Inherit Client freshness | Calculate freshness from the data's `UpdatedAt`; zero means immediately stale |
| `Retry` | `*int` | Inherit Client retry count | Additional attempts after the first failure; zero or negative disables retries |
| `RetryIf` | `func(error) bool` | Inherit Client error filter | True permits an additional attempt, subject to the retry count and terminal errors |
| `RetryDelay` | `func(int) time.Duration` | Inherit Client retry delay | Return the delay before an additional attempt; numbering starts at 1 |
| `Timeout` | `*time.Duration` | Inherit Client load timeout | Bound the entire load, including queuing, execution, and retry delays; zero or negative adds no timeout |

Pointer fields distinguish inheritance from explicit zero values. For example, `Retry: nil` inherits the default retry count; a pointer to 0 disables retries. Caller context deadlines and cancellation remain effective even with a zero `Timeout`.

Cancellation, deadline errors, `ErrQueueFull`, and `ErrBatcherClosed` never retry, including wrapped errors, and bypass `RetryIf`. The predicate runs only when attempts remain and the error is retryable. A predicate always returning true can override the Client filter but cannot bypass terminal errors.

`RetryIf` and `RetryDelay` run outside the Client lock and may call Client APIs. Applications must synchronize shared mutable state captured by callbacks.

## Defaults and overrides

- Omitting options or passing an empty value uses Client defaults. `QueryOptions.Disable` defaults to false.
- If several options values are supplied, only the last applies; their fields are not merged. Nil fields in the last value inherit Client defaults.
- Pointed-to values are copied when creating `Query` or calling `Fetch`. Later changes to the original variables do not alter existing configuration.
- The same key still shares data and requests. Different options do not create separate caches or replace query parameters and identity scope in the key.

## Configuration for shared requests

A new load uses its initiator's loader, retry settings, and timeout. Later same-key calls do not replace them. A joiner's context can end its own wait, but its `Timeout` setting applies only to new loads it initiates.

Automatic refresh after invalidation uses the oldest still-enabled handle's configuration. After that handle closes or is disabled, the next enabled handle supplies future automatic refreshes. Manual `Refetch` uses the current handle's configuration when starting a new load.

## Cache policy limits

`StaleTime` changes ordinary cache freshness decisions without enabling polling. Passing time does not automatically publish notifications or start requests; `Snapshot()` reevaluates state when read. `Get`, `Prefetch`, and `Fetch` without options use Client freshness defaults.

Explicit invalidation and failed refreshes that install no new data make ordinary cached data stale for all queries. Absolute deadlines returned through `FetchWithExpiry` are unaffected by `StaleTime` overrides, including explicitly cached fallback data and errors.

The Client owns cache capacity, `MaxAge`, GC, and the clock. A longer `StaleTime` cannot extend data availability beyond `MaxAge`.

## Compatibility

`FetchWithOptions` remains as a deprecated compatibility entry point; new code uses `Fetch(..., options)`. Existing direct four-argument `Fetch` calls need no changes. In v0.2.0, `Fetch` gains a variadic parameter. Passing `Fetch[V]` as a fixed four-argument function value requires updating the function type or using a forwarding closure with the original signature.

The old `QueryOptions.Enabled *bool` is replaced by `Disable bool`. Omit `Disable` when the old value was nil or pointed to true; use `Disable: true` when it pointed to false. Existing `SetEnabled(bool)` calls remain unchanged.

## Related guides

- [Queries and conditions](queries.en-US.md): `Query`, `Fetch`, state subscriptions, and enablement.
- [Cache and lifecycle](cache.en-US.md): Client defaults, expiration, and resource management.
- [Invalidation](invalidation.en-US.md): invalidate and refresh associated queries.
