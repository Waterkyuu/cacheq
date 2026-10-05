<div align="center">
  <img src="./assets/go-query.png" alt="go-query" width="144" />

  <h1>go-query</h1>

  <p><strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a></p>

  <p><strong>Cache, retry, and share your queries</strong></p>
  <p>A typed, dependency-free query client for Go 1.22 and later</p>
  <p>Share data across components, combine duplicate requests, and observe query state.</p>

  <p>
    <img src="https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.22+" />
  </p>

  <p><a href="#features">Features</a> · <a href="#install">Install</a></p>
</div>

## Features

- Data caching with configurable `StaleTime` and visible expiration.
- One in-flight request per key; different keys load independently.
- Bounded retries with exponential backoff, custom delay, and request timeout.
- Shared data and state subscriptions for multiple components.
- Immediate stale data with background refresh through `Query`.
- Manual invalidation, forced refresh, prefetch, local updates, and cancellation.

## Install

```sh
go get github.com/Waterkyuu/go-query@v0.1.0
```

## Fetch and share data

```go
package main

import (
    "context"
    "fmt"
    "time"

    query "github.com/Waterkyuu/go-query"
)

func main() {
    client := query.NewClient[string, string](query.Options{
        StaleTime: time.Minute,
        Retry:     2,
        Timeout:   10 * time.Second,
    })
    defer client.Close()

    fetch := func(ctx context.Context) (string, error) {
        return "hello", nil
    }
    value, err := client.Fetch(context.Background(), "greeting", fetch)
    fmt.Println(value, err)
}
```

Reuse this client in every component that reads the same key. Fresh reads return cached data. Concurrent reads of a stale or missing key share one request. A key must identify the complete request, including parameters, provider, or user identity when relevant. Values are shared and read-only; clone slices and maps before modifying them.

## Check freshness and refresh in the background

```go
state := client.Snapshot("greeting") // reads state without a request
fmt.Println(state.HasData, state.Stale, state.Fetching, state.Err)

state = client.Query("greeting", fetch) // returns current data immediately
// If stale or missing, one background request updates the shared result.
```

`Status` is `Idle`, `Pending`, `Success`, or `Error`. `HasData` distinguishes missing data from an available zero value. `Fetching` is independent of the result status, so a successful result remains visible while refreshing. `UpdatedAt` and `ExpiresAt` expose the data's age and freshness deadline. Failed refreshes retain earlier data while exposing the error.

## Observe changes

```go
updates, unsubscribe := client.Subscribe("greeting")
defer unsubscribe()

for state := range updates {
    // Send state to your own UI or application event loop.
    _ = state
}
```

Subscriptions deliver the initial and latest states. Slow readers can skip intermediate transitions. Unsubscribe when a component closes. The loop ends when the subscription or client closes; start `Query` or `Fetch` separately to load data.

## Control the shared result

| Method | Behavior |
| --- | --- |
| `Invalidate(key)` | Marks data stale; subscribed queries refresh automatically. |
| `Refetch(ctx, key, fetch)` | Loads again even when data is fresh, sharing an active request. |
| `Prefetch(ctx, key, fetch)` | Warms the same cache before a component needs it. |
| `Set(key, value)` | Installs local or optimistic data and prevents an older response from overwriting it. |
| `Cancel(key)` | Cancels active work while retaining completed data. |
| `Remove(key)` | Removes data and prevents an old load from restoring it. |
| `Clear()` | Clears all results and cancels current requests. |
| `Close()` | Stops work, closes subscriptions, and rejects new requests. |

## Freshness, retries, and cancellation

`StaleTime` defaults to zero: completed data is immediately stale. Set a positive duration to avoid unnecessary requests. `Retry` counts additional attempts and defaults to zero. When enabled, default backoff starts at one second and doubles up to thirty seconds; override `RetryDelay` when needed. Cancellation and deadline errors are never retried. `Timeout` bounds a complete load including retries.

The first waiting caller controls its load through its context. Canceling another waiter leaves that load running. If the owner cancels, remaining callers may retry with their own contexts. Background loads belong to the client and stop on `Close`. Fetchers must honor context cancellation.

## Preserve a disk snapshot's expiration

`FetchWithExpiry` accepts a loader returning `(value, expiresAt, error)`. This keeps the absolute freshness of data restored from disk instead of granting it a new `StaleTime`. A zero expiration disables fresh reuse. Returning fallback data, a future expiration, and an error delays new requests until that deadline. Disk persistence and HTTP transport remain application-owned.

Inject `Options.Clock` for deterministic freshness checks. The client provides no browser focus integration or automatic garbage collection; remove unused keys explicitly and close the client at the end of its lifetime.
