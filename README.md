<div align="center">
  <img src="./assets/cacheq.png" alt="cacheq" width="144" />

  <h1>cacheq</h1>

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

- One shared client caches different result types: details, lists, and configuration.
- Each query retains static typing; incompatible types for a key return an error.
- One `Query` entry point supplies state, updates, enablement, and manual refresh.
- Freshness, background refresh, same-key request sharing, retries, and load timeouts.
- Single-key, mixed-type batch, and predicate invalidation with optional deferred refresh.
- Local writes, prefetching, cancellation, and inactive cache cleanup.

## Install

```sh
go get github.com/Waterkyuu/cacheq
```

Import `github.com/Waterkyuu/cacheq` as package `cacheq`. Requires Go 1.22 or later and has no external dependencies.

## One client, different data types

```go
// User contains a user's display data.
type User struct {
	// Name contains the user's display name.
	Name string
}

client := cacheq.NewClient(cacheq.Options{
	StaleTime: time.Minute,
	GCTime:    5 * time.Minute,
})
defer client.Close()

getUser := func(context.Context) (User, error) {
	return User{Name: "Alice"}, nil
}
getUsers := func(context.Context) ([]User, error) {
	return []User{{Name: "Alice"}}, nil
}

detail := cacheq.Query(client, "user:42", getUser) // QueryHandle[User]
users := cacheq.Query(client, "users", getUsers)   // QueryHandle[[]User]
defer detail.Close()
defer users.Close()

state := users.Snapshot()
// state.Data is []User; state.Fetching reports an active request.
```

Create the client at the application boundary and inject it into consumers. Same-key queries share data and in-flight work; different keys may contain different types. Sharing is in-process.

`Query` returns a handle immediately. Missing data loads in the background, fresh data is reused, and stale data stays available during refresh. Receive later state through `Updates()`. Use `cacheq.Fetch(ctx, client, key, fetcher)` to await fresh data without a subscription.

## Load only when a condition permits

```go
users := cacheq.Query(
	client,
	"users",
	getUsers,
	cacheq.QueryOptions{
		Enabled: loggedIn,
	},
)
defer users.Close()

// Update permission from the application's login-state change handler.
if err := users.SetEnabled(true); err != nil {
	return err
}

// Manual refresh uses the bound fetcher and also works while disabled.
updatedUsers, err := users.Refetch(ctx)
```

Omitting options enables loading; an explicit empty `QueryOptions{}` disables it. Each consumer controls its own automatic requests while still receiving updates produced by other consumers.

## Invalidate related data after a mutation

```go
if err := client.Invalidate([]any{"user:42", "users"}); err != nil {
	return err
}
```

`Invalidate` accepts one key, a `[]any` batch, or a `func(any) bool` predicate. Detail `User` and list `[]User` results are invalidated in the same client. Enabled consumers refresh by default; `Refetch: cacheq.RefetchNone` marks data stale without initiating work until a later use.

## Feature guides

Read the [documentation website](https://waterkyuu.github.io/cacheq/) in English or Chinese.

| Guide | Coverage |
| --- | --- |
| [Queries and conditions](docs/queries.en-US.md) | `Query`, snapshots, updates, enablement, refresh, `Fetch`, and HTTP loaders |
| [Cache and lifecycle](docs/cache.en-US.md) | Cache operations, options, cleanup, cancellation, removal, closure, and errors |
| [Invalidation](docs/invalidation.en-US.md) | Single-key, batch, predicate invalidation, and refresh modes |

The guides explain every public API and include a complete runnable program. Treat cached values as immutable; copy slices and maps before modifying them. Different result types cannot reuse the same key.

## Verify

```sh
task lint
task test
task build
```

`task test` includes race detection and [HTTP e2e tests](e2e/query_lifecycle_test.go). The isolated local service exercises mutations, heterogeneous cache data, enablement, request sharing, retries, and cancellation without external services.
