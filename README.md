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

## Quick start

```go
client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
defer client.Close()

query := cacheq.Query(client, "greeting", func(context.Context) (string, error) {
	return "hello", nil
})
defer query.Close()
```

`Query` loads in the background; queries with the same key share cached data and requests. Use `Snapshot()` for state, `Updates()` for notifications, or `Fetch` to wait for a result.

## Use cases

| Scenario | Pair with | Purpose |
| --- | --- | --- |
| Bubble Tea / TUI | `Query` + `Updates()` + `tea.Cmd` | Deliver loading, refresh, and error state to the message loop |
| CLI / background jobs | `Fetch` + `Options.Timeout` | Wait for results, reuse in-process data, and bound load time |
| HTTP handlers | A shared `Client` + `Fetch(r.Context(), ...)` | Merge concurrent same-key requests and respond to cancellation |

After a successful mutation, use `Invalidate` for related queries. HTTP cache keys must include result-affecting parameters and user or tenant scope. See the [query guide](docs/queries.en-US.md) and [invalidation guide](docs/invalidation.en-US.md) for API usage.

## Bubble Tea example

[Full source and instructions](examples/bubbletea/) demonstrate query updates in a TUI, background refresh, retained data on failure, and exit cleanup. The example requires Go 1.26+.

```sh
cd examples/bubbletea
go run .
```

## Feature guides

Read the [documentation website](https://waterkyuu.github.io/cacheq/) in English or Chinese.

| Guide | Coverage |
| --- | --- |
| [Queries and conditions](docs/queries.en-US.md) | `Query`, snapshots, updates, enablement, refresh, `Fetch`, and HTTP loaders |
| [Cache and lifecycle](docs/cache.en-US.md) | Cache operations, options, cleanup, cancellation, removal, closure, and errors |
| [Invalidation](docs/invalidation.en-US.md) | Single-key, batch, predicate invalidation, and refresh modes |
| [Observability](docs/observability.en-US.md) | Cache hits, shared requests, load outcomes and duration, retries, and cleanup statistics |

The full Bubble Tea program lives in `examples/bubbletea`; the guides cover API details. Treat cached values as immutable; copy slices and maps before modifying them. Different result types cannot reuse the same key.

## Verify

```sh
task lint
task test
task build
```

`task test` includes race detection and [HTTP e2e tests](e2e/query_lifecycle_test.go). The isolated local service exercises mutations, heterogeneous cache data, enablement, request sharing, retries, and cancellation without external services.
