<div align="center">
  <img src="./assets/cacheq.png" alt="cacheq" width="144" />

  <h1>cacheq</h1>

  <p><strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a></p>

  <p><strong>Manage shared query data, state, and refresh lifecycles</strong></p>
  <p>A typed, dependency-free query client for Go 1.22 and later</p>
  <p>Share data across components, combine duplicate requests, and observe query state.</p>

  <p>
    <img src="https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.22+" />
  </p>

  <p><a href="#features">Features</a> · <a href="#install">Install</a></p>
</div>

## When to use cacheq

Use cacheq when several parts of one Go application need the same remote or
expensive data and must react to loading, refreshes, or failures. One shared
client handles cache reuse, in-flight request sharing, and query notifications;
consumers keep their own typed handles.

| Your application needs | Choose |
| --- | --- |
| Shared data plus loading, error, refresh, and invalidation state | cacheq |
| A few local values with basic expiration and no query observers | A simple map with synchronization and expiration, or a TTL cache |
| An admission policy optimized for a high-volume cache and a memory budget | A cache designed for that workload; benchmark before choosing |
| Persistent or cross-process cache sharing | An external storage or cache system; cacheq stores data in one process |

## Features

- One shared client caches different result types: details, lists, and configuration.
- Each query retains static typing; incompatible types for a key return an error.
- One `Query` entry point supplies state, updates, enablement, and manual refresh.
- Freshness, background refresh, same-key request sharing, retries, and load timeouts.
- Per-consumer freshness, retry filtering, and timeout overrides through query and fetch options.
- Single-key, mixed-type batch, and predicate invalidation with optional deferred refresh.
- Local writes, prefetching, cancellation, and inactive cache cleanup.

## Install

```sh
go get github.com/Waterkyuu/cacheq
```

Import `github.com/Waterkyuu/cacheq` as package `cacheq`. Requires Go 1.22 or later and has no external dependencies.

## Quick start

Save this complete program as `main.go` in a Go module that imports cacheq, then run `go run .`. It needs no server or credentials.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main gives two components the same query without loading the data twice.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()

	var calls atomic.Int32
	load := func(context.Context) (string, error) {
		calls.Add(1)
		return "hello", nil
	}
	queries := []*cacheq.QueryHandle[string]{
		cacheq.Query(client, "greeting", load),
		cacheq.Query(client, "greeting", load),
	}
	for _, query := range queries {
		defer query.Close()
	}
	for i, query := range queries {
		for state := range query.Updates() {
			if state.Err != nil {
				panic(state.Err)
			}
			if state.HasData && !state.Fetching {
				fmt.Printf("component %d: %s\n", i+1, state.Data)
				break
			}
		}
	}
	fmt.Printf("loader calls: %d\n", calls.Load())
}
```

Expected output:

```text
component 1: hello
component 2: hello
loader calls: 1
```

`Query` loads in the background. These two components either join the same active request or reuse its fresh result. `Updates()` delivers the initial and latest states; slow consumers can skip intermediate transitions. Use `Snapshot()` for current state, or `Fetch` to wait for a result without subscribing.

## Use cases

| Scenario | Pair with | Purpose |
| --- | --- | --- |
| Bubble Tea / TUI | `Query` + `Updates()` + `tea.Cmd` | Deliver loading, refresh, and error state to the message loop |
| CLI / background jobs | `Fetch` + `Options.Timeout` | Wait for results, reuse in-process data, and bound load time |
| HTTP handlers | A shared `Client` + `Fetch(r.Context(), ...)` | Merge concurrent same-key requests and respond to cancellation |
| MCP resources | `FetchWithExpiry` + `Invalidate` + `Stats()` | Share backend reads across connections, preserve TTL, and notify after changes |

After a successful mutation, use `Invalidate` for related queries. HTTP cache keys must include result-affecting parameters and user or tenant scope. See the [query guide](docs/queries.en-US.md) and [invalidation guide](docs/invalidation.en-US.md) for API usage.

## Bubble Tea example

[Run the task board](examples/bubbletea/) to see list, detail, and summary views subscribe to one query. Toggle a task to refresh all three views, or inject a failure to see retained data and recovery. The example requires Go 1.26+.

See the [walkthrough](docs/bubbletea.en-US.md) for the subscription and mutation flow.

```sh
cd examples/bubbletea
go run .
```

## MCP example

[Full source and instructions](examples/mcp/) demonstrate resource caching, change notifications, and private scopes. The example requires Go 1.25+.

```sh
cd examples/mcp
go run .
```

## Reproducible benchmarks

Compare **no cache, first load, and cache hits** for **100 sequential reads** of the same data in the [benchmark guide](docs/benchmarks.en-US.md). The main table reports total time and actual loader calls; internal cost and concurrency diagnostics are documented separately.

```sh
go test -run '^$' -bench '^BenchmarkCacheBenefit$' -benchmem -count=3 -cpu=1
```

The [concurrent request scenarios](docs/benchmarks.en-US.md#concurrent-request-scenarios) compare 1, 4, 16, and 64 caller goroutines handling the same 64 requests, with GOMAXPROCS set to 1 and 4, against shared and distinct keys.

## Feature guides

Read the [documentation website](https://waterkyuu.github.io/cacheq/) in English or Chinese.

| Guide | Coverage |
| --- | --- |
| [Queries and conditions](docs/queries.en-US.md) | `Query`, snapshots, updates, enablement, refresh, `Fetch`, and HTTP loaders |
| [Query options](docs/query-options.en-US.md) | Per-consumer defaults, independent freshness, filtered retries, timeouts, and shared request ownership |
| [Cache and lifecycle](docs/cache.en-US.md) | Cache operations, options, cleanup, cancellation, removal, closure, and errors |
| [Invalidation](docs/invalidation.en-US.md) | Single-key, batch, predicate invalidation, and refresh modes |
| [Observability](docs/observability.en-US.md) | Cache hits, shared requests, load outcomes and duration, retries, and cleanup statistics |
| [MCP resource caching](docs/mcp.en-US.md) | Server-side reuse, remaining TTL, change notifications, private scopes, and e2e |

Complete integrations live in `examples`; the guides cover API details. Treat cached values as immutable; copy slices and maps before modifying them. Different result types cannot reuse the same key.

## Versioning and upgrades

cacheq follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). During `v0`, patch releases preserve compatibility; minor releases may introduce breaking changes with migration notes. From `v1`, breaking public API changes require a new major version. The core library currently supports Go 1.22 and later; example modules may require newer Go versions.

See the [version policy](docs/versioning.en-US.md) for the public API boundary, deprecation policy, Go support, and upgrade guidance. Published version tags are never moved or reused.

## Contributing

See the [contribution guide](CONTRIBUTING.md) for local setup, Go code guidelines,
tests, documentation, and the PR workflow. Bug reports and feature proposals are
welcome through GitHub Issues.

## License

cacheq is licensed under the [MIT License](LICENSE).
