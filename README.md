<div align="center">
  <img src="./assets/soroi.png" alt="soroi" width="144" />

  <h1>soroi</h1>

  <p><strong>English</strong> | <a href="./README.zh-CN.md">简体中文</a></p>

  <p><strong>Manage shared query data, state, and refresh lifecycles</strong></p>
  <p>A typed, dependency-free query client for Go 1.22 and later</p>
  <p>Share data across components, combine duplicate requests, and observe query state.</p>

  <p>
    <img src="https://img.shields.io/badge/Go-1.22%2B-00ADD8?style=flat-square&logo=go&logoColor=white" alt="Go 1.22+" />
  </p>

  <p><a href="#features">Features</a> · <a href="#install">Install</a></p>
</div>

## When to use soroi

Use soroi when your Go application needs to coordinate data loading and
synchronization: reuse cached results, share concurrent requests, batch reads,
or refresh related queries after a write.

It fits backend services, CLI/TUI applications, and background workers. You
provide the HTTP requests, database operations, or other loaders; soroi manages
how they execute and how consumers receive their results.

| Application scenario | How soroi helps |
| --- | --- |
| Several handlers or components read the same user, configuration, or resource | Reuse fresh cached data and share in-flight requests for the same key |
| A list or aggregation needs many objects from a backend with a bulk API | Collect different-key loads into batches, or submit known keys with `LoadMany` |
| A successful write must update a detail view and refresh related lists or totals | Coordinate cache updates and invalidation through Mutation; optionally wait for associated refreshes |
| Multiple callers need the same query key, loader, and loading policy | Create reusable definitions with `QueryDefinition` and shared `QueryOptions`; use tags and filters to manage related queries |
| A view or long-running task needs to react to loading, results, errors, and refreshes | Subscribe to typed query updates; inspect diagnostic events to trace execution |
| Reads and writes share a database or service with limited capacity | Bound active callbacks and queued work with a shared `Executor` |

soroi owns query state and local results within one process. Selected queries can
reuse serialized results across processes through an optional
[shared result cache](docs/shared-cache.en-US.md), including a separate Redis adapter.
For simple local values that only need expiration, a TTL cache may be sufficient.

## Features

- Type-safe shared queries and caching.
- Cache policies and lifecycle management.
- Optional shared result caching with Redis and per-key invalidation.
- Cache updates, mutations, and invalidation.
- Automatic batch loading.
- Shared execution budgets.
- Query observability.

## Install

```sh
go get github.com/Waterkyuu/soroi
```

Import `github.com/Waterkyuu/soroi` as package `soroi`. Requires Go 1.22 or later and has no external dependencies.

Users of `cacheq` should read the [v0.3.1 migration instructions](docs/versioning.en-US.md#migrating-from-cacheq-to-soroi) before upgrading; module paths and the Redis default prefix change.

## Quick start

Save this complete program as `main.go` in a Go module that imports soroi, then run `go run .`. It needs no server or credentials.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
)

// main gives two components the same query without loading the data twice.
func main() {
	client := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer client.Close()

	var calls atomic.Int32
	load := func(context.Context) (string, error) {
		calls.Add(1)
		return "hello", nil
	}
	queries := []*soroi.QueryHandle[string]{
		soroi.Query(client, "greeting", load),
		soroi.Query(client, "greeting", load),
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

## Redis examples

[Source and run instructions](examples/redis/) demonstrate shared results, cache failure fallback, remote tags, and cross-instance invalidation notifications. These examples require Go 1.26+ and a local Redis server.

See the [shared cache guide](docs/shared-cache.en-US.md) and [shared invalidation guide](docs/shared-invalidation.en-US.md) for setup, expected output, and API contracts.

## Reproducible benchmarks

Compare **no cache, first load, and cache hits** for **100 sequential reads** of the same data in the [benchmark guide](benchmarks/benchmarks.en-US.md). The main table reports total time and actual loader calls; internal cost and concurrency diagnostics are documented separately.

```sh
go test -run '^$' -bench '^BenchmarkCacheBenefit$' -benchmem -count=3 -cpu=1
```

The [concurrent request scenarios](benchmarks/benchmarks.en-US.md#concurrent-request-scenarios) compare cache behavior for shared and distinct keys with 100 reads, GOMAXPROCS=4, and four request goroutines performing 25 reads each.

## Feature guides

Read the [documentation website](https://waterkyuu.github.io/soroi/) in English or Chinese.

| Guide | Coverage |
| --- | --- |
| [Queries and conditions](docs/queries.en-US.md) | `Query`, snapshots, updates, enablement, refresh, `Fetch`, and HTTP loaders |
| [Query options](docs/query-options.en-US.md) | Shared loading options, reusable typed query definitions, freshness, retries, and timeouts |
| [Query filters](docs/query-filters.en-US.md) | Reusable key and predicate selection for invalidation, awaited refresh, cancellation, and removal |
| [Query tags](docs/query-tags.en-US.md) | Declare related query groups, select by label, and manage registration and retention |
| [Automatic batch loading](docs/batching.en-US.md) | Concurrent-key batching, LoadMany for known keys, query API integration, per-key results, and cancellation |
| [Cache and lifecycle](docs/cache.en-US.md) | Cache operations, options, cleanup, cancellation, removal, closure, and errors |
| [Shared result cache](docs/shared-cache.en-US.md) | Optional Redis caching, codecs, source freshness, per-key invalidation, conditional writes, and failure handling |
| [Shared invalidation and tags](docs/shared-invalidation.en-US.md) | Cross-instance notifications, remote tag generations, watcher lifecycle, connection recovery, and atomic Redis tag layouts |
| [Invalidation & refresh](docs/invalidation.en-US.md) | Single-key, batch, predicate invalidation, awaitable refresh, and bounded follow-up loads |
| [Atomic cache updates](docs/updates.en-US.md) | Conflict-safe typed transformations, immutable values, and explicit retry contracts |
| [Mutations & synchronization](docs/mutations.en-US.md) | Write state, optional refresh waiting, associated invalidation, and separate synchronization errors |
| [Execution budgets](docs/execution.en-US.md) | Shared concurrency limits, bounded FIFO queues, overload handling, and cancellation |
| [Observability](docs/observability.en-US.md) | Cache hits, shared requests, load outcomes and duration, retries, and cleanup statistics |
| [Diagnostic events](docs/events.en-US.md) | Per-key load causes and sources, shared operation IDs, joins, retries, discarded results, and bounded subscriptions |
| [MCP resource caching](docs/mcp.en-US.md) | Server-side reuse, remaining TTL, change notifications, private scopes, and e2e |

Complete integrations live in `examples`; the guides cover API details. Treat cached values as immutable; copy slices and maps before modifying them. Different result types cannot reuse the same key.

## Versioning and upgrades

soroi follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html). During `v0`, patch releases preserve compatibility; minor releases may introduce breaking changes with migration notes. From `v1`, breaking public API changes require a new major version. The core library currently supports Go 1.22 and later; example modules may require newer Go versions.

See the [version policy](docs/versioning.en-US.md) for the public API boundary, deprecation policy, Go support, and upgrade guidance. Published version tags are never moved or reused.

## Contributing

See the [contribution guide](CONTRIBUTING.md) for local setup, Go code guidelines,
tests, documentation, and the PR workflow. Bug reports and feature proposals are
welcome through GitHub Issues.

## License

soroi is licensed under the [MIT License](LICENSE).
