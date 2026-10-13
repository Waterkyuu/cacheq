# Shared result caching with Redis

[简体中文](shared-cache.zh-CN.md) · [Cache and lifecycle](cache.en-US.md) · [Invalidation](invalidation.en-US.md)

## Overview

A cacheq `Client` owns typed query state inside one process. If two service instances each create a Client, their local caches are independent: a cache hit in instance A does not populate instance B. Both instances can therefore read the same resource from a database or HTTP service even while A already has a fresh result.

An optional shared cache adds another place to reuse successful results. A checks its local cache, then Redis, then the authoritative source. B follows the same path and can reuse the result A stored in Redis. This reduces repeated source reads across instances and can help newly started instances reuse existing results.

The source remains authoritative. Redis contains disposable serialized results. Cacheq still owns local subscriptions, loading state, type checks, request merging, retries, and local retention. Redis does not store goroutines, callbacks, subscription channels, Go type identities, or errors.

Shared caching is useful when multiple instances repeatedly read the same resources and those resources tolerate a defined freshness window. Applications using only one process, or results requiring every read to reach the source, may not benefit. Redis introduces network and serialization cost; compare it with the actual source cost before enabling it broadly.

## Read and refresh flow

```text
Definition.Fetch / Definition.Prefetch / Definition.Query
  |
  +-- local result is fresh --> return local result
  |
  +-- start or join one local same-key load
        |
        +-- normal read: obtain shared version and record
        |     +-- fresh, compatible record --> decode --> publish locally
        |     +-- miss, expired record, or cache error --> source
        |
        +-- forced or invalidation refresh: obtain version, ignore shared data --> source
              |
              +-- source error --> normal cacheq error/stale-data behavior
              +-- source success --> encode --> conditional shared write --> publish locally
```

Storage and codec calls run outside the Client lock. The existing Executor slot covers the whole attempt, including shared-cache access; store timeouts are separate from the source timeout. Retries retain the initiating consumer's normal policy. Cache read/write failures alone do not trigger query retries. Source failures can trigger retries.

A forced refresh still joins an existing same-key load, as before. It does not upgrade or restart a load already reading Redis. A newly started forced refresh bypasses shared data. Local invalidation of an active load leaves its eventual result stale and follows existing `RefetchAfterLoad` rules. After a failed refresh, newly started attempts continue to bypass shared data until a successful load or local write replaces the invalidated state.

## Requirements and quick start

The core module requires Go 1.22 or later. The optional Redis module requires Go 1.26 or later because it uses go-redis v9.23.0. Redis must support `EVAL`, `GET`, `SET` with `PX`, and `DEL`; the adapter uses a writable primary connection.

These instructions use the checked-out implementation. The adapter is a separate module with a local replacement for the root module; a published adapter version is not assumed.

In one terminal, start a disposable local Redis server on a free port:

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

In another terminal, from the repository root:

```sh
cd store/redis
REDIS_ADDR=127.0.0.1:16379 go run ./example
```

Expected output:

```text
A first: Alice, source calls: 1
B shared: Alice, source calls: 1
A local: Alice, source calls: 1
A refreshed: Bob, source calls: 2
B local: Alice, source calls: 2
B refreshed: Bob, source calls: 3
shared hits: 1, writes: 3, bypasses: 2
C fallback: Bob, source calls: 4
shared read errors: 1
```

The example uses an atomic in-memory source instead of a database, making source changes and call counts explicit. A and B have separate local Clients but share one Redis record. The `B local` result reflects the default local consistency contract: this example does not enable WatchShared, so B receives no notification. B's explicit local invalidation then bypasses Redis and reaches the source. Finally, the example closes its Redis connection: a new Client C still returns the source result and records a shared read error.

The reserved `cacheq-demo` namespace is reset before each run. Do not use it for production records. Its small generation key remains after data expires; stop the disposable server with Ctrl+C after the demonstration.

The complete program below is maintained in [store/redis/example/main.go](../store/redis/example/main.go). It includes connection settings, a typed source, local Clients, explicit invalidation, and cleanup.

```go
// Command example demonstrates shared reads, explicit invalidation, and cache failure fallback.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
	redistore "github.com/Waterkyuu/cacheq/store/redis"
	"github.com/redis/go-redis/v9"
)

// User represents the immutable result shared by the example's query clients.
type User struct {
	// Name is the display name returned by the simulated authoritative source.
	Name string
}

// main runs the example and reports setup or query failures to the shell.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run uses two clients and a simulated source to make each cache layer's effects observable.
func run() error {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "127.0.0.1:6379"
	}
	remote := redis.NewClient(&redis.Options{
		Addr: addr, ContextTimeoutEnabled: true, DialTimeout: time.Second,
		ReadTimeout: time.Second, WriteTimeout: time.Second, MaxRetries: -1,
	})
	defer func() { _ = remote.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := remote.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("connect to example Redis: %w", err)
	}
	store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq-demo"})
	if err != nil {
		return err
	}
	shared, err := cacheq.NewSharedCache[User](store, cacheq.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute,
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		return err
	}
	// Reset this reserved demonstration key so repeated runs have identical output.
	if err := shared.Invalidate(ctx); err != nil {
		return err
	}
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	definition := cacheq.QueryDefinition[User]{
		Key: "user:7", Shared: shared,
		Fetcher: func(context.Context) (User, error) {
			calls.Add(1)
			return source.Load().(User), nil
		},
	}
	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	if err := printRead(ctx, "A first", a, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "B shared", b, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "A local", a, definition, &calls); err != nil {
		return err
	}
	// A business write must succeed before invalidating its cached results.
	source.Store(User{Name: "Bob"})
	if err := definition.InvalidateShared(ctx, a, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		return err
	}
	if err := printRead(ctx, "A refreshed", a, definition, &calls); err != nil {
		return err
	}
	if err := printRead(ctx, "B local", b, definition, &calls); err != nil {
		return err
	}
	if err := b.Invalidate(definition.Key, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone}); err != nil {
		return err
	}
	if err := printRead(ctx, "B refreshed", b, definition, &calls); err != nil {
		return err
	}
	stats := shared.Stats()
	fmt.Printf("shared hits: %d, writes: %d, bypasses: %d\n", stats.Hits, stats.Writes, stats.Bypasses)
	// Closing only the borrowed Redis connection simulates an unavailable cache without stopping the source.
	if err := remote.Close(); err != nil {
		return err
	}
	c := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer c.Close()
	if err := printRead(ctx, "C fallback", c, definition, &calls); err != nil {
		return err
	}
	fmt.Printf("shared read errors: %d\n", shared.Stats().ReadErrors)
	return nil
}

// printRead reports the typed result and actual source calls for one explicit query read.
func printRead(
	ctx context.Context,
	label string,
	client *cacheq.Client,
	definition cacheq.QueryDefinition[User],
	calls *atomic.Int64,
) error {
	value, err := definition.Fetch(ctx, client)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %s, source calls: %d\n", label, value.Name, calls.Load())
	return nil
}
```

Subsequent snippets use the program's query definition, Client `a`, and a non-nil `ctx`, inside an error-returning business function.

## Configuration, keys, and serialization

| Configuration | Responsibility | Default or validation |
| --- | --- | --- |
| `QueryDefinition.Key` | Identity and static type binding inside one Client | Existing comparable-key rules |
| `QueryDefinition.Shared` | Optional typed shared-cache configuration | nil: local-only behavior |
| `SharedCacheOptions.Key` | Stable cross-process result identity | Required nonempty string |
| `SharedCacheOptions.Format` | Schema and codec version of the value | Required nonempty string |
| `SharedCacheOptions.TTL` | Maximum shared freshness and record retention from source completion | Required positive duration |
| `SharedCacheOptions.Timeout` | Deadline for each store call | Zero: one second; negative: invalid |
| `SharedCacheOptions.ShareTags` | Register QueryDefinition.Tags remotely | false; requires an enabled SharedTagStore |
| `SharedCacheOptions.Codec` | Typed value encoding and decoding | nil: `JSONCodec[V]` |
| `redistore.Options.Prefix` | Redis namespace separating applications/environments | Empty: `cacheq`; braces rejected |

Local keys can be comparable structs or other supported Go values. They are not automatically converted into remote keys. Supply the shared key explicitly, including tenant, resource, parameters, authorization scope, and schema version when they affect results. Avoid pointer addresses, `fmt.Sprint` of arbitrary values, and secrets in key identities.

All clients sharing a remote key must agree on result semantics, Format, and codec. Use a different key when changing incompatible schemas or representations. Format checking prevents accidental decoding; it is not a schema migration system. Incompatible callers using one key will overwrite one another's records after falling back to the source.

The outer envelope stores Format, source completion time, absolute shared expiry, and encoded bytes. The default value codec uses standard JSON. Exported fields, nil pointers, empty values, and zero values follow `encoding/json` rules; cached nil is distinct from a miss because the envelope still exists. Functions, channels, unsupported cycles, pointer identity, and unexported fields are not preserved. Implement `Codec[V]` for another representation, with concurrency-safe methods.

The result type remains bound locally by cacheq. No reflection-based cross-process type registry is introduced. Definitions for the same local key must use consistent loaders and shared configurations; joining a load preserves its initiator's configuration.

## Freshness, retention, and time

Shared caching preserves two different timestamps:

- **FetchedAt** is when the successful source callback completed. It travels with the shared record and determines consumer freshness.
- **UpdatedAt** is when this Client installed usable data. It remains the local Snapshot timestamp and the start of local MaxAge.

For a successful shared query, effective freshness ends at the earliest of:

```text
FetchedAt + this consumer's StaleTime
shared record's absolute ExpiresAt
local UpdatedAt + MaxAge, when MaxAge is enabled
```

If A loads at 12:00:00 and B restores at 12:00:30 with StaleTime of one minute, B is fresh only until 12:01:00. Restoring does not grant another minute. With shared TTL of five minutes, a consumer's ten-minute StaleTime is capped by 12:05:00. Zero or negative StaleTime still makes data immediately stale and prevents ordinary shared reuse.

Redis payload TTL uses the remaining absolute lifetime, including time spent encoding. Sub-millisecond Redis TTLs round up, but the envelope deadline is checked before reuse. No hit renews Redis TTL. Different machines need reasonably synchronized clocks; a record dated after the reader's clock is treated as a miss.

Shared expiry ends freshness, not all local data availability. Local stale data can remain visible during refresh until MaxAge or other local removal policies release it. GCTime and LRU eviction affect only the local Client and do not delete Redis data. MaxAge retains its existing local-installation meaning; it is not an end-to-end source-age guarantee.

## Business writes and explicit invalidation

Commit the database/API write first, then invalidate cached results:

```go
// The business write has already committed successfully.
if err := definition.InvalidateShared(ctx, a, cacheq.InvalidateOptions{
    Refetch: cacheq.RefetchNone,
}); err != nil {
    return err
}
_, err := definition.Fetch(ctx, a)
return err
```

`InvalidateShared` validates the local key, type, Client state, and context, invalidates the remote record, then calls local Invalidate. Remote failure leaves local state unchanged. Remote success is not rolled back if the Client closes before local invalidation. A timeout or transport failure can have an uncertain remote outcome: retrying remote invalidation safely creates another generation, but business writes must not be blindly retried.

Without RefetchNone, enabled observers refresh under existing local rules. The method does not await their completion; use the existing local refresh APIs when completion must be observed. Other local Clients are notified only through WatchShared. Remote tags use InvalidateSharedTags; see [shared invalidation and tags](shared-invalidation.en-US.md).

`shared.Invalidate(ctx)` performs only the remote operation. `client.Invalidate(key)` performs only local invalidation, although a newly started refresh bypasses shared data and can write a new shared result.

`Set` and `Update` remain local writes, including after a shared load. They reset local freshness normally and do not automatically publish serialized data. `Get`, `Snapshot`, `Remove`, `Clear`, GC, capacity eviction, and Client.Close perform no remote deletion. This keeps network failures and connection lifetime explicit at the application boundary.

## Concurrent invalidation contract

Consider this controlled ordering:

```text
A reads remote generation G1 and starts fetching old source data
B commits a business write
B invalidates: replace G1 with G2 and delete payload atomically
A finishes its old fetch and attempts to store it using G1
Redis compares G1 with G2 and rejects the write
```

The rejected write increments WriteConflicts. A's source call can still return successfully: a cache conflict is not a source failure. If the same Client was locally invalidated while loading, its installed result remains stale under existing cacheq rules.

This protection applies to loads that captured their token before a successful remote invalidation. It does not order business transactions, compare the true age of source replicas, or prevent two loaders in the same generation from finishing out of order. Request merging remains scoped to one Client; there is no distributed lock or cross-instance singleflight.

## Redis storage and operational boundaries

Each logical shared key maps to two physical Redis keys:

```text
<prefix>:{<SHA-256 of logical key>}:version
<prefix>:{<SHA-256 of logical key>}:data
```

The identical hash tag keeps both keys in the same Redis Cluster slot. Atomic Lua scripts implement reads, conditional writes, and invalidations. A read can initialize metadata, so a read-only replica or GET-only credential is insufficient. Redis Cluster is supported by key placement; integration tests here use a standalone local server, not a live cluster or Sentinel failover.

Generation tokens are random and generation keys have no TTL. Budget one small persistent metadata key per touched query identity, even after its data expires. Unbounded identity cardinality therefore requires an application capacity/namespace lifecycle policy. If metadata disappears, the next read creates a new token and deletes orphaned data; a write with absent or obsolete metadata is rejected. Restoring older Redis snapshots or losing acknowledged writes during failover can roll back generations and lies outside this protocol's durability guarantee.

Supply a concurrency-safe go-redis client and configure `ContextTimeoutEnabled`, dial, read, write, and retry settings for the application's budget. Context cancellation is cooperative; a driver or callback ignoring it cannot be forcibly terminated by cacheq. Borrowed connections are closed by the application, never by Client.Close.

## Failure handling and observability

| Condition | Query behavior | Shared counter |
| --- | --- | --- |
| Fresh local result | Return local data, no network call | None |
| Fresh shared record | Decode and install locally | Hits |
| Missing, expired, or consumer-stale record | Load source | Misses |
| Invalid envelope, Format mismatch, or codec decode failure | Load source | Misses and DecodeErrors |
| Read failure, read timeout, or empty version | Load source; skip shared write without a valid token | ReadErrors |
| Forced/invalidation refresh | Ignore record data, obtain token, load source | Bypasses |
| Encode or store write failure | Return successful source data | EncodeErrors or WriteErrors |
| Token changed during source load | Return source data, reject remote write, keep the local result stale | WriteConflicts |
| Source error | Normal query failure/retry and local stale-data behavior; no remote error record | Normal Client stats |
| Caller cancellation or query timeout | Cancel query according to existing rules | Operations completed before cancellation may already be counted |
| Explicit remote invalidation failure | Return wrapped error preserving errors.Is | InvalidationErrors |

`shared.Stats()` belongs to that SharedCache object and accumulates across its participating Clients. It is not a server-wide metric and separate processes do not automatically combine counters. Existing Client Stats still describes local cache decisions and loads; a shared hit occurs inside a local load. Export these snapshots into application monitoring as needed.

## Implementing a storage adapter

A new adapter implements Read, Write, and Invalidate with the SharedStore contract. A miss must still return a nonempty generation. Conditional comparison and installation must be atomic with respect to invalidation. Returning unconditional success from Write defeats old-write protection. Every returned Data slice must be owned by the recipient, and storage callbacks must honor context and be concurrency-safe.

Core code and custom adapters using only its contracts can remain on Go 1.22. The Redis adapter's dependency requirement is isolated in its own go.mod. Repository checks do not automatically traverse nested modules, so CI separately checks adapter module metadata, lint, race tests, build, and dependencies.

## Tests and limitations

From the repository root:

```sh
task fmt
task lint
task test
task build
cd store/redis
go vet ./...
golangci-lint run --config ../../.golangci.yml --timeout=5m
CACHEQ_REQUIRE_REDIS=1 go test -race ./... -count=1
go build ./...
```

Root unit tests cover client reuse, original freshness, TTL boundaries, nil data, codec failures, source failures, local writes, cancellation, Close, merge ordering, and old-write rejection. HTTP tests verify prefetch, subscription publication, source call counts, and refresh. Adapter tests launch disposable Redis processes with persistence disabled, exercise actual Lua scripts, inspect payload expiry, and simulate metadata loss. They skip explicitly when redis-server is unavailable unless CACHEQ_REQUIRE_REDIS=1; CI requires it.

The shared cache supports one shared result layer, Redis, JSON, and per-key invalidation fencing. Optional notifications and remote tag generations are specified in [shared invalidation and tags](shared-invalidation.en-US.md). Arbitrary cache chains, cached errors, distributed locks, Redis transactions for Update, and durable query state are not supported.
