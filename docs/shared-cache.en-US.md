# Shared result caching with Redis

[简体中文](shared-cache.zh-CN.md)

Use `store/redis` to share query results between service instances reading the same user profile, reducing repeated database or API calls. This guide explains API usage for creating a store, creating a shared cache, defining queries, fetching results, and invalidation, with a complete program at the end.

## Installation

You need Go 1.26+, this cacheq checkout, and an installed `redis-server`. The core library requires only Go 1.22+; `store/redis` is a separate module whose go-redis dependency requires Go 1.26+.

The following setup uses local source without assuming an installable adapter release. Run from the **cacheq repository root** to create an independent temporary example module:

```sh
CACHEQ_SOURCE_DIR="$(pwd)"
CACHEQ_DEMO_DIR="$(mktemp -d)"
cd "$CACHEQ_DEMO_DIR"
go mod init example.com/cacheq-redis-demo
go mod edit -replace="github.com/Waterkyuu/cacheq=$CACHEQ_SOURCE_DIR"
go mod edit -replace="github.com/Waterkyuu/cacheq/store/redis=$CACHEQ_SOURCE_DIR/store/redis"
go get github.com/Waterkyuu/cacheq/store/redis@v0.0.0
```

The two replacements point to the core and Redis adapter source. For an existing project, configure these paths in its `go.mod` and add the dependency without running `go mod init` again.

Open another terminal, start a disposable Redis server, and leave it running:

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

The program connects to `127.0.0.1:16379`. Use a writable primary connection that permits `EVAL` and the scripts' `GET`, `SET` (including `PX`), and `DEL` commands. Reads can initialize cache metadata too.

## Minimal two-client demo

The runnable [minimal example](../examples/redis/shared_cache_minimal/main.go) uses two Clients with independent local caches and one Redis store. `SharedCache` supplies storage rules, and `Definition.Shared` attaches those rules to the query. Constructing either object does not load data.

With the disposable Redis above running, execute from the **repository root**:

```sh
REDIS_ADDR=127.0.0.1:16379 go -C examples/redis run ./shared_cache_minimal
```

Expected output:

```text
source: loading user 7
A first: Alice, shared hits: 0
B first: Alice, shared hits: 1
A again: Alice, shared hits: 1
```

1. A's first `definition.Fetch(ctx, a)` finds no fresh local or shared value. cacheq calls the source callback, encodes Alice as JSON in Redis, and retains Alice in A's local cache.
2. B's first `definition.Fetch(ctx, b)` finds no local value, decodes the Redis record into a new `User`, and retains it in B's local cache. The source callback does not run.
3. A's next `definition.Fetch(ctx, a)` returns its fresh local value. The shared-hit counter stays at one because Redis is not accessed.

`Definition.Key` identifies an entry within each Client. `SharedCacheOptions.Key`, together with the adapter's Prefix, identifies the Redis record; this example uses `user:7` for both keys. A and B run in one process to make the sequence visible. Separate service processes can reuse the same record by configuring the same Redis namespace, shared key, compatible Format, and result type. They do not need to share a Go object.

The example clears only its reserved `cacheq-minimal-demo` shared key before reading, so repeated runs produce the same output. `StaleTime` controls when Clients consider data fresh, while the shared `TTL` caps the shared result's freshness and retention. Both are one minute here; the Redis adapter's invalidation metadata can outlive the result payload.

## Usage

The following sections follow API usage order: create a Redis store, create a shared cache, define a query, fetch results, and invalidate them. The example's result type is User, representing a user profile:

```go
// User contains the profile fields shared between service instances.
type User struct {
	// Name is the user's display name.
	Name string
}
```

Declare User at package level. Place the initialization and operation snippets below in the same main function, in order, reusing the objects created earlier. They are not standalone programs. The runnable program with imports and all setup appears under "Complete examples and related guides" at the end.

### Create a Redis store

Use `redistore.New(remote, options)` to create a Redis storage adapter. The application creates remote as a go-redis client; the returned store implements SharedStore for subsequent shared cache operations. Prefix gives records an application namespace, `cacheq-quickstart` here.

This code creates an example context with a ten-second lifetime and a Redis connection, then creates the adapter. Ping verifies connectivity for the example; redistore.New itself validates configuration without server I/O.

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

// The application owns the Redis connection and closes it at shutdown.
remote := redis.NewClient(&redis.Options{
	Addr: "127.0.0.1:16379", ContextTimeoutEnabled: true,
	DialTimeout: time.Second, ReadTimeout: time.Second,
	WriteTimeout: time.Second, MaxRetries: -1,
})
defer func() { _ = remote.Close() }()
if err := remote.Ping(ctx).Err(); err != nil {
	panic(err)
}
// Store adapts the borrowed connection to cacheq operations in this namespace.
store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq-quickstart"})
if err != nil {
	panic(err)
}
```

In an application, create the Redis connection and store at startup and reuse them for multiple queries. The application owns connection cleanup; do not reconnect for every read.

### Create a shared cache

Use `cacheq.NewSharedCache(store, options)` to configure shared caching for one query result. The function infers the saved and restored result type from `SharedCacheOptions[V]`; this example uses `SharedCacheOptions[User]` with the User defined above. The store argument is the Redis adapter created in the previous step.

This example shares the profile of user 7 in tenant 42 under `tenant:42:user:7:v1`, using default JSON encoding and the format label `user.json.v1`. It retains the result for at most five minutes from source completion and waits at most 200 milliseconds for each shared store operation.

```go
// Bind one User record's identity and caching rules to the Redis adapter.
// Construction validates configuration without reading or writing Redis.
shared, err := cacheq.NewSharedCache(store, cacheq.SharedCacheOptions[User]{
	// Key identifies this tenant's user profile across service instances.
	Key: "tenant:42:user:7:v1",
	// Format must match the schema and encoding expected by every reader.
	Format: "user.json.v1",
	// TTL caps shared freshness and retention from source completion.
	TTL: 5 * time.Minute,
	// Timeout bounds each store call, not the source Fetcher.
	Timeout: 200 * time.Millisecond,
})
if err != nil {
	panic(err)
}
```

The returned shared has type `*cacheq.SharedCache[User]` and holds storage and reuse rules for this data. Construction only validates and saves configuration; it does not load a profile. The next section attaches it to a query through Definition.Shared.

For user 8, create a configuration with a different key such as `tenant:42:user:8:v1`, reusing the same store. Colons and key segments are application naming conventions. Format labels the schema and encoding version readers must match; it does not choose a codec. JSON is used here because Codec is unset. Invalid key, Format, TTL, Timeout, or other options return errors; see "Configuration" for constraints.

### Define a query using the shared cache

Call `cacheq.QueryDefinition(key, fetcher)` to define the local query identity and source read. The loader returns `User`, so the function infers `User` and returns `(cacheq.Definition[User], error)`. Handle the construction error, then assign the shared object from the previous step to the returned definition's `Shared` field; it must have type `*cacheq.SharedCache[User]`.

Here source is a concurrency-safe database substitute and calls counts source reads. This code only defines the Fetcher callback; constructing a query definition does not execute it. In application code, replace it with a database or API read that uses its supplied context.

```go
// A concurrency-safe in-memory source stands in for a database or API.
var source atomic.Value
source.Store(User{Name: "Alice"})
var calls atomic.Int64
// Definition connects the shared rules to a local query identity and source.
definition, err := cacheq.QueryDefinition("tenant:42:user:7", func(context.Context) (User, error) {
	calls.Add(1)
	return source.Load().(User), nil
})
if err != nil {
	panic(err)
}
definition.Shared = shared

```

Definition.Key identifies the query within one Client; SharedCacheOptions.Key identifies the cross-instance record. Configure them separately; neither is derived from the other. Use consistent result types and loading rules for the same local key.

### Fetch query results

Use `definition.Fetch(ctx, client)` to wait for a User or an error. A Client owns local query state. Applications normally reuse a Client and supply each request's context. This example creates Clients A and B to simulate separate service instances.

StaleTime of one minute permits reusing a source result as fresh within that interval. A invokes the Fetcher and writes Redis when no usable record exists; B can then reuse that Redis record. Finally, shared.Stats().Hits reports shared cache hits.

```go
// Both Clients use the same shared configuration and keep separate local results.
a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
defer a.Close()
defer b.Close()

first, err := definition.Fetch(ctx, a)
if err != nil {
	panic(err)
}
fmt.Printf("A: %s, source calls: %d\n", first.Name, calls.Load())

second, err := definition.Fetch(ctx, b)
if err != nil {
	panic(err)
}
fmt.Printf("B: %s, source calls: %d\n", second.Name, calls.Load())
fmt.Printf("shared hits: %d\n", shared.Stats().Hits)
```

Reads check fresh local data, a fresh Redis record, then the Fetcher. B's shared hit does not invoke the Fetcher again. Subsequent fresh reads use each Client's local data. The complete example at the end resets its initial record for repeatable output.

At shutdown, close Clients before the Redis connection. Client.Close does not close the injected connection or delete Redis data. Processes sharing a record must agree on the Redis dataset, namespace, shared key, Format, Codec, and EnableTags setting.

### Invalidate a result after updating the source

Commit the database or API write before calling `definition.InvalidateShared`. It invalidates the Redis record first, then the query in the supplied Client. A subsequent read loads from the source.

Insert this snippet at the end of the complete example's main function at the end, after its last output line. It reuses `source`, `definition`, `ctx`, `a`, and `calls`, simulating a successful business write by changing the in-memory source:

```go
// Simulate a successful database or API write before invalidating the cache.
source.Store(User{Name: "Bob"})
if err := definition.InvalidateShared(ctx, a, cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
}); err != nil {
	panic(err)
}
updated, err := definition.Fetch(ctx, a)
if err != nil {
	panic(err)
}
fmt.Printf("A refreshed: %s, source calls: %d\n", updated.Name, calls.Load())
```

Additional output:

```text
A refreshed: Bob, source calls: 2
```

`RefetchNone` marks data stale without starting an automatic refresh; the next Fetch performs the read here. With omitted or empty options, enabled observed queries can refresh under the default policy, but InvalidateShared itself does not wait for refresh completion.

B can still return its local Alice until its freshness deadline or local invalidation. This example has no cross-instance notifications. To have other instances process invalidations promptly, configure EnableNotifications and WatchShared using [shared invalidation and tags](shared-invalidation.en-US.md). Enable remote tags when grouping several shared keys.

### Refresh only the current Client

To have only B reload its data, insert this code after the previous snippet. It reuses `b` and the existing query definition:

```go
if err := b.Invalidate(definition.Key, cacheq.InvalidateOptions{
	Refetch: cacheq.RefetchNone,
}); err != nil {
	panic(err)
}
refreshed, err := definition.Fetch(ctx, b)
if err != nil {
	panic(err)
}
fmt.Printf("B refreshed: %s, source calls: %d\n", refreshed.Name, calls.Load())
```

Additional output:

```text
B refreshed: Bob, source calls: 3
```

A newly started read after local invalidation bypasses the Redis payload and loads the source, increasing the call count. This operation does not directly notify other Clients.

Choose an invalidation API by its scope:

| API | Effect |
| --- | --- |
| `definition.InvalidateShared(ctx, client, options...)` | Invalidate the shared record, then the supplied Client's query; other watchers can receive the event asynchronously when notifications are enabled |
| `shared.Invalidate(ctx)` | Perform shared invalidation without directly operating on a Client; enabled watchers can still process it asynchronously |
| `client.Invalidate(key)` | Invalidate only this Client's query; a subsequent newly started load bypasses shared data |

## Configuration

### Query identity and serialization

| Field | Purpose | Required or default | Constraints |
| --- | --- | --- | --- |
| `Definition.Key` | Query identity within this Client | Required | Use a supported comparable value containing result-affecting tenant, permission, and parameter scope; use consistent result types and loading configurations for the same key |
| `Definition.Shared` | Connect the query to shared caching | Default nil: local caching only | The `V` in `SharedCache[V]` matches the query result type |
| `SharedCacheOptions.Key` | Cross-instance record identity | Required nonempty string | Not derived automatically from the local key; the example uses `tenant:42:user:7:v1` |
| `SharedCacheOptions.Format` | Result schema and encoding version | Required nonempty string | The example uses `user.json.v1`; incompatible upgrades also need a new shared key |
| `SharedCacheOptions.Codec` | Encode and decode results | Default `JSONCodec[V]` | JSON preserves only serializable fields; custom `Codec[V]` methods must support concurrent calls |

Key determines whether two callers refer to the same data; Format determines how to interpret it. Incompatible callers using one key replace each other's records and repeatedly load the source even when their formats differ. Format validation is not data migration.

Default JSON supports encodable zero values, empty values, and nil; a cached nil remains a valid result. Unexported fields, functions, channels, and pointer identity are not preserved. Shared keys should contain neither secrets nor identities based on pointer addresses or arbitrary `fmt.Sprint` output.

### Freshness and timeouts

| Field | Purpose | Required or default | Constraints |
| --- | --- | --- | --- |
| `Options.StaleTime` | How long a source result can be reused as fresh | Default 0: immediately stale | Configure a positive value on the Client or query consumer to reuse shared records; the example uses one minute |
| `SharedCacheOptions.TTL` | Maximum shared freshness and storage lifetime from source completion | Required positive duration | The example uses five minutes; hits do not renew it |
| `SharedCacheOptions.Timeout` | Timeout for each shared store call | 0 selects one second; negative is invalid | The example uses 200 milliseconds; it does not bound the Fetcher |

For example, A loads Alice from the source at 12:00:00. B hits Redis at 12:00:30 with a one-minute StaleTime, so B is fresh only until 12:01:00. The hit does not restart freshness. That record cannot be reused as fresh after the deadline even if its Redis TTL has time remaining. With a ten-minute StaleTime and a five-minute shared TTL, freshness still ends at 12:05:00.

GCTime and capacity eviction manage only local data, without deleting Redis records. MaxAge starts when data is installed locally and can remove it earlier; it does not guarantee age measured from the original source read. See [cache and lifecycle](cache.en-US.md).

Timeout bounds each store call separately, not the whole query. Request contexts and query timeout policies still control loading lifetime. Also configure go-redis context, connection, socket, and retry budgets. The example enables ContextTimeoutEnabled and disables driver retries with `MaxRetries: -1`.

### Redis namespace and optional capabilities

| Field | Purpose | Required or default | Constraints |
| --- | --- | --- | --- |
| `redistore.Options.Prefix` | Isolate Redis namespaces by application or environment | Empty selects `cacheq` | Braces are rejected; use your own production namespace |
| `redistore.Options.EnableNotifications` | Publish invalidations and allow watcher subscriptions | Default false | Requires additional Pub/Sub permissions; enable on every publisher |
| `redistore.Options.EnableTags` | Enable remote tag invalidation across keys | Default false | Uses a separate data layout; one namespace occupies one Cluster slot |
| `SharedCacheOptions.ShareTags` | Register Definition.Tags remotely | Default false | Requires enabled store tag support; existing tags on the same shared record are still honored |

See [shared invalidation and tags](shared-invalidation.en-US.md) for notification/tag examples, permissions, watcher cleanup, and Cluster tradeoffs.

## Error handling and common problems

Fetch still follows the source outcome: a Redis read failure falls back to the source, while encoding or shared write failures do not turn a successful source read into a query error. Source failures return query errors and follow configured retry and local stale-data policies. Caller cancellation or query timeouts can still terminate the query.

Explicit invalidation does not silently degrade: if the remote InvalidateShared operation fails, it returns an error and leaves local state unchanged. After a successful business write, handle cache synchronization failure separately instead of blindly retrying the business write. A transport timeout can mean that the remote operation already ran. Retrying invalidation is safe, but a committed remote invalidation is not rolled back if subsequent local processing fails.

| Symptom | Cause to check | Action |
| --- | --- | --- |
| `go run` cannot find the adapter or requires a newer Go version | Incorrect local replacement paths or an older toolchain | Check both source replacements and use Go 1.26+ |
| The example's Ping fails | Redis is stopped, or its address or permissions are wrong | Check the server at `127.0.0.1:16379`; this is a demonstration startup check, not a cacheq requirement |
| B loads the source again and ReadErrors increases | Redis connection, ACL, or operation timeout | Inspect `shared.Stats().LastError`, then check connection settings, writable-primary access, and script permissions |
| Hits stays at 0 without read errors | StaleTime is 0, the record expired, or reads always hit local data | Set positive freshness and use a new Client to verify shared hits; local hits do not increment shared Hits |
| DecodeErrors increases and the source runs | Format mismatch, damaged record, or codec decoding failure | Inspect LastError for the failing stage or actual record format; align types, Format, and Codec, using a new key for incompatible results |
| Source reads succeed but EncodeErrors or WriteErrors increases | The result cannot be encoded or the Redis write fails | Inspect LastError for the original codec or storage error; local results remain usable |
| B returns old data after A invalidates | B has fresh local data without a watcher | Invalidate B locally or configure WatchShared |
| Configuration construction fails | Empty key or Format, nonpositive TTL, or missing tag support | Check the configuration tables; invalid shared options return ErrInvalidSharedCache and missing capabilities return ErrSharedCapability |

Add this snippet at the end of the complete example's main function at the end to inspect the existing `shared` object's statistics:

```go
stats := shared.Stats()
fmt.Printf("hits=%d misses=%d read_errors=%d write_errors=%d\n",
	stats.Hits, stats.Misses, stats.ReadErrors, stats.WriteErrors)
fmt.Printf("last_error=%v\n", stats.LastError)
```

Additional output for the successful complete example:

```text
hits=1 misses=1 read_errors=0 write_errors=0
last_error=<nil>
```

`shared.Stats()` counts only operations using this SharedCache object, not Redis-wide or cross-process activity. Bypasses counts attempts that skip shared data after forced refresh or invalidation. WriteConflicts counts shared writes rejected after invalidation during a load. Client statistics continue to describe local caching and loading.

`SharedStats.LastError` retains the latest counted shared-cache failure. Its error message identifies the operation (`read`, `decode`, `encode`, `write`, or `invalidate`), logical shared Key, and expected Format. Codec and envelope failures also identify the failed stage. A format mismatch reports the actual stored format; invalid envelope timestamps and empty store versions receive explanatory errors. cacheq does not append the cached payload; underlying errors retain their own diagnostic details. Error message text is diagnostic output, not a stable parsing API.

Underlying store, codec, and JSON errors remain in the wrapping chain. Use `errors.Is(stats.LastError, context.DeadlineExceeded)` to identify a store deadline, or `errors.As` to inspect a driver or codec error type; import the standard `errors` package when using these functions. Applications must check for nil before calling LastError.Error. Custom errors must remain immutable while retained.

The diagnostic and its existing error counter are recorded together under synchronization. LastError is nil before the first counted failure. Successful reads, writes, local hits, and invalidations do not clear it; a later counted failure replaces it without changing previously returned snapshots. Only one error is retained: if decoding fails and the fallback result then fails encoding, both counters increase and LastError describes encoding. Concurrent failures are ordered by statistics recording, not by operation start time. This is diagnostic history, not a current health indicator or a per-request error log.

Missing or expired records, zero freshness, and conditional-write conflicts do not set LastError. Source failures remain query errors and do not replace the shared-cache diagnostic. A shared-store timeout that permits source fallback is retained; caller cancellation still terminates the query under the existing rules. Operations rejected before storage, and reads abandoned by query cancellation before error accounting, do not create a diagnostic. Explicit shared-key invalidation retains its failure and still returns it to the caller. Direct store operations, client tag invalidation, and watcher failures are outside this SharedCache object's counters; watcher failures remain available through SharedWatchStats.LastError.

## Behavior and limitations

- Reads try fresh local data, then a fresh Redis record, then the source. Newly started forced or invalidation refreshes skip Redis data but still obtain a version for the subsequent conditional write. If the same key is already loading, refresh joins that load without restarting or upgrading it. After a failed refresh, new loads continue to bypass shared data until a successful load or local write replaces the invalidated state.
- Shared reads preserve the original source completion time. Hits do not extend TTL; the end of shared freshness does not immediately remove local stale data. Keep clocks reasonably synchronized across machines; records dated after the reader's current time are treated as misses.
- If a record is invalidated after a load obtains its shared version but before it writes back, that load cannot write its old result back to Redis. A rejected write increments WriteConflicts. Successful source data can still be returned, but the local result remains stale. This protection neither orders database transactions nor orders loaders finishing within the same version.
- Same-key request merging occurs only within one Client, without a distributed lock. Several instances can load the source simultaneously on a shared miss. Shared caching does not persist query errors, subscriptions, or durable query state.
- Set and Update change local data without writing to Redis. Get and Snapshot do not read Redis. Remove, Clear, GC, and Client.Close do not delete shared records or publish invalidations.
- The default Redis layout keeps one expiring payload key and one version metadata key without a TTL per shared identity. Metadata still occupies space after data expires; plan capacity by query identity count. After metadata loss, the next read creates a new version and discards orphaned data.
- The adapter uses atomic Lua scripts and places each record's physical keys in one Cluster slot in the default layout. Tests use standalone Redis, without live Cluster or Sentinel failover coverage. Snapshot rollback or failover losing acknowledged writes has no stronger durability guarantee than Redis itself.
- Store and Codec calls run outside the Client lock; an Executor slot covers the entire load. Shared cache failures alone do not trigger query retries; source failures follow the query retry policy. Cancellation is cooperative, so both the driver and Fetcher must honor context.

## Complete examples and related guides

### Run the complete example

Save the following complete program as main.go in the temporary module created during installation (`$CACHEQ_DEMO_DIR`). It combines the creation and fetching operations above, resetting only its demonstration record for repeatable output.

```go
package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
	redistore "github.com/Waterkyuu/cacheq/store/redis"
	"github.com/redis/go-redis/v9"
)

// User contains the profile fields shared between service instances.
type User struct {
	// Name is the user's display name.
	Name string
}

// main demonstrates a source read followed by reuse in a separate local Client.
func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The application owns the Redis connection and closes it at shutdown.
	remote := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:16379", ContextTimeoutEnabled: true,
		DialTimeout: time.Second, ReadTimeout: time.Second,
		WriteTimeout: time.Second, MaxRetries: -1,
	})
	defer func() { _ = remote.Close() }()
	if err := remote.Ping(ctx).Err(); err != nil {
		panic(err)
	}
	// Store adapts the borrowed connection to cacheq operations in this namespace.
	store, err := redistore.New(remote, redistore.Options{Prefix: "cacheq-quickstart"})
	if err != nil {
		panic(err)
	}

	// Bind one User record's identity and caching rules to the Redis adapter.
	// Construction validates configuration without reading or writing Redis.
	shared, err := cacheq.NewSharedCache(store, cacheq.SharedCacheOptions[User]{
		// Key identifies this tenant's user profile across service instances.
		Key: "tenant:42:user:7:v1",
		// Format must match the schema and encoding expected by every reader.
		Format: "user.json.v1",
		// TTL caps shared freshness and retention from source completion.
		TTL: 5 * time.Minute,
		// Timeout bounds each store call, not the source Fetcher.
		Timeout: 200 * time.Millisecond,
	})
	if err != nil {
		panic(err)
	}
	// Reset only this demonstration record so repeated runs have the same output.
	if err := shared.Invalidate(ctx); err != nil {
		panic(err)
	}

	// A concurrency-safe in-memory source stands in for a database or API.
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	// Definition connects the shared rules to a local query identity and source.
	definition, err := cacheq.QueryDefinition("tenant:42:user:7", func(context.Context) (User, error) {
		calls.Add(1)
		return source.Load().(User), nil
	})
	if err != nil {
		panic(err)
	}
	definition.Shared = shared

	// Both Clients use the same shared configuration and keep separate local results.
	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()

	first, err := definition.Fetch(ctx, a)
	if err != nil {
		panic(err)
	}
	fmt.Printf("A: %s, source calls: %d\n", first.Name, calls.Load())

	second, err := definition.Fetch(ctx, b)
	if err != nil {
		panic(err)
	}
	fmt.Printf("B: %s, source calls: %d\n", second.Name, calls.Load())
	fmt.Printf("shared hits: %d\n", shared.Stats().Hits)
}
```

Run from the directory containing main.go:

```sh
go run .
```

Expected output:

```text
A: Alice, source calls: 1
B: Alice, source calls: 1
shared hits: 1
```

After A loads the source, B reuses the Redis result, so the source call count stays at 1. The program resets only the demonstration record in the cacheq-quickstart namespace. Do not perform this reset at application startup or keep production data in this namespace. Stop the disposable server with Ctrl+C in its terminal when finished.

### Related guides

- [Complete Redis examples and run instructions](../examples/redis/): run shared_cache for shared reads, explicit invalidation, and Redis failure fallback; run shared_invalidation for tags and cross-instance notifications.
- [Shared invalidation and tags](shared-invalidation.en-US.md): promptly mark other instances' local data stale and invalidate related detail/list queries together.
- [Cache and lifecycle](cache.en-US.md): configure local freshness, retention, and resource cleanup.
- [Invalidation and refresh](invalidation.en-US.md): choose automatic refresh policies and wait for local refresh completion.
- [SharedStore contract](../shared_store.go): atomic operations, version checks, context, and data ownership when implementing custom storage.
