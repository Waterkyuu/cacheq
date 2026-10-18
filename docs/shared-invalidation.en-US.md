# Shared invalidation notifications and tags

[简体中文](shared-invalidation.zh-CN.md) · [Shared result cache](shared-cache.en-US.md) · [Query tags](query-tags.en-US.md)

## Overview

`Client.WatchShared` receives remote invalidations and applies them to local queries bound to the same store instance. Enabled observed queries refresh according to the watch policy; disabled queries retain their data and publish stale snapshots. Notifications are optional: a Client without a watcher continues to rely on its normal freshness deadlines.

Set `redistore.Options.EnableNotifications: true` to publish changes and allow watchers. Its default false preserves the original storage-only command and ACL contract. All writers must enable it for immediate notification propagation.

Remote tags group different shared keys and result types. Enable `redistore.Options.EnableTags` on the store and `SharedCacheOptions.ShareTags` on each participating query. `Definition.Tags` then supplies the labels registered remotely. `Client.InvalidateSharedTags` advances the remote tag generations and invalidates matching queries in the initiating Client. Watchers propagate the operation to other Clients in the same remote namespace.

The authoritative database or API write must commit before invalidation. Notification delivery does not provide a distributed transaction, a synchronous barrier across instances, or durable event replay.

## Installation

The core contracts remain compatible with Go 1.22. The optional Redis module requires Go 1.26 and the dependency version declared in its go.mod. In addition to the shared cache's commands, notifications require `PUBLISH` and `SUBSCRIBE`; tags require `HGET`, `HSET`, `HGETALL`, and `HEXISTS`. Use a writable primary connection with appropriate ACL permissions and context/socket timeouts.

From the checked-out repository, start a disposable server in one terminal:

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

In another terminal, prepare the existing example module from the repository root. Its checked-in `go.mod` uses local replacements for the core and Redis adapter; no published adapter version is required:

```sh
cd examples/redis
go mod download
```

## Usage

The snippets below extend an application's `run() error` function and are not standalone programs. Reuse `ctx`, the service lifetime context, and `remote`, the configured `*redis.Client`, from [Create a Redis store](shared-cache.en-US.md#create-a-redis-store). The application owns that connection and its timeouts. Use the imports `context`, `fmt`, `time`, `github.com/Waterkyuu/soroi`, and `redistore "github.com/Waterkyuu/soroi/store/redis"`. The complete program below includes all setup and imports.

The existing query returns this profile type, declared at package level:

```go
// User represents the immutable source value shared by detail and list queries.
type User struct {
	// Name is the display name returned by the authoritative source.
	Name string
}
```

### Enable notifications on the store

Replace the application's existing `redistore.New` call with the configuration below. `EnableNotifications` defaults to false; true publishes invalidations and permits subscriptions. Keep the existing application's Prefix in production; `soroi-notification-usage` is an example namespace. Every writer requiring notification propagation must enable publishing. Redis ACLs need `PUBLISH` and `SUBSCRIBE` in addition to the [shared cache commands](shared-cache.en-US.md#installation).

```go
store, err := redistore.New(remote, redistore.Options{
	Prefix: "soroi-notification-usage", EnableNotifications: true,
})
if err != nil {
	return err
}
```

The returned `store` borrows `remote`; construction validates configuration without Redis I/O. Reuse this exact store object for the query bindings and watcher within each Client. Separate processes can construct their own objects with the same remote namespace. Enabling notifications does not require remote tags.

### Bind an existing query to the notification store

Reuse `definition`, the application's `soroi.Definition[User]` created by `QueryDefinition` as in [Define a query](shared-cache.en-US.md#define-a-query-using-the-shared-cache). Its Fetcher reads the committed user profile from the application's source. `User` is the profile result type declared above.

`NewSharedCache` infers `User` from `SharedCacheOptions[User]`. The key includes tenant, record identity, and schema version; `Format` selects the JSON schema and `TTL` caps shared freshness at five minutes. Keep the application's existing key, format, and TTL when updating a configuration. Attach the returned shared cache to the definition before using it with a Client.

```go
shared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[User]{
	Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute,
})
if err != nil {
	return err
}
definition.Shared = shared
```

Neither construction nor assignment performs Redis I/O. A later load binds the query's local key to this store and shared key. No tags are needed for key invalidation.

### Start one watcher for the Client and store

Reuse `client`, the application's long-lived `*soroi.Client` (`a` in the complete example). Call `WatchShared` once at startup for this Client/store pair. It performs subscription I/O, waits for admission, and returns a `*SharedWatcher`; setup failures return an error without retaining a reader. Existing queries bound to this store become stale to repair the pre-subscription gap.

```go
watch, err := client.WatchShared(ctx, store, soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
})
if err != nil {
	return err
}
defer watch.Close()
```

The watcher uses the service lifetime `ctx`, not a short-lived request context. Its lifetime also ends when the Client closes. This example selects `RefetchNone`, so notifications retain data and mark it stale without loading automatically. Omitting the options uses `RefetchObserved`, allowing enabled observers to refresh. Another process starts its own watcher on its own Client and store.

### Load the bound query

Load the existing definition through the same Client. `Fetch` returns a `User` or an error; a fresh local hit performs no Redis I/O, while a load can read Redis and invoke the source Fetcher. The returned user is usable immediately; watcher delivery to peers remains asynchronous.

```go
user, err := definition.Fetch(ctx, client)
if err != nil {
	return err
}
fmt.Printf("loaded user: %s\n", user.Name)
```

### Invalidate one shared key after a committed write

After the authoritative database or API write succeeds, call `definition.InvalidateShared` with the same Client. It performs remote key invalidation first, publishes the notification, then marks that definition's exact local key stale. `RefetchNone` leaves the next read responsible for loading; it does not wait for peer watchers or refresh completion.

```go
// The database or API write has already committed successfully.
if err := definition.InvalidateShared(ctx, client, soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
}); err != nil {
	return fmt.Errorf("invalidate committed user change: %w", err)
}
```

Remote failure returns an error and leaves local state unchanged; a timeout can leave the remote outcome uncertain. Handle invalidation failure separately from the already committed write. Retrying the business write solely because invalidation failed can duplicate its effects. `shared.Invalidate(ctx)` is the remote-only alternative; it does not synchronously apply the initiating Client's local invalidation.

### Enable optional remote tags

Skip this section when key invalidation is sufficient. To group detail and list records, create a store with `EnableTags: true` and start its watcher. The separate example prefix keeps this optional flow independent from the key-only flow above. Tag mode uses a separate Redis layout and notification channel; all participants in a tagged namespace must agree on Prefix and EnableTags. Tags additionally require `HGET`, `HSET`, `HGETALL`, and `HEXISTS` permissions.

```go
tagStore, err := redistore.New(remote, redistore.Options{
	Prefix: "soroi-tag-usage", EnableNotifications: true, EnableTags: true,
})
if err != nil {
	return err
}
tagWatch, err := client.WatchShared(ctx, tagStore, soroi.InvalidateOptions{
	Refetch: soroi.RefetchNone,
})
if err != nil {
	return err
}
defer tagWatch.Close()
```

Store construction performs no I/O; `WatchShared` subscribes using the same admission and lifetime rules as above. Reuse `tagStore` for every tagged shared cache and `tagWatch` for this Client/store pair. Enabling tags does not migrate records from the key-only layout.

### Register a definition's remote tags

Enable `ShareTags` in `SharedCacheOptions[User]` and assign `Definition.Tags` before loading. `ShareTags` requests remote registration; without it, newly declared labels remain local. Construction returns `ErrSharedCapability` if the store cannot register tags.

The following code copies the existing definition, preserving its source Fetcher, and uses a new local key so no fresh local result skips the initial shared read. The label includes the tenant and groups this detail record with any list record declaring the same label. `NewSharedCache` validates without I/O; `tagged.Fetch` performs the shared read that registers membership, including on a Redis cache hit.

```go
tagShared, err := soroi.NewSharedCache(tagStore, soroi.SharedCacheOptions[User]{
	Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute,
	ShareTags: true,
})
if err != nil {
	return err
}
tagged := definition
tagged.Key = "tagged-detail"
tagged.Shared = tagShared
tagged.Tags = []string{"tenant:42:users"}
if _, err := tagged.Fetch(ctx, client); err != nil {
	return err
}
```

Constructing a definition, creating a disabled handle, or hitting fresh local data does not register remote membership. When adding labels to an already loaded local key, create a handle from the updated definition and explicitly `Refetch(ctx)` to force registration. Remote membership is additive; changing groups may require a new versioned shared key or namespace.

### Invalidate the registered tags after a committed write

`client.InvalidateSharedTags` takes the same `tagStore` and the labels to invalidate. It performs remote I/O and then invalidates this Client's matching bound queries. Matching is exact, case-sensitive OR: any listed label is sufficient, and an empty list is a no-op. Other Clients receive notifications asynchronously through their own watchers.

```go
// The business write affecting the tagged records has already committed.
operation, cancel := context.WithTimeout(ctx, 2*time.Second)
defer cancel()
if err := client.InvalidateSharedTags(
	operation,
	tagStore,
	[]string{"tenant:42:users"},
	soroi.InvalidateOptions{Refetch: soroi.RefetchNone},
); err != nil {
	return fmt.Errorf("invalidate committed users change: %w", err)
}
```

The two-second operation deadline bounds this tag call; unlike shared-key invalidation, it uses the supplied context directly. Remote errors leave local state unchanged. A successful remote operation is not rolled back if local processing later fails. Handle this failure separately from the business write.

### Inspect watcher diagnostics and release the subscription

`watch.Stats()` reads counters without Redis I/O. `LastError` retains the latest transport or decoding failure even after recovery; use it with `Errors` and `Resyncs` to diagnose notification gaps. `Done()` closes when processing stops and subscription resources have been released. Unexpected stream closure terminates the watcher; investigate the failure and recreate it if the application needs to resume watching.

```go
stats := watch.Stats()
if stats.LastError != nil {
	fmt.Printf("watcher errors: %d, last error: %v\n", stats.Errors, stats.LastError)
}
select {
case <-watch.Done():
	fmt.Println("watcher stopped")
default:
}
watch.Close()
```

`Close()` cancels processing and waits for subscription release. It is idempotent, so explicit shutdown can coexist with deferred cleanup. Closing or canceling a watcher does not close its Client or the borrowed Redis connection. At shutdown, release watchers and Clients before closing `remote`; query handles still require their own cleanup.

## Complete example

The program below combines remote tags and two Clients to invalidate a user detail and list after a committed source change. It is maintained at [examples/redis/shared_invalidation/main.go](../examples/redis/shared_invalidation/main.go). To run an edited copy, save it as `main.go` in a new `invalidation-guide` directory inside the configured `examples/redis` module, then run it from that module with `REDIS_ADDR=127.0.0.1:16379 go run ./invalidation-guide`.

The example resets only the reserved `soroi-invalidation-demo` namespace's demonstration label, so repeated runs produce the same output. Metadata remains until the disposable Redis server is stopped. `RefetchNone` and disabled handles make the source call counts deterministic; production applications can use the default observed-query refresh policy.

```go
// Command shared_invalidation demonstrates typed remote tag invalidation and local notification delivery.
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/soroi"
	redistore "github.com/Waterkyuu/soroi/store/redis"
	"github.com/redis/go-redis/v9"
)

// User represents the immutable source value shared by detail and list queries.
type User struct {
	// Name is the display name returned by the authoritative source.
	Name string
}

// main reports configuration and query errors to the shell.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run commits a simulated source change before remotely invalidating two related query result types.
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
		return err
	}
	store, err := redistore.New(
		remote,
		redistore.Options{Prefix: "soroi-invalidation-demo", EnableTags: true, EnableNotifications: true},
	)
	if err != nil {
		return err
	}
	detailShared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	listShared, err := soroi.NewSharedCache(store, soroi.SharedCacheOptions[[]User]{
		Key: "tenant:42:users:v1", Format: "users.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	tags := []string{"tenant:42:users"}
	// Reset only the reserved demonstration namespace's label so repeated runs produce the same output.
	if _, err := store.InvalidateTags(ctx, tags); err != nil {
		return err
	}
	var source atomic.Value
	source.Store(User{Name: "Alice"})
	var calls atomic.Int64
	detail, err := soroi.QueryDefinition(
		"detail",
		func(context.Context) (User, error) { calls.Add(1); return source.Load().(User), nil },
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		return err
	}
	detail.Tags = tags
	detail.Shared = detailShared
	list, err := soroi.QueryDefinition(
		"list",
		func(context.Context) ([]User, error) { calls.Add(1); return []User{source.Load().(User)}, nil },
		soroi.QueryOptions{Disable: true},
	)
	if err != nil {
		return err
	}
	list.Tags = tags
	list.Shared = listShared
	a := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	b := soroi.NewClient(soroi.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	watchA, err := a.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		return err
	}
	defer watchA.Close()
	watchB, err := b.WatchShared(ctx, store, soroi.InvalidateOptions{Refetch: soroi.RefetchNone})
	if err != nil {
		return err
	}
	defer watchB.Close()
	if err := detail.Prefetch(ctx, a); err != nil {
		return err
	}
	if err := list.Prefetch(ctx, a); err != nil {
		return err
	}
	if _, err := detail.Fetch(ctx, b); err != nil {
		return err
	}
	if _, err := list.Fetch(ctx, b); err != nil {
		return err
	}
	d, l := detail.Query(b), list.Query(b)
	defer d.Close()
	defer l.Close()
	fmt.Printf("initial detail: %s\n", d.Snapshot().Data.Name)
	fmt.Printf("initial list: %s\n", l.Snapshot().Data[0].Name)
	fmt.Printf("source calls: %d\n", calls.Load())
	// In production, this step is a successful database or API commit.
	source.Store(User{Name: "Bob"})
	if err := a.InvalidateSharedTags(
		ctx,
		store,
		tags,
		soroi.InvalidateOptions{Refetch: soroi.RefetchNone},
	); err != nil {
		return err
	}
	if err := waitStale(ctx, d); err != nil {
		return err
	}
	if err := waitStale(ctx, l); err != nil {
		return err
	}
	fmt.Printf("notified detail stale: %t\n", d.Snapshot().Stale)
	fmt.Printf("notified list stale: %t\n", l.Snapshot().Stale)
	fmt.Printf("retained detail: %s\n", d.Snapshot().Data.Name)
	newDetail, err := detail.Fetch(ctx, b)
	if err != nil {
		return err
	}
	newList, err := list.Fetch(ctx, b)
	if err != nil {
		return err
	}
	fmt.Printf("refreshed detail: %s\n", newDetail.Name)
	fmt.Printf("refreshed list: %s\n", newList[0].Name)
	fmt.Printf("source calls: %d\n", calls.Load())
	return nil
}

// waitStale waits for notification publication without sleeps, polling, or assumptions about delivery latency.
func waitStale[V any](ctx context.Context, query *soroi.QueryHandle[V]) error {
	for {
		select {
		case state := <-query.Updates():
			if state.HasData && state.Stale {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
```

Run the maintained example from the repository root while the disposable Redis server is running:

```sh
cd examples/redis
REDIS_ADDR=127.0.0.1:16379 go run ./shared_invalidation
```

Expected output:

```text
initial detail: Alice
initial list: Alice
source calls: 2
notified detail stale: true
notified list stale: true
retained detail: Alice
refreshed detail: Bob
refreshed list: Bob
source calls: 4
```

Two Clients reuse a `User` detail and a `[]User` list through Redis. A commits the source change and invalidates their shared label. B receives stale snapshots for both result types, retains Alice during the stale interval, and explicitly fetches Bob. The source call count rises from two to four only after the explicit reads.

## API contracts

| API | Contract |
| --- | --- |
| `client.WatchShared(ctx, store, options...)` | Confirm subscription admission, mark existing bound queries stale, and return a `*SharedWatcher` with zero or one refresh policy |
| `watch.Close()` | Cancel processing and wait for watcher-owned subscription release; idempotent and safe under concurrent closure |
| `watch.Done()` | Close after processing stops and resources are released |
| `watch.Stats()` | Return cumulative delivery and recovery statistics |
| `definition.InvalidateShared(ctx, client, options...)` | Commit remote key invalidation before applying the exact local-key invalidation; publish one operation receipt |
| `shared.Invalidate(ctx)` | Perform remote key invalidation only; active watchers can still invalidate their local queries asynchronously |
| `client.InvalidateSharedTags(ctx, store, tags, options...)` | Commit remote OR-matching tag invalidation, then apply it to this Client's bound queries; no remote rollback on local failure |
| `store.InvalidateTags(ctx, tags)` | Return the committed tag receipt without directly operating on a Client |

Use the same `Store` object for a Client's query configurations and its watcher. `InvalidationScope()` is a process-local routing identity, not a cross-process cache key. Different processes may create different Store objects with the same Redis address, Prefix, and EnableTags configuration; their notifications use the same remote channel. Different local Store objects intentionally have separate routing identities, even when their remote configurations happen to match.

Create one watcher per Client and Store pair. Key-only notifications work with the default Redis layout when EnableNotifications is true. A store with notifications disabled rejects WatchShared before subscribing; plain key invalidation needs no PUBLISH permission. `EnableTags` is required for remote tag operations; `ShareTags` requests remote registration from the query definition. Constructor validation returns `ErrSharedCapability` when the configured store cannot register tags. A custom RedisClient implementing only Eval can continue to provide shared storage, but WatchShared requires the optional Subscribe capability.

Notification transport supports a single Redis primary, Sentinel clients, and Redis Cluster. A go-redis Ring contains independent servers without cluster-wide Pub/Sub forwarding; enabling notifications with a Ring is rejected with ErrSharedCapability. Its storage-only P0 use remains supported.

Watch setup uses the caller context and Client lifetime. The Redis adapter bounds initial subscription acknowledgement by one second; command-client dial settings also apply. Tag invalidation uses the supplied caller context directly: give it a deadline suitable for the operation. Key invalidation retains SharedCacheOptions.Timeout. These explicit invalidation calls do not automatically retry business writes.

## Notification policy and local lifecycle

`WatchShared` accepts zero or one `InvalidateOptions` value. Omitted options and the zero value use `RefetchObserved` with `RefetchAfterLoad: false`: matching retained results become stale and enabled observers refresh when no load is active. Set `RefetchAfterLoad: true` to coalesce an active load into at most one follow-up. Extra configurations return `ErrTooManyOptions` before opening a subscription or marking local queries stale. `RefetchNone` publishes stale state without starting new work.

Notifications preserve HasData and existing values. They do not enable disabled handles or create subscriptions for unobserved queries. A subsequent explicit Fetch can load a replacement. Query handles still own their own Close, independently of watcher lifetime.

Bindings survive local Set, Update, data-age expiry, and data removal while a handle or active query state retains ownership. A completely released query key loses its binding and does not participate in later notifications. Local Set and Update still perform no remote write. Remove, Clear, GC, and Client.Close do not delete Redis records or publish remote invalidations.

Operation IDs suppress duplicate application of the most recent receipt. This covers a helper's local invalidation racing with its own broadcast. Receipts do not impose a business transaction order or promise exactly-once delivery across reconnects. Delayed older operations can conservatively cause another refresh.

Caller cancellation and Client.Close stop the watcher. Client.Close initiates cancellation; `watch.Close()` or waiting for Done provides the resource-release barrier. The borrowed Redis command client stays open. An unexpectedly closed stream triggers a final scope reset, records an error, and terminates the watcher; inspect Done and recreate it when using a custom source with a terminal failure.

## Remote tag registration and matching

Remote labels follow the local exact-string and OR rules: case-sensitive comparison, no prefix or whitespace normalization, duplicate labels have no additional effect, and an empty string is an ordinary tag. An empty invalidation list is a no-op. Tenant boundaries must be part of the label or Redis namespace, for example `tenant:42:users`.

Remote registration occurs during a shared read, including a shared hit. Constructing a definition, creating a disabled handle, or reusing a fresh local result does not perform Redis I/O and therefore does not register new remote relationships. Local labels remain available for QueryFilter operations. To register newly added remote labels immediately, start an explicit Refetch through a handle using the updated definition.

Remote membership is additive per shared key. A tagged store's Read checks all recorded labels, including labels registered by another Client. Other readers of that record inherit its remote membership even when their own ShareTags setting is false. RegisteredTags notifications transfer newly added relationships into retained local bindings without marking their data stale. This makes a later peer-declared tag invalidation reach the same shared key locally.

After learning remote dependencies, typed query reads confirm the version snapshot before reusing a tagged result. A changed snapshot discards the cached payload and falls back to the source. This closes the interval in which a notification could arrive before an unknown tag is registered locally. Tagged reuse adds a confirmation read; untagged records keep the original single-read path.

Per-key invalidation, data expiry, and local removal do not remove remote membership. Membership and tag generations persist within the namespace. Schema or grouping changes requiring a different membership set should use a new versioned shared key or namespace. No query predicate or local QueryFilter is serialized into a remote scan.

## Atomic versions and Redis Cluster layout

With EnableTags false, each record retains its per-key hash slot. With EnableTags true, every record and tag-generation hash in a namespace uses the same hash tag:

```text
<prefix>:tagged:{<SHA-256 of prefix>}:<SHA-256 of logical key>:version
<prefix>:tagged:{<SHA-256 of prefix>}:<SHA-256 of logical key>:data
<prefix>:tagged:{<SHA-256 of prefix>}:<SHA-256 of logical key>:version:tags
<prefix>:tagged:{<SHA-256 of prefix>}:tag-generations
```

The opaque Version snapshot combines the key generation with all registered tag generations. Adding membership changes the key generation, so an earlier writer cannot omit the new relationship. Conditional writes compare every captured generation atomically. InvalidateTags updates all requested labels atomically and publishes one receipt; it does not enumerate records. An invalidated payload is removed lazily on its next read.

A source load whose conditional write is rejected still returns its successful value, but its local result remains stale and increments WriteConflicts. This preserves usable data without marking a snapshot from an obsolete generation fresh, including when a notification was missed.

Missing key, membership, or tag-generation metadata cannot grant an old writer permission to restore a valid payload. Random tokens, membership initialization checks, and snapshot confirmation handle metadata recreation. Restoring older Redis snapshots or losing acknowledged writes during failover can still roll back generations; the protocol does not add durability beyond Redis.

One namespace occupies one cluster slot in tag mode. This enables atomic cross-key label checks but concentrates that namespace's traffic on one primary. Split independent tenant namespaces when distribution is required; a tag operation cannot span those namespaces atomically. Enabling tags uses a separate physical layout and notification channel, so it starts a new cache namespace rather than migrating the default layout in place. Deploy all participants with consistent Prefix and EnableTags settings.

The internal membership marker cannot collide with encoded labels. Labels and notification identities preserve arbitrary Go string bytes using base64 JSON representations. Application queries continue to use their original exact strings.

## Connection gaps and freshness fallback

Pub/Sub is asynchronous and has no replay log. Initial subscription admission is followed by a local scope reset, repairing any pre-subscription gap. A detected transport failure causes one reset for that disconnected interval. A successful resubscription causes another reset, invalidating any result installed while notifications were unavailable. Malformed messages also cause a reset rather than being silently discarded.

The adapter buffers 32 events and uses cancellation-aware backpressure. It does not use go-redis's channel API that can silently drop messages after a send timeout. Slow consumers can still cause the Redis connection to be interrupted by server output-buffer limits; reconnection resets repair that gap. Network failure detection is not instantaneous. Normal StaleTime and shared absolute expiry remain the freshness bounds while notification delivery is delayed or unavailable.

Tag generations are also checked on reads and writes independently of Pub/Sub. Notifications accelerate local invalidation; they are not the sole correctness mechanism. Time passing alone does not publish updates or start polling. There is no synchronous all-instance acknowledgement or distributed lock.

## Observability, errors, and tests

| Watch statistic | Meaning |
| --- | --- |
| `Invalidations` | Number of matching local queries changed by normal remote operations; duplicate receipts and membership-only messages do not increase it |
| `Resyncs` | Startup and connection-gap resets, including resets with no retained queries |
| `Errors` | Transport, decoding, and unexpected stream-closure failures reported by the source |
| `LastError` | Latest diagnostic error, retained after recovery |

SharedCache.Stats continues to describe value reuse and persistence. Client.Stats continues to describe local load/cache activity. Watch statistics belong to one watcher and are not aggregated across processes.

Remote invalidation failure returns its error without changing local state. A transport timeout may have an uncertain remote outcome; retrying invalidation generates another safe receipt. A remote operation already committed is not rolled back if the Client closes before local application. Watch setup errors return without retaining a reader. Missing capabilities return ErrSharedCapability. Do not retry an already committed database write solely because cache synchronization failed.

Run the root lint, race tests, and build, and independently run the Redis module's vet, lint, race tests, and build. The tests cover key/tag delivery, disabled and enabled consumers, receipt deduplication, remote tag union propagation, tenant isolation, dependency-registration races, cancellation, connection recovery, malformed payloads, binary identities, and metadata loss. Redis tests use disposable local servers with persistence disabled; CI requires redis-server.
