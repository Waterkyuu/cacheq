# Shared invalidation notifications and tags

[简体中文](shared-invalidation.zh-CN.md) · [Shared result cache](shared-cache.en-US.md) · [Query tags](query-tags.en-US.md)

## Overview

`Client.WatchShared` receives remote invalidations and applies them to local queries bound to the same store instance. Enabled observed queries refresh according to the watch policy; disabled queries retain their data and publish stale snapshots. Notifications are optional: a Client without a watcher continues to rely on its normal freshness deadlines.

Set `redistore.Options.EnableNotifications: true` to publish changes and allow watchers. Its default false preserves the original storage-only command and ACL contract. All writers must enable it for immediate notification propagation.

Remote tags group different shared keys and result types. Enable `redistore.Options.EnableTags` on the store and `SharedCacheOptions.ShareTags` on each participating query. `QueryDefinition.Tags` then supplies the labels registered remotely. `Client.InvalidateSharedTags` advances the remote tag generations and invalidates matching queries in the initiating Client. Watchers propagate the operation to other Clients in the same remote namespace.

The authoritative database or API write must commit before invalidation. Notification delivery does not provide a distributed transaction, a synchronous barrier across instances, or durable event replay.

## Requirements and quick start

The core contracts remain compatible with Go 1.22. The optional Redis module requires Go 1.26 and the dependency version declared in its go.mod. In addition to the shared cache's commands, notifications require `PUBLISH` and `SUBSCRIBE`; tags require `HGET`, `HSET`, `HGETALL`, and `HEXISTS`. Use a writable primary connection with appropriate ACL permissions and context/socket timeouts.

From the checked-out repository, start a disposable server in one terminal:

```sh
redis-server --bind 127.0.0.1 --port 16379 --save '' --appendonly no
```

From the repository root, in another terminal:

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

Two Clients reuse a `User` detail and a `[]User` list through Redis. A commits the source change and invalidates their shared label. B receives stale snapshots for both result types without manual local invalidation, retains Alice during the stale interval, and explicitly fetches Bob. `RefetchNone` and disabled handles make this example's source call counts deterministic. Production applications can use the default observed-query refresh policy.

The example resets only the reserved `cacheq-invalidation-demo` namespace's demonstration label. Metadata remains until that disposable Redis server is stopped. The complete program is maintained at [examples/redis/shared_invalidation/main.go](../examples/redis/shared_invalidation/main.go).

```go
// Command shared_invalidation demonstrates typed remote tag invalidation and local notification delivery.
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
		redistore.Options{Prefix: "cacheq-invalidation-demo", EnableTags: true, EnableNotifications: true},
	)
	if err != nil {
		return err
	}
	detailShared, err := cacheq.NewSharedCache[User](store, cacheq.SharedCacheOptions[User]{
		Key: "tenant:42:user:7:v1", Format: "user.json.v1", TTL: 5 * time.Minute, ShareTags: true,
	})
	if err != nil {
		return err
	}
	listShared, err := cacheq.NewSharedCache[[]User](store, cacheq.SharedCacheOptions[[]User]{
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
	detail := cacheq.QueryDefinition[User]{
		Key: "detail", Tags: tags, Shared: detailShared, Options: cacheq.QueryOptions{Disable: true},
		Fetcher: func(context.Context) (User, error) { calls.Add(1); return source.Load().(User), nil },
	}
	list := cacheq.QueryDefinition[[]User]{
		Key: "list", Tags: tags, Shared: listShared, Options: cacheq.QueryOptions{Disable: true},
		Fetcher: func(context.Context) ([]User, error) { calls.Add(1); return []User{source.Load().(User)}, nil },
	}
	a := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	b := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer a.Close()
	defer b.Close()
	watchA, err := a.WatchShared(ctx, store, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone})
	if err != nil {
		return err
	}
	defer watchA.Close()
	watchB, err := b.WatchShared(ctx, store, cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone})
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
		cacheq.InvalidateOptions{Refetch: cacheq.RefetchNone},
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
func waitStale[V any](ctx context.Context, query *cacheq.QueryHandle[V]) error {
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

## API contracts

| API | Contract |
| --- | --- |
| `client.WatchShared(ctx, store, options...)` | Confirm subscription admission, mark existing bound queries stale, and return a `*SharedWatcher`; the last complete option wins |
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

With no watch options, the policy is `RefetchObserved` with `RefetchAfterLoad: true`. It marks matching retained results stale and schedules refresh for enabled observers, coalescing an active load into at most one follow-up. If an option is supplied, that whole value replaces the default; omitted fields are zero values. `RefetchNone` publishes stale state without starting new work.

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
