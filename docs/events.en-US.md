# Diagnostic events

[简体中文](events.zh-CN.md)

`Client.SubscribeEvents` provides a diagnostic event stream with optional key filtering. It records cache hits, loads, request sharing, retries, invalidation, and removal. The API supports server handlers, background jobs, CLI programs, and applications using `Query`.

`Stats()` reports cumulative Client activity; diagnostic events describe the source, cause, and outcome of individual operations. Subscribing does not change caching, request sharing, or cancellation behavior.

## Quick start

This example subscribes to `product:42`, performs two `Fetch` calls, and prints load and cache-hit events. Save it as `main.go` in a module importing cacheq, then run `go run .`.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main records loading and cache reuse for a single query key.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()

	events, err := client.SubscribeEvents(cacheq.EventOptions{
		Key:    "product:42",
		Buffer: 16,
	})
	if err != nil {
		panic(err)
	}
	defer events.Close()

	load := func(context.Context) (string, error) { return "product 42", nil }
	for range 2 {
		if _, err := cacheq.Fetch(context.Background(), client, "product:42", load); err != nil {
			panic(err)
		}
	}

	// The example buffers four events; services should consume the stream concurrently.
	client.Close()
	for event := range events.Events() {
		fmt.Printf("%v %s load=%d source=%s reason=%s joined=%d attempts=%d\n",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason, event.Joined, event.Attempts)
	}
	fmt.Printf("dropped=%d\n", events.Dropped())
}
```

Output:

```text
product:42 load_started load=1 source=fetch reason=missing joined=0 attempts=0
product:42 load_finished load=1 source=fetch reason=missing joined=0 attempts=1
product:42 cache_hit load=0 source=fetch reason=fresh joined=0 attempts=0
<nil> client_closed load=0 source=none reason=client_closed joined=0 attempts=0
dropped=0
```

The first call starts a load whose start and completion share `LoadID: 1`. The second call reuses fresh data without invoking the loader. `Attempts` is zero at registration and one at completion, indicating one actual invocation.

The example reads buffered records after closing the Client. Long-running applications should consume concurrently to avoid filling the buffer.

## Subscription options

```go
subscription, err := client.SubscribeEvents(cacheq.EventOptions{
	Key:    "product:42",
	Buffer: 256,
})
```

| Field | Default | Contract |
| --- | --- | --- |
| `Key` | `nil` | `nil` observes all keys; other values select an exact key |
| `Buffer` | `128` | Maximum unread records; zero selects the default |

An exact filter must be comparable and equal to itself. Slices, maps, and NaN-containing keys return `ErrInvalidKey`. Negative buffer capacities return `ErrInvalidEventOptions`; closed Clients return `ErrClosed`.

Subscriptions receive future events without replay. Subscribing during a load can produce join or completion events without a preceding start event.

### Logging integration

This snippet uses the standard library's `log` package to consume an existing subscription in a separate goroutine:

```go
go func() {
	for event := range subscription.Events() {
		log.Printf("key=%v event=%s load=%d trigger=%s reason=%s joined=%d attempts=%d duration=%s applied=%t err=%v",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason,
			event.Joined, event.Attempts, event.Duration, event.Applied, event.Err)
	}
}()
```

Call `subscription.Close()` when the consumer leaves. This closes only diagnostics without canceling queries or modifying cache state. Events contain keys and errors, but no cached values.

## Event fields

| Field | Contract |
| --- | --- |
| `Sequence` | Client-wide emitted event sequence; filtering and drops can leave gaps |
| `Time` | Observation time using `Options.Clock` |
| `Key` | Query key; nil for `EventClientClosed` |
| `Kind` | Event type |
| `LoadID` | Client-local shared operation ID; zero when no load is associated |
| `Trigger` | API decision that triggered the operation |
| `Reason` | Cache decision, removal, or discard cause |
| `Joined` | Accumulated additional joins to the operation |
| `Attempts` | Actual loader invocations, including the initial attempt and retries |
| `Duration` | Total time at completion, including scheduling and retry waits; measured independently of `Options.Clock` |
| `Err` | Retained cache error, previous retry failure, or final load error |
| `Applied` | Whether a completed outcome was installed in shared cache state |

## Event types

`Kind.String()` returns the log label below.

| Type | Log label | Condition |
| --- | --- | --- |
| `EventCacheHit` | `cache_hit` | A query decision reused an outcome considered fresh by its consumer |
| `EventLoadStarted` | `load_started` | A new shared operation was registered |
| `EventLoadJoined` | `load_joined` | Another consumer joined an active load |
| `EventRetryStarted` | `retry_started` | An additional attempt began; `Err` contains the previous failure |
| `EventLoadFinished` | `load_finished` | A load completed, including failure and cancellation |
| `EventResultDiscarded` | `result_discarded` | A completed outcome was not applied to shared cache state |
| `EventInvalidated` | `invalidated` | An existing key was explicitly marked stale |
| `EventCacheRemoved` | `cache_removed` | Retained state was cleared, or data reached `MaxAge` |
| `EventLocalWrite` | `local_write` | `Set` successfully installed a local value |
| `EventClientClosed` | `client_closed` | The Client closed; broadcast to every subscription, including exact-key filters |

`Get` and `Snapshot` do not emit cache-hit events. They can emit removal when they discover data beyond `MaxAge`. Rejected API calls, removal of an absent key, and internal cleanup of empty type metadata emit no event.

## Operation sources and reasons

### Sources

| `Trigger` | Operation |
| --- | --- |
| `TriggerFetch` | `Fetch`, `FetchWithOptions`, `FetchWithExpiry` |
| `TriggerQuery` | Automatic loading or cache reuse when constructing an enabled `Query` |
| `TriggerEnable` | `SetEnabled(true)` transitioning from disabled to enabled |
| `TriggerRefetch` | `QueryHandle.Refetch` |
| `TriggerInvalidate` | Explicit invalidation and its automatic refresh |
| `TriggerPrefetch` | `Prefetch` |
| `TriggerNone` | Local writes, cache removals, Client closure, and other non-query transitions |

Retries and completions retain the original load initiator. Joins record the joining consumer's source. An invalidation event uses `TriggerInvalidate` without implying that a refresh started.

### Reasons

| `Reason` | Log label | Contract |
| --- | --- | --- |
| `ReasonFresh` | `fresh` | This consumer can reuse cached data |
| `ReasonMissing` | `missing` | No usable cached result is available |
| `ReasonExpired` | `expired` | This consumer's freshness deadline was reached |
| `ReasonInvalidated` | `invalidated` | Explicit invalidation requires fresh data |
| `ReasonPreviousFailure` | `previous_failure` | An earlier failed load left the outcome stale |
| `ReasonRefetch` | `refetch` | Explicit `Refetch` requires another load |
| `ReasonCapacity` | `capacity` | LRU eviction enforced `MaxEntries` |
| `ReasonInactive` | `inactive` | Idle collection reached `GCTime` |
| `ReasonMaxAge` | `max_age` | Data reached its maximum available age |
| `ReasonRemove` | `remove` | One key was explicitly removed |
| `ReasonClear` | `clear` | The cache was explicitly cleared |
| `ReasonSet` | `set` | A local write superseded older work |
| `ReasonCancel` | `cancel` | Caller cancellation, a deadline, or `Client.Cancel` discarded the outcome |
| `ReasonClientClosed` | `client_closed` | The Client ended its lifetime |
| `ReasonNone` | `none` | No applicable reason |

Ordinary freshness is evaluated per consumer. The same key can produce `fresh` for one consumer and `expired` for another. A failed `FetchWithExpiry` with a future deadline can retain fallback data and its error: cache-hit events preserve `Err`, and subsequent expiration reports `expired`.

Removal history lives only in the stream. Capacity eviction emits `cache_removed reason=capacity`; a subsequent load reports `missing`. When `MaxAge` removes data but its type binding remains, subsequent loading reports `max_age`.

## Load correlation and counts

Start, joins, retries, completion, and discard share a `LoadID`. In-flight invalidation also carries that ID; other cache transitions use zero. IDs are allocated without subscribers, allowing later subscribers to correlate subsequent events.

`Joined` counts additional joins and excludes the initiator. A completion with `Joined: 3` represents three extra joins. It does not count distinct users, current waiters, or passive handles. Canceled joins remain counted.

If owner cancellation causes surviving callers to start replacement work, the replacement has its own ID and join count. `Stats().MergedRequests` retains its existing per-call counting rules.

`Attempts` counts per-key loader invocations without inferring SQL or HTTP activity. With a `Batcher`, multiple key loaders can share one bulk callback.

## Applied and discarded outcomes

`EventLoadFinished.Applied` indicates installation in shared cache state. An installed error or a result kept stale by in-flight invalidation still counts as applied. This field does not indicate success or permanent retention.

An unapplied outcome emits `EventLoadFinished` with `Applied: false`, followed by `EventResultDiscarded`. Discard causes include cancellation, `Set`, `Remove`, and `Clear`. Late outcomes cannot overwrite replacement state.

## Delivery and closure

- Publication does not wait for consumers or invoke application logging callbacks.
- Full buffers drop new events. `Dropped()` reports cumulative loss; previously buffered records remain ordered.
- `Sequence` increases across emitted Client events. Subscriptions receiving the same event see the same sequence and timestamp; filtering and drops leave gaps.
- Multi-key operations emit one event per selected key. Map-based selection has no guaranteed key order.
- `EventSubscription.Close()` and `Client.Close()` close the corresponding channels. Buffered events remain readable after closure.
- Client closure attempts to send `EventClientClosed`. A full buffer can drop that record, but the channel still closes.
- `Client.Close()` does not wait for loaders that ignore cancellation. Late completions are not delivered to closed subscriptions.

Delivery is bounded and intended for logging and diagnostics. Monitor `Dropped()` when completeness matters; the stream does not guarantee lossless auditing.
