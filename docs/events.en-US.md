# Per-key diagnostic events

[简体中文](events.zh-CN.md)

Use `Client.SubscribeEvents` to explain why a key loaded again and how its work was shared. `Stats()` still reports client totals; events describe individual cache decisions and load lifecycles. The feature works with server handlers, jobs, CLI programs, and query handles.

The subscription observes future events. It does not retain history or change freshness, retries, cancellation, or request sharing.

## Complete runnable example

Save this program as `main.go` in a module that imports cacheq, then run `go run .`. No server or credentials are needed.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main observes a first load, cache reuse, and client shutdown for one key.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	events, err := client.SubscribeEvents(cacheq.EventOptions{Key: "product:42", Buffer: 16})
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

	// This short example fits in the buffer; a running service should consume concurrently.
	client.Close()
	for event := range events.Events() {
		fmt.Printf("%v %s load=%d source=%s reason=%s joined=%d attempts=%d\n",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason, event.Joined, event.Attempts)
	}
	fmt.Printf("dropped=%d\n", events.Dropped())
}
```

Expected output:

```text
product:42 load_started load=1 source=fetch reason=missing joined=0 attempts=0
product:42 load_finished load=1 source=fetch reason=missing joined=0 attempts=1
product:42 cache_hit load=0 source=fetch reason=fresh joined=0 attempts=0
<nil> client_closed load=0 source=none reason=client_closed joined=0 attempts=0
dropped=0
```

The first `Fetch` invokes the loader; the second reuses its fresh result. `load=1` connects the first load's start and completion. A cache hit has no associated load, so its ID is zero. `attempts=0` at registration means the callback has not been invoked yet; completion reports one actual invocation.

## Subscribe to one key or all keys

```go
events, err := client.SubscribeEvents(cacheq.EventOptions{
	Key:    "product:42",
	Buffer: 256,
})
```

- Omit `Key` to observe every key. An exact filter must be comparable and equal to itself; slices, maps, and NaN-containing keys return `ErrInvalidKey`.
- `Buffer: 0` uses 128 slots. Negative capacities return `ErrInvalidEventOptions`.
- A closed client returns `ErrClosed`.
- Subscribing during a load can observe its joins and completion without receiving its earlier start. `LoadID` is allocated even when nobody subscribes.

In a running service, consume the stream concurrently and connect it to your existing logger or metrics code:

```go
go func() {
	for event := range events.Events() {
		log.Printf("key=%v event=%s load=%d source=%s reason=%s joined=%d attempts=%d duration=%s applied=%t err=%v",
			event.Key, event.Kind, event.LoadID, event.Trigger, event.Reason,
			event.Joined, event.Attempts, event.Duration, event.Applied, event.Err)
	}
}()
```

The snippet uses the standard library's `log` package. Call `events.Close()` when its consumer leaves; this closes only the diagnostic stream. `client.Close()` also closes all event streams. Buffered events remain readable after either closure.

## What happened?

`Kind` describes the transition; its `String()` label is convenient for logs.

| Kind | Meaning |
| --- | --- |
| `EventCacheHit` / `cache_hit` | A query decision reused an outcome considered fresh by this consumer |
| `EventLoadStarted` / `load_started` | A new shared load was registered |
| `EventLoadJoined` / `load_joined` | Another consumer joined that load |
| `EventRetryStarted` / `retry_started` | An additional loader attempt began; `Err` holds the previous failure |
| `EventLoadFinished` / `load_finished` | A load completed, including failures and cancellation |
| `EventResultDiscarded` / `result_discarded` | The completed outcome was not applied to shared cache state |
| `EventInvalidated` / `invalidated` | An existing key was explicitly marked stale |
| `EventCacheRemoved` / `cache_removed` | Retained state was cleared, or its data reached `MaxAge` |
| `EventLocalWrite` / `local_write` | `Set` successfully installed a value |
| `EventClientClosed` / `client_closed` | The client closed; broadcast with a nil key, including to filtered subscriptions |

`Get` and `Snapshot` are passive reads and do not emit cache-hit events. They can emit a removal event when they discover data beyond `MaxAge`. Rejected API calls and removal of an absent key emit no event. Releasing empty internal type metadata emits no removal event.

## Why did it load?

`Trigger` identifies the API decision: `fetch`, `query`, `enable`, `refetch`, `invalidate`, or `prefetch`. Retry and completion events retain the original initiator. A join records the joining consumer's API. An invalidation event also uses `invalidate`, even if no refresh starts. Writes, removals, and client closure use `none`.

`Reason` explains the cache condition or transition:

| Reason label | Meaning |
| --- | --- |
| `fresh` | This consumer could reuse the result |
| `missing` | No usable cached result was available |
| `expired` | This consumer's freshness deadline was reached |
| `invalidated` | Explicit invalidation required fresh data |
| `previous_failure` | An earlier failed load left the outcome stale |
| `refetch` | A handle explicitly requested a load regardless of freshness |
| `capacity` | `MaxEntries` caused LRU eviction |
| `inactive` | `GCTime` caused idle collection |
| `max_age` | Data reached its maximum available age |
| `remove` / `clear` | A key or the cache was explicitly removed |
| `set` | A local write superseded older work |
| `cancel` | Caller cancellation, a deadline, or `Client.Cancel` superseded the result |
| `client_closed` | The client ended its lifetime |

Ordinary freshness is evaluated per consumer. Two consumers of the same key can legitimately report `fresh` and `expired` with different `StaleTime` policies. A failed `FetchWithExpiry` with a future deadline can cache fallback data and its error: reuse emits `cache_hit` with `Err`, and later expiration reports `expired`.

Removal history is available in the stream, without an unbounded map of removed keys. For example, capacity eviction emits `cache_removed reason=capacity`; the next load reports `missing`. `MaxAge` preserves the `max_age` cause while the entry's type binding remains.

## Correlate shared work and outcomes

- `LoadID` identifies one shared operation within a client. Start, joins, retries, completion, and discard share that ID. In-flight invalidation also carries it. Other cache transitions use zero.
- `Joined` counts additional joins accumulated for that operation. A completion with `Joined: 3` means three additional joins besides its initiator. It is not a count of distinct users, currently waiting callers, or passive handles. A canceled join still counts.
- If a canceled owner causes surviving callers to start replacement work, the replacement has a new `LoadID` and its own join count. `Stats().MergedRequests` keeps its existing per-call counting rules.
- `Attempts` counts per-key loader invocations, including the first and actual retries. With a `Batcher`, two key loaders can share one bulk callback. Events do not infer database or HTTP call counts.
- `Duration` on completion includes scheduling and retry waits, measured by the load-duration clock. `Time` uses `Options.Clock`; changing that clock does not change duration measurement.
- `Applied` is true when a completed outcome was installed in cache. An installed error or a result kept stale by in-flight invalidation still counts as applied. It does not mean success or permanent retention.
- An unapplied completion emits `load_finished` with `Applied: false`, followed by `result_discarded`. Its reason identifies cancellation, `Set`, `Remove`, or `Clear`; a late outcome cannot overwrite replacement state.

Events contain keys and errors, but no cached data. Treat keys and errors according to your application's logging policy.

## Bounded delivery

Publication never waits for the consumer or invokes application logging callbacks. A full buffer drops new events and increments `events.Dropped()`; earlier buffered events remain in order. Read the counter to detect incomplete diagnostics. This stream is unsuitable for lossless auditing.

`Sequence` increases across emitted events on the client. Filtering and buffer drops can leave gaps. Multiple subscriptions observing the same emitted event receive the same sequence and timestamp. A multi-key operation emits one event per selected key; map-based selection has no guaranteed key order.

Client closure tries to deliver `client_closed` before closing channels. A full buffer can drop that terminal record too; channel closure still occurs. `Client.Close` does not wait for loaders that ignore cancellation, and late completions are not emitted into closed streams.
