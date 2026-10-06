# Observability and client statistics

[简体中文](observability.zh-CN.md) · [Cache & lifecycle](cache.en-US.md) · [Queries & conditions](queries.en-US.md)

Use `client.Stats()` to inspect cache reuse, request sharing, load outcomes, and cleanup. Run this complete example to see two reads of the same key execute one load.

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

// main demonstrates one load followed by a fresh cache hit.
func main() {
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Minute})
	defer client.Close()
	load := func(context.Context) (string, error) {
		return "hello", nil
	}
	for i := 0; i < 2; i++ {
		if _, err := cacheq.Fetch(
			context.Background(),
			client,
			"greeting",
			load,
		); err != nil {
			log.Fatal(err)
		}
	}
	state := client.Stats()
	fmt.Printf(
		"hits=%d misses=%d loads=%d entries=%d\n",
		state.CacheHits,
		state.CacheMisses,
		state.Loads,
		state.CacheEntries,
	)
}
```

Output: `hits=1 misses=1 loads=1 entries=1`. Applications can read snapshots periodically for logs or their existing monitoring systems.

## `Stats`: inspect cache savings and load activity

```go
state := client.Stats()
fmt.Printf(
	"hits=%d misses=%d merged=%d loads=%d retries=%d entries=%d\n",
	state.CacheHits,
	state.CacheMisses,
	state.MergedRequests,
	state.Loads,
	state.Retries,
	state.CacheEntries,
)
completed := state.LoadSuccesses + state.LoadFailures + state.LoadCancellations
if completed > 0 {
	fmt.Println("average load duration:", state.TotalLoadDuration/time.Duration(completed))
}
```

`Stats()` returns a coherent value snapshot for this client. Counters accumulate from construction and survive `Clear` and `Close`. Reading statistics does not request data, update recency, restart GC timers, or expire old values. Independent clients have independent counters.

| Field | Meaning |
| --- | --- |
| `CacheHits` | Valid non-forced `Fetch`, `FetchWithExpiry`, and `Prefetch` calls, enabled `Query` construction, or disabled-to-enabled transitions reusing fresh data |
| `CacheMisses` | Those decisions without fresh data, counted once per call or transition, including `ErrNoFetcher` |
| `MergedRequests` | Calls or enabled consumers joining existing work, counted once even if owner cancellation requires a replacement load |
| `Loads` | Shared operations started, including explicit `Refetch` and automatic invalidation refreshes; retries belong to the same operation |
| `LoadSuccesses` | Completed operations with no final error |
| `LoadFailures` | Completed operations with a final error other than cancellation or deadline expiration |
| `LoadCancellations` | Operations ending with cancellation or a deadline error; canceling only a waiting consumer does not cancel shared work |
| `Retries` | Actual additional loader attempts; canceled backoff waits do not count |
| `TotalLoadDuration` | Sum of elapsed time for completed operations, including scheduling, retry waits, and cancellation; measured independently of `Options.Clock` |
| `CacheEntries` | Current retained results and errors, excluding empty type metadata; over-age data awaiting a cache operation is still counted |
| `LRUEvictions` | Results or errors discarded to enforce `MaxEntries` |
| `GCCollections` | Keys removed by inactive-cache timers |
| `AgeExpirations` | Values cleared when cache operations discover they have reached `MaxAge`, once per installed value |

`Get`, handle `Snapshot()`, disabled query construction, and invalid or already-canceled calls do not count as cache decisions. A fresh fallback containing an error is still a cache hit. Creating an enabled query over stale data counts as a miss even if the old value remains visible. Explicit `Refetch` and invalidation refreshes count as loads but do not add hits or misses.

In-progress work contributes to `Loads`, but its outcome and duration appear only after completion. Detached or canceled operations remain included even when their result is not installed. Already-started work can finish updating counters after `Close`. Manual `Remove` and `Clear` do not increment cleanup-cause counters.

## Verification

```sh
go test -race ./... -run 'Test(Stats|HTTPStats)' -count=1
```

[Statistics tests](../stats_test.go) cover counting boundaries, cancellation, retries, cleanup, and client isolation; [HTTP e2e tests](../e2e/stats_test.go) compare shared requests, retries, cache reuse, and refresh outcomes against actual HTTP requests.
