# Per-consumer query options

[简体中文](query-options.zh-CN.md)

`QueryOptions` overrides Client defaults directly at each `Query` call. There is no policy registry by key. Each handle owns copied values for `StaleTime`, `Retry`, `RetryIf`, `RetryDelay`, and `Timeout`. Use `FetchWithOptions` and `FetchOptions` for an imperative call with the same overrides; existing `Fetch`, `Prefetch`, and `FetchWithExpiry` signatures remain unchanged.

## Complete runnable example

Save this program as `main.go` in an application importing cacheq. It uses no network service and does not sleep. Both consumers share one value, but disagree about freshness. The first load retries once after a transient error.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Waterkyuu/cacheq"
)

// main demonstrates shared data with independent freshness and a filtered retry policy.
func main() {
	client := cacheq.NewClient(cacheq.Options{
		StaleTime: time.Minute,
		Retry:     2,
		Timeout:   10 * time.Second,
		Clock:     func() time.Time { return time.Unix(0, 0) },
	})
	defer client.Close()

	transient := errors.New("temporarily unavailable")
	var calls atomic.Int32
	load := func(context.Context) (string, error) {
		if calls.Add(1) == 1 {
			return "", transient
		}
		return "hello", nil
	}

	hour, immediate, timeout := time.Hour, time.Duration(0), 3*time.Second
	retries := 1
	steady := cacheq.Query(client, "greeting", load, cacheq.QueryOptions{
		StaleTime:  &hour,
		Retry:      &retries,
		RetryIf:    func(err error) bool { return errors.Is(err, transient) },
		RetryDelay: func(int) time.Duration { return 0 },
		Timeout:    &timeout,
	})
	defer steady.Close()

	live := cacheq.Query(client, "greeting", load, cacheq.QueryOptions{
		Disable:   true,
		StaleTime: &immediate,
	})
	defer live.Close()

	value, err := cacheq.FetchWithOptions(
		context.Background(),
		client,
		"greeting",
		load,
		cacheq.FetchOptions{StaleTime: &hour},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	fmt.Printf("steady stale=%t; live stale=%t\n", steady.Snapshot().Stale, live.Snapshot().Stale)
	stats := client.Stats()
	fmt.Printf("loads=%d; retries=%d\n", stats.Loads, stats.Retries)
}
```

Expected output:

```text
hello
steady stale=false; live stale=true
loads=1; retries=1
```

## Inheritance and explicit zero values

| Field | Omitted / nil | Explicit override |
| --- | --- | --- |
| `Disable` | False permits automatic loading | True disables automatic loading |
| `StaleTime` | Use Client freshness | Pointer to a duration; zero makes ordinary data immediately stale |
| `Retry` | Use Client retry count | Pointer to an integer; zero disables additional attempts |
| `Timeout` | Use Client timeout | Pointer to a duration; zero removes the Client timeout, while caller cancellation and deadlines still apply |
| `RetryIf` | Use the Client predicate | A function deciding which errors permit another attempt |
| `RetryDelay` | Use Client backoff | A function taking the additional attempt number, starting at one |

Pointer values are copied when a call or handle is created. Later mutation of the source variables does not reconfigure a handle. Callback closures retain their captured state; synchronize it when shared across loads. A handle keeps its policy through enablement changes and `Refetch`.

`Disable` defaults to false, so an empty `QueryOptions` or freshness-only overrides permit automatic loading. Set `Disable: true` to disable automatic loading explicitly. The flag is copied at construction; use `SetEnabled` to change it later. When several `QueryOptions` arguments are supplied, only the last one's fields are applied over Client defaults.

`RetryIf` is available in both Client `Options` and per-consumer options. It is consulted only for retryable errors while attempts remain. Cancellation and deadline errors never retry. Nil inherits the Client predicate; to override a restrictive Client predicate, provide a function returning true. Predicates and delay callbacks run outside the Client lock and may call Client APIs.

## Shared data, independent freshness

For ordinary successful loads and `Set`, each consumer uses the shared `UpdatedAt` and its own `StaleTime` to calculate `Stale` and `ExpiresAt`. `Get`, `Fetch`, and `Prefetch` use Client defaults. A caller with a longer window may reuse data that another consumer considers stale. Changing one consumer's freshness does not register settings for later callers.

`FetchWithExpiry` supplies an authoritative absolute deadline, including for explicitly cached fallback data and errors. Consumer `StaleTime` cannot shorten or extend this deadline. Client `MaxAge` caps availability and freshness in either case. Capacity, GC, and the clock remain Client policies.

Explicit invalidation and a failed refresh mark retained data stale for every consumer. Passing time alone does not publish updates or start requests; `Snapshot` evaluates current freshness. `Updates` carries consumer-specific snapshots on ordinary state transitions.

## Which request policy wins?

The consumer starting a load supplies its loader, retry count, predicate, backoff, and whole-load timeout. Same-key consumers joining that load reuse it without replacing these settings. A joining call's context still controls its own wait. Its `Timeout` override applies only when it starts a new load.

Automatic invalidation refresh uses the oldest still-enabled handle's loader and policy. Disabling or closing that handle allows the next enabled handle to supply future automatic refreshes. Existing requests keep their initiator's settings. Manual `Refetch` uses the invoking handle when starting a new load.

Same-key loaders must still represent the same data; include parameters, user identity, and tenant scope in the key. The implementation adopts TanStack Query's distinction between shared data and observer-specific options, while preserving cacheq's explicit-expiry, cancellation, and invalidation contracts. See [TanStack's default option merge](https://github.com/TanStack/query/blob/main/packages/query-core/src/queryClient.ts), [observer freshness](https://github.com/TanStack/query/blob/main/packages/query-core/src/queryObserver.ts), and [request reuse](https://github.com/TanStack/query/blob/main/packages/query-core/src/query.ts).

## Migrating enablement configuration

`QueryOptions.Enabled *bool` from v0.1.1 is replaced by `Disable bool`. Omit `Disable` when the old `Enabled` was nil or pointed to true. Replace a pointer to false with `Disable: true`; for a computed boolean, use `Disable: !enabled`. Empty `QueryOptions{}` and freshness-only overrides still enable automatic loading. Existing `SetEnabled(bool)` calls remain unchanged.

## Verification

```sh
go test -race ./... -run 'Test(QueryOptions|FetchOptions)' -count=1
```

[Behavior tests](../query_options_test.go) cover independent freshness, inherited and zero-valued options, copied policy values, explicit deadlines, availability limits, shared requests, retries, callback cancellation, and deterministic automatic refresh selection.
