# Reproducible cacheq benchmarks

[简体中文](benchmarks.zh-CN.md)

The benchmarks in [query_benchmark_test.go](../query_benchmark_test.go) measure
public API costs without network services or extra module dependencies. They
check returned values and report allocations. Request-sharing and eviction
workloads also check their counters, so a faster run cannot silently do less work.

## Run from the repository root

For repeated measurements across two worker counts:

```sh
go test -run '^$' -bench . -benchmem -count=3 -cpu=1,4
```

For a quick workload correctness check, use `-benchtime=1x`. A single iteration
is not a performance measurement. Use ordinary builds for published timings;
the race detector changes costs substantially.

Record the source revision, Go version, OS, architecture, CPU, command, and all
samples. Keep the machine idle and use the same environment for comparisons.
Do not select just the fastest run. For a before/after comparison, save each
revision's output and analyze repeated samples with your preferred statistical
tool.

## Workloads and units

| Benchmark | One operation measures |
| --- | --- |
| `CacheHit/Get`, `Fetch`, `Snapshot` | One warm typed public API read; setup and initial data installation are excluded |
| `CacheHit/MapMutexReference` | A locked map read and value check, without TTL, query state, statistics, or request sharing |
| `ParallelFetch/Keys=1,1024` | One warm Fetch; workers cycle through preloaded keys with no loader execution |
| `SharedLoad/Consumers=1,8,64` | One complete concurrent subscription burst: reset the key, register consumers, load once, receive results, close handles |
| `SubscriberUpdates/Subscribers=1,16,128` | One Set and all notifications; Consumed also drains each subscriber's notification |
| `CapacityEviction/Entries=64,1024` | One Set in a full LRU cache, cycling through capacity + 1 keys to force one eviction |
| `GCRetentionRead/GCTime=0s,1h0m0s` | One Get of an unsubscribed key, with inactive retention disabled or a timer stopped and rescheduled |

Each SharedLoad operation is a **burst**, not one consumer call. Its
`consumers/burst` and `loads/burst` metrics show the amount of work.
A channel barrier holds the loader until every concurrent consumer has
registered. Exactly one loader must execute per burst, with every other
consumer joining it. No sleep is used to create overlap.

LatestOnly leaves notification channels unread after registration, exercising
replacement of the latest snapshot for slow consumers. Consumed receives every
publication. Each Set is one operation regardless of the subscriber count.

ParallelFetch uses `testing.B.RunParallel`; `-cpu` changes GOMAXPROCS and the
worker count. Its ns/op describes aggregate throughput, not per-request tail
latency. Distributed keys cycle through a warm working set, rather than
measuring unbounded insertion. Keys are boxed before timing to avoid charging
key construction to cache access.

GCRetentionRead measures production timer scheduling, **not** expiration or
collection throughput. Its long deadline prevents timed deletion during the run.
GC cleanup behavior remains covered by the deterministic retention tests.

## Recorded local sample

Measured on 2026-10-07 with Go 1.27.1, macOS / darwin arm64, Apple M3 Pro.
Numbers below are medians of three samples in ns/op, rounded to one decimal.
The [raw output](benchmarks-darwin-arm64.txt) includes all workloads and allocations.

```sh
go test -run '^$' -bench . -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

| Workload | GOMAXPROCS=1 | GOMAXPROCS=4 |
| --- | ---: | ---: |
| CacheHit / Get | 208.1 | 207.4 |
| CacheHit / Fetch | 231.9 | 212.1 |
| ParallelFetch / 1 key | 189.1 | 338.3 |
| ParallelFetch / 1024 keys | 218.5 | 308.7 |
| SharedLoad / 64 consumers, per burst | 73235.0 | 77942.0 |
| SubscriberUpdates / 128 consumed, per Set | 12322.0 | 12301.0 |
| CapacityEviction / 1024 entries | 305.8 | 250.1 |
| GCRetentionRead / timer enabled | 423.3 | 391.5 |

Every shared-load configuration reports 1 load/burst, and every capacity
configuration reports 1 eviction/op. Warm concurrent reads do not scale
linearly on this machine. That motivates profiling the shared lock under an
application workload before deciding whether sharding is worthwhile.

These are short local samples of small integer values, not a cross-platform
performance promise. Subscriber tests measure notification and receive costs,
not rendering. There is no external I/O, serialization, retry, or large payload.
Allocation counts include the public API path and, for bursts, goroutine and
subscription setup.

## Interpret comparisons carefully

The map-and-mutex reference is a minimal synchronization cost reference, not a
feature-equivalent cache. It does not implement freshness, retained errors,
invalidation, subscriptions, cancellation, or request sharing. Its timing cannot
support a claim that one complete solution is a given multiple faster.

For a comparison with another cache, align key construction, result types,
expiration behavior, initial state, concurrency, load latency, and capacity.
Compare basic retrieval separately from the query lifecycle, and describe the
additional application code needed to provide matching semantics.

To investigate warm-read lock contention:

```sh
go test -run '^$' -bench '^BenchmarkParallelFetch$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

Profiling adds overhead; collect ordinary timings separately. Optimize only
when an observed application workload justifies the complexity.
