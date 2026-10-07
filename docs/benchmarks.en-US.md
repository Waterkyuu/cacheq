# Cache benefit benchmarks

[简体中文](benchmarks.zh-CN.md)

The main comparison answers a practical question: how much work does cacheq save
when an application reads the same data repeatedly?

## Main comparison: 100 reads of the same data

All three scenarios perform 100 sequential reads and return the same SHA-256
digest of a fixed 64 KiB payload. They use the same loader, context, and result
checks. Each reported duration covers **the complete batch of 100 reads**.

| Scenario | Reads | Total time | Actual loader calls |
| --- | ---: | ---: | ---: |
| No cache: load for every read | 100 | 2,184.51 µs | 100 |
| First load: start empty, then reuse the result | 100 | 46.56 µs | 1 |
| Cache hits: start with a fresh cached result | 100 | 21.46 µs | 0 |

“First load” includes one cache miss followed by 99 hits. It measures the cost of
serving repeated requests starting with an empty cache. “Cache hits” measures 100
hits after a separate warm-up; the warm-up's one loader call and duration are
excluded from that row. Resetting the cold cache between batches is also excluded.
Every batch verifies the returned digest, and each scenario asserts its exact
loader count.

Measured on 2026-10-07 with Go 1.27.1, macOS / darwin arm64, Apple M3 Pro.
Durations are medians of five samples, converted from ns/op to microseconds per
batch. [Raw output](benchmarks-cache-benefit-darwin-arm64.txt) includes every
sample and allocations.

## Reproduce

Run from the repository root:

```sh
go test -run '^$' -bench '^BenchmarkCacheBenefit$' -benchmem -benchtime=300ms -count=5 -cpu=1
```

In Go's output, one operation is one batch: `ns/op` is the total time for 100
reads, `reads/batch` is 100, and `loads/batch` is 100, 1, or 0. Divide ns/op by
1,000 to get the microsecond totals in the main table. `B/op` and `allocs/op`
also apply to a complete batch.

The SHA-256 loader performs deterministic CPU work without network requests or
artificial sleeps. This is a reproducible example of avoiding repeated backend
work. The benefit depends on the real loader's cost, freshness window, and hit
rate; replace the workload with your application's loader before estimating its
benefit. The sample does not measure request merging, failures, or retries.

For a quick correctness check:

```sh
go test -race -run '^$' -bench '^BenchmarkCacheBenefit$' -benchtime=1x
```

Use ordinary builds for timings. Record the source revision, Go version, OS,
architecture, CPU, command, and all samples. Keep the machine idle and compare
repeated samples under the same conditions.

## Internal cost and concurrency diagnostics

The remaining benchmarks help maintainers investigate API overhead and
contention. Their earlier [raw sample](benchmarks-darwin-arm64.txt) is retained
separately from the cache-benefit comparison.

| Benchmark | One operation measures |
| --- | --- |
| `CacheHit/Get`, `Fetch`, `Snapshot` | One warm typed public API read |
| `CacheHit/MapMutexReference` | A locked map read without query lifecycle management |
| `ParallelFetch/Keys=1,1024` | One warm Fetch across concurrent workers |
| `SharedLoad/Consumers=1,8,64` | One concurrent subscription burst, including registration and cleanup |
| `SubscriberUpdates/Subscribers=1,16,128` | One Set and its notifications; Consumed also drains every notification |
| `CapacityEviction/Entries=64,1024` | One write that evicts an entry from a full LRU cache |
| `GCRetentionRead/GCTime=0s,1h0m0s` | One read with inactive retention disabled or a timer rescheduled |

SharedLoad holds the loader behind a channel barrier until every consumer has
registered. It checks for exactly one load per burst and one join per additional
consumer. LatestOnly subscriber tests exercise snapshot replacement for slow
consumers. Capacity tests check for one eviction per write. GCRetentionRead
measures timer scheduling, not expiration or collection throughput.

The map-and-mutex reference supplies fewer semantics than cacheq and cannot
demonstrate the benefit of caching. ParallelFetch's ns/op represents aggregate
throughput, not per-request tail latency. Increasing GOMAXPROCS affects concurrent
workers; it does not turn serial benchmarks into parallel workloads.

Run the dedicated concurrency diagnostics when investigating scalability:

```sh
go test -run '^$' -bench '^BenchmarkParallelFetch$' -benchmem -count=3 -cpu=1,4
```

To collect a mutex profile:

```sh
go test -run '^$' -bench '^BenchmarkParallelFetch$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

Profiling adds overhead; collect ordinary timings separately.
