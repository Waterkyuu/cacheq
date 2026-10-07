# Cache benefit benchmarks

[简体中文](benchmarks.zh-CN.md)

The main comparison answers a practical question: how much work does cacheq save
when an application reads the same data repeatedly?

## Main comparison: 100 sequential reads of the same data

All three scenarios use one request goroutine to perform **100 sequential reads,
waiting for each read to return before starting the next**, and return the same SHA-256
digest of a fixed 64 KiB payload. They use the same loader, context, and result
checks. Each reported duration covers **the complete batch of 100 sequential reads**.

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

## Concurrent request scenarios

Keep each batch at **64 requests**, compare **1, 4, 16, and 64 caller goroutines**,
and measure separately with **GOMAXPROCS=1 and 4**. One goroutine handles all
64 requests sequentially; multiple goroutines divide those same 64 requests.
GOMAXPROCS limits CPU parallelism for Go code and is configured independently
of the caller count.

Read either one shared key or 64 distinct keys, comparing no cache, first loads,
and cache hits. Every configuration uses the same loader: compute the SHA-256
digest of a fixed 64 KiB payload for that key. Distinct keys have distinct payloads.
All times below are **microseconds per complete batch of 64 requests**; lower is faster.

### GOMAXPROCS=1

| Requested data | Cache state | 1 goroutine | 4 | 16 | 64 | Actual loader calls |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Same key | No cache | 1,398.58 | 1,430.35 | 1,417.42 | 1,456.69 | 64 |
| Same key | First loads from an empty cache | 44.20 | 44.20 | 52.64 | 82.18 | 1 |
| Same key | All cache hits | 15.02 | 16.45 | 23.81 | 51.90 | 0 |
| Different keys | No cache | 1,415.30 | 1,418.52 | 1,451.63 | 1,492.18 | 64 |
| Different keys | First loads from an empty cache | 1,502.89 | 1,494.51 | 1,561.59 | 1,666.83 | 64 |
| Different keys | All cache hits | 14.82 | 16.60 | 24.40 | 57.47 | 0 |

### GOMAXPROCS=4

| Requested data | Cache state | 1 goroutine | 4 | 16 | 64 | Actual loader calls |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Same key | No cache | 1,398.65 | 421.73 | 428.44 | 495.77 | 64 |
| Same key | First loads from an empty cache | 51.82 | 56.79 | 62.11 | 71.89 | 1 |
| Same key | All cache hits | 17.43 | 23.13 | 34.19 | 49.60 | 0 |
| Different keys | No cache | 1,414.06 | 414.18 | 424.92 | 452.89 | 64 |
| Different keys | First loads from an empty cache | 1,758.86 | 442.75 | 476.14 | 529.24 | 64 |
| Different keys | All cache hits | 18.06 | 23.71 | 34.91 | 52.08 | 0 |

### Interpreting the results

With GOMAXPROCS=1, more callers do not substantially accelerate uncached
computation. With GOMAXPROCS=4, four callers reduce the same-key uncached batch
from 1,398.65 to 421.73 microseconds, about 3.3 times faster. First loads for
distinct keys can also benefit from parallel CPU computation.

Same-key first loads execute the loader once in every configuration. Later
requests either join the active load or reuse its cached result. First loads
for distinct keys still require 64 loader calls; caching cannot skip the initial
computation for independent data.

Cache hits do not get faster with more callers in this sample. Those reads are
short, and timing includes goroutine creation, scheduling, and shared Client
synchronization. These are batch completion times; the differences cannot all
be attributed to lock contention and do not imply that every request gets slower.

Warm-up and clearing between cold batches are excluded. Every read checks its
key's expected digest, and every configuration verifies its exact loader count.
Measured on 2026-10-08 with Go 1.27.1, macOS / darwin arm64, Apple M3 Pro.
Durations are medians of three samples.
[Raw output](benchmarks-concurrent-darwin-arm64.txt) includes all combinations of
CPU parallelism and caller count, together with allocations.

### Reproduce

Run from the repository root, comparing both CPU parallelism limits:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

Compare only one and four callers:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Workers=(1|4)$' -benchmem -benchtime=200ms -count=3 -cpu=1,4
```

For example, `BenchmarkConcurrentFetch/Workers=4/SameKey/FirstLoad-4` means
four callers (`Workers=4`) with GOMAXPROCS=4 (the `-4` suffix).
GOMAXPROCS=1 has no numeric suffix.

| Output | Meaning |
| --- | --- |
| `ns/op` | Total time for a complete batch of 64 requests; divide by 1,000 for microseconds |
| `requests/batch` | Requests per batch, fixed at 64 |
| `goroutines/batch` | Caller goroutines per batch: 1, 4, 16, or 64; excludes cacheq's internal goroutines |
| `loads/batch` | Actual loader calls in the measured batch |
| `merged/batch` | Average requests that joined a load while it was active |
| `B/op`, `allocs/op` | Allocations for the entire batch, including caller goroutine setup |

For correctness and race checking, without performance claims:

```sh
go test -race -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchtime=1x -cpu=1,4
```

Keep the machine idle and compare repeated samples under the same conditions.
Record the source revision, Go version, CPU, and command. This CPU-bound sample's
benefit depends on load cost and reuse; use a representative application loader
when evaluating a real workload.

To investigate lock contention, collect a separate profile:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Workers=64$/SameKey/CacheHit$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

Collect ordinary timings separately from profiling.
