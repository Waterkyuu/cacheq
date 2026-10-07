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

`BenchmarkConcurrentFetch` simulates concurrent application requests calling cacheq. Every
scenario explicitly starts **4, 16, or 64 request goroutines**. CPU parallelism
is fixed separately at GOMAXPROCS=4; it does not set the request count.

### 64 concurrent requests

Each batch serves 64 requests, either for one shared product key or for 64
different product keys. All modes use the same loader: compute the SHA-256
digest of a fixed 64 KiB payload for that key. Distinct keys have distinct payloads.

| Requested data | Cache state | Total batch time | Actual loader calls |
| --- | --- | ---: | ---: |
| Same key | No cache | 429.02 µs | 64 |
| Same key | First load from an empty cache | 71.17 µs | 1 |
| Same key | All cache hits | 49.21 µs | 0 |
| Different keys | No cache | 439.12 µs | 64 |
| Different keys | First loads from an empty cache | 474.60 µs | 64 |
| Different keys | All cache hits | 51.29 µs | 0 |

Same-key cold requests share one load or reuse its newly cached result.
Different-key cold requests each need their own load. The latter scenario
exposes cache management overhead when nothing can yet be reused; caching does
not remove those initial backend calculations.

Warm-up and clearing between cold batches are excluded. Warm-up performs one
load per key before measurement; the warm-cache row counts only measured loads.
Every result is checked against the expected digest for its key, and each mode
asserts the exact number of measured loader calls.

Measured on 2026-10-08 with Go 1.27.1, macOS / darwin arm64, Apple M3 Pro.
Durations are medians of three samples.
[Raw output](benchmarks-concurrent-darwin-arm64.txt) includes every request count,
allocation data, and the average number of requests joining an active load.

### Reproduce

Run all request counts from the repository root, with CPU parallelism fixed:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchmem -benchtime=200ms -count=3 -cpu=4
```

Run only the 64-request scenarios:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Requests=64$' -benchmem -benchtime=200ms -count=3 -cpu=4
```

Benchmark names describe the scenario directly, for example:
`BenchmarkConcurrentFetch/Requests=64/SameKey/FirstLoad`.

| Output | Meaning |
| --- | --- |
| `ns/op` | Total time for a complete request batch; divide by 1,000 for microseconds |
| `requests/batch` | The explicit number of request goroutines: 4, 16, or 64 |
| `loads/batch` | Actual loader calls in the measured batch |
| `merged/batch` | Average requests that joined a load while it was active |
| `B/op`, `allocs/op` | Allocations for the entire batch, including request goroutine setup |

For correctness and race checking, without performance claims:

```sh
go test -race -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchtime=1x -cpu=4
```

Keep the machine idle and compare repeated samples under the same conditions.
Record the source revision, Go version, CPU, and command. This CPU-bound sample's
benefit depends on load cost and reuse; use a representative application loader
when evaluating a real workload. Timings include concurrency setup costs and
cannot be compared directly with older single-read measurements.

To investigate lock contention while keeping the request count explicit:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$/^Requests=64$/SameKey/CacheHit$' -benchtime=3s -cpu=4 -mutexprofile=/tmp/cacheq-mutex.pprof -o /tmp/cacheq-profile.test
go tool pprof /tmp/cacheq-profile.test /tmp/cacheq-mutex.pprof
```

Collect ordinary timings separately from profiling.
