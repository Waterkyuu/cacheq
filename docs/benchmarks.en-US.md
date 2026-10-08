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

Each batch performs **100 reads with GOMAXPROCS=4 and four request goroutines**.
All four callers start together, and each performs 25 reads sequentially,
waiting for one read to return before starting its next. The sequential
comparison above already covers reads by one caller.

Read either one shared key or 100 distinct keys, comparing no cache, first loads,
and cache hits under the same concurrency settings. Every scenario uses the same
loader as the sequential comparison: compute the SHA-256 digest of a fixed
64 KiB payload. Distinct keys have distinct payloads. Each reported duration
covers **the complete batch of 100 reads**, including caller goroutine setup,
synchronization, and result checks.

| Requested data | Cache state | Reads | Total time | Actual loader calls |
| --- | --- | ---: | ---: | ---: |
| Same key | No cache | 100 | 630.09 µs | 100 |
| Same key | First loads from an empty cache | 100 | 66.83 µs | 1 |
| Same key | All cache hits | 100 | 34.17 µs | 0 |
| Different keys | No cache | 100 | 629.47 µs | 100 |
| Different keys | First loads from an empty cache | 100 | 669.40 µs | 100 |
| Different keys | All cache hits | 100 | 35.69 µs | 0 |

### Interpreting the results

Compare cache scenarios within this table to measure cacheq's benefit and
additional cost under identical concurrency conditions. Same-key first loads
execute the loader once; overlapping requests join that load, and subsequent
reads reuse its result. These samples report about three merged requests per
batch, so this row includes both request merging and later cache hits.

First loads for 100 distinct keys still require 100 loader calls. This row
measures the additional cache lookup, write, and synchronization cost without
avoiding backend work. Warm-cache rows require no loader calls for either key
scope; their elapsed time measures serving cached results.

Warm-up and clearing between cold batches are excluded. Every read checks its
key's expected digest, and every scenario verifies its exact loader count.
Measured on 2026-10-09 with Go 1.27.1, macOS / darwin arm64, Apple M3 Pro.
Durations are medians of three samples.
[Raw output](benchmarks-concurrent-darwin-arm64.txt) includes all samples,
allocations, the command, and benchmark source identification.

### Reproduce

Run from the repository root with CPU parallelism fixed at four:

```sh
go test -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchmem -benchtime=200ms -count=3 -cpu=4
```

For example, `BenchmarkConcurrentFetch/Workers=4/SameKey/FirstLoad-4` means
four callers (`Workers=4`) with GOMAXPROCS=4 (the `-4` suffix).

| Output | Meaning |
| --- | --- |
| `ns/op` | Total time for a complete batch of 100 reads; divide by 1,000 for microseconds |
| `requests/batch` | Reads per batch, fixed at 100 |
| `goroutines/batch` | Request goroutines per batch, fixed at 4; excludes cacheq's internal goroutines |
| `loads/batch` | Actual loader calls in the measured batch |
| `merged/batch` | Average requests that joined a load while it was active |
| `B/op`, `allocs/op` | Allocations for the entire batch, including caller goroutine setup |

For correctness and race checking, without performance claims:

```sh
go test -race -run '^$' -bench '^BenchmarkConcurrentFetch$' -benchtime=1x -cpu=4
```

Keep the machine idle and compare repeated samples under the same conditions.
This CPU-bound sample's benefit depends on load cost and reuse; use a
representative application loader when evaluating a real workload.
