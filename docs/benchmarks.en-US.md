# Concurrent request benchmarks

[简体中文](benchmarks.zh-CN.md)

These benchmarks simulate concurrent application requests calling cacheq. Every
scenario explicitly starts **4, 16, or 64 request goroutines**. CPU parallelism
is fixed separately at GOMAXPROCS=4; it does not set the request count.

## Main comparison: 64 concurrent requests

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

## How concurrency is created

For each batch:

1. Create the specified number of goroutines, one per request.
2. Each goroutine signals readiness and waits on the same start channel.
3. After all are ready, close that channel to release the requests together.
4. Each goroutine performs one direct loader call or one cacheq Fetch.
5. Wait for all requests and check every result.

This models the cache access inside simultaneous application requests. It calls
Fetch directly and does not include HTTP transport, JSON encoding, or a real
database. The timed batch includes goroutine creation, the start barrier,
scheduling, reads, completion waits, and result checks. It is a complete burst
duration, not per-request tail latency or steady-state cache-read throughput.

The loader does deterministic CPU work without sleep-based delays. Same-key
cold requests arriving while loading contribute to `merged/batch`; later ones
can hit the newly installed result. The merge count is measured rather than
assumed to be the request count minus one. In the recorded 64-request sample,
the median of the per-run averages was 59.37 joined requests per batch.
They all still resulted in exactly one load.

## Reproduce

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
