package cacheq_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	cacheq "github.com/Waterkyuu/cacheq"
)

const (
	// benchmarkReadsPerBatch fixes the read count in the serial and concurrent benefit comparisons.
	benchmarkReadsPerBatch = 100
	// benchmarkPayloadBytes fixes the amount of deterministic backend work at 64 KiB per load.
	benchmarkPayloadBytes = 64 * 1024
	// benchmarkConcurrentRequests keeps concurrent batches the same size as the serial comparison.
	benchmarkConcurrentRequests = benchmarkReadsPerBatch
	// benchmarkConcurrentWorkers divides each concurrent batch equally among four request goroutines.
	benchmarkConcurrentWorkers = 4
)

// BenchmarkCacheBenefit compares equal read batches without caching, from an empty cache, and from a warm cache.
// All modes use the same SHA-256 loader; cold-cache resets and warm-cache preparation are excluded from timing.
func BenchmarkCacheBenefit(b *testing.B) {
	payload := make([]byte, benchmarkPayloadBytes)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	want := sha256.Sum256(payload)
	for _, mode := range []string{"NoCache", "FirstLoad", "CacheHit"} {
		b.Run(mode, func(b *testing.B) {
			client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
			defer client.Close()
			var calls atomic.Uint64
			load := func(ctx context.Context) ([sha256.Size]byte, error) {
				if err := ctx.Err(); err != nil {
					return [sha256.Size]byte{}, err
				}
				calls.Add(1)
				return sha256.Sum256(payload), nil
			}
			ctx := context.Background()
			read := load
			if mode != "NoCache" {
				read = func(ctx context.Context) ([sha256.Size]byte, error) {
					return cacheq.Fetch(ctx, client, "benefit", load)
				}
			}
			if mode == "CacheHit" {
				if value, err := read(ctx); err != nil || value != want {
					b.Fatalf("warm cache preparation failed: %v", err)
				}
				calls.Store(0)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "FirstLoad" {
					// Each batch starts empty, so calibration cannot turn the cold case into a warm-cache test.
					b.StopTimer()
					client.Clear()
					b.StartTimer()
				}
				for range benchmarkReadsPerBatch {
					if value, err := read(ctx); err != nil || value != want {
						b.Fatalf("%s returned an incorrect result: %v", mode, err)
					}
				}
			}
			b.StopTimer()
			var expected uint64
			switch mode {
			case "NoCache":
				expected = uint64(b.N) * benchmarkReadsPerBatch
			case "FirstLoad":
				expected = uint64(b.N)
			}
			if calls.Load() != expected {
				b.Fatalf("%s executed %d loads, want %d", mode, calls.Load(), expected)
			}
			b.ReportMetric(float64(calls.Load())/float64(b.N), "loads/batch")
			b.ReportMetric(benchmarkReadsPerBatch, "reads/batch")
		})
	}
}

// BenchmarkConcurrentFetch compares cache scenarios for 100 reads split among four request goroutines.
// Run with -cpu=4 so every scenario uses the same CPU parallelism and caller count.
func BenchmarkConcurrentFetch(b *testing.B) {
	b.Run(fmt.Sprintf("Workers=%d", benchmarkConcurrentWorkers), func(b *testing.B) {
		for _, scope := range []string{"SameKey", "DifferentKeys"} {
			b.Run(scope, func(b *testing.B) {
				for _, mode := range []string{"NoCache", "FirstLoad", "CacheHit"} {
					b.Run(mode, func(b *testing.B) {
						benchmarkConcurrentFetch(
							b,
							scope == "SameKey",
							mode,
						)
					})
				}
			})
		}
	})
}

// benchmarkConcurrentFetch divides 100 reads equally among four request goroutines.
// Every result and measured loader count is checked; cold resets and warm-up are excluded from timing.
func benchmarkConcurrentFetch(b *testing.B, sameKey bool, mode string) {
	keyCount := benchmarkConcurrentRequests
	if sameKey {
		keyCount = 1
	}
	client := cacheq.NewClient(cacheq.Options{StaleTime: time.Hour})
	defer client.Close()
	var calls atomic.Uint64
	keys := make([]any, keyCount)
	want := make([][sha256.Size]byte, keyCount)
	loaders := make([]cacheq.Fetcher[[sha256.Size]byte], keyCount)
	for keyIndex := range keys {
		keys[keyIndex] = fmt.Sprintf("product:%d", keyIndex+1)
		// Distinct keys represent distinct data, so an accidental cross-key reuse cannot pass.
		payload := make([]byte, benchmarkPayloadBytes)
		for i := range payload {
			payload[i] = byte((i + keyIndex) % 251)
		}
		want[keyIndex] = sha256.Sum256(payload)
		loaders[keyIndex] = func(ctx context.Context) ([sha256.Size]byte, error) {
			if err := ctx.Err(); err != nil {
				return [sha256.Size]byte{}, err
			}
			calls.Add(1)
			return sha256.Sum256(payload), nil
		}
	}
	ctx := context.Background()
	if mode == "CacheHit" {
		for keyIndex, key := range keys {
			value, err := cacheq.Fetch(ctx, client, key, loaders[keyIndex])
			if err != nil || value != want[keyIndex] {
				b.Fatalf("warm-up for %v failed: %v", key, err)
			}
		}
		calls.Store(0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if mode == "FirstLoad" {
			// Reset every batch so benchmark calibration cannot turn misses into pre-existing hits.
			b.StopTimer()
			client.Clear()
			b.StartTimer()
		}
		start := make(chan struct{})
		results := make([][sha256.Size]byte, benchmarkConcurrentRequests)
		failures := make([]error, benchmarkConcurrentRequests)
		var ready, finished sync.WaitGroup
		ready.Add(benchmarkConcurrentWorkers)
		finished.Add(benchmarkConcurrentWorkers)
		for worker := range benchmarkConcurrentWorkers {
			go func() {
				defer finished.Done()
				ready.Done()
				<-start

				// Strided ownership assigns every request once and gives callers disjoint result slots.
				for request := worker; request < benchmarkConcurrentRequests; request += benchmarkConcurrentWorkers {
					keyIndex := request
					if sameKey {
						keyIndex = 0
					}
					if mode == "NoCache" {
						results[request], failures[request] = loaders[keyIndex](ctx)
					} else {
						results[request], failures[request] = cacheq.Fetch(
							ctx,
							client,
							keys[keyIndex],
							loaders[keyIndex],
						)
					}
				}
			}()
		}
		// Start all four callers together; each performs 25 reads sequentially.
		ready.Wait()
		close(start)
		finished.Wait()
		for request, err := range failures {
			if err != nil {
				b.Fatal(err)
			}
			keyIndex := request
			if sameKey {
				keyIndex = 0
			}
			// Checking all result slots also rejects accidentally skipped requests.
			if results[request] != want[keyIndex] {
				b.Fatalf("request %d returned data for the wrong key or was not executed", request)
			}
		}
	}
	b.StopTimer()
	expected := uint64(b.N) * benchmarkConcurrentRequests
	switch mode {
	case "FirstLoad":
		expected = uint64(b.N) * uint64(keyCount)
	case "CacheHit":
		expected = 0
	}
	if calls.Load() != expected {
		b.Fatalf("%s executed %d loads, want %d", mode, calls.Load(), expected)
	}
	b.ReportMetric(float64(calls.Load())/float64(b.N), "loads/batch")
	b.ReportMetric(benchmarkConcurrentRequests, "requests/batch")
	b.ReportMetric(benchmarkConcurrentWorkers, "goroutines/batch")
	b.ReportMetric(float64(client.Stats().MergedRequests)/float64(b.N), "merged/batch")
}
