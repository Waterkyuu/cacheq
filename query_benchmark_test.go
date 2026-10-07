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

// benchmarkPayloadBytes fixes each backend calculation at 64 KiB without artificial delays.
const benchmarkPayloadBytes = 64 * 1024

// BenchmarkConcurrentFetch compares equal concurrent request batches with explicit goroutine counts.
// CPU parallelism is selected separately with -cpu; it never determines the number of requests.
func BenchmarkConcurrentFetch(b *testing.B) {
	for _, requests := range []int{4, 16, 64} {
		b.Run(fmt.Sprintf("Requests=%d", requests), func(b *testing.B) {
			for _, scope := range []string{"SameKey", "DifferentKeys"} {
				b.Run(scope, func(b *testing.B) {
					for _, mode := range []string{"NoCache", "FirstLoad", "CacheHit"} {
						b.Run(mode, func(b *testing.B) {
							benchmarkConcurrentFetch(
								b,
								requests,
								scope == "SameKey",
								mode,
							)
						})
					}
				})
			}
		})
	}
}

// benchmarkConcurrentFetch starts one goroutine per request behind a shared readiness barrier.
// Every result and measured loader count is checked; cold resets and warm-up are excluded from timing.
func benchmarkConcurrentFetch(b *testing.B, requests int, sameKey bool, mode string) {
	keyCount := requests
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
		failures := make([]error, requests)
		var ready, finished sync.WaitGroup
		ready.Add(requests)
		finished.Add(requests)
		for request := range requests {
			go func() {
				defer finished.Done()
				keyIndex := request
				if sameKey {
					keyIndex = 0
				}
				ready.Done()
				<-start

				// Each worker issues one read, representing one simultaneous application request.
				var value [sha256.Size]byte
				var err error
				if mode == "NoCache" {
					value, err = loaders[keyIndex](ctx)
				} else {
					value, err = cacheq.Fetch(ctx, client, keys[keyIndex], loaders[keyIndex])
				}
				if err != nil {
					failures[request] = err
					return
				}
				if value != want[keyIndex] {
					failures[request] = fmt.Errorf("request %d returned data for the wrong key", request)
				}
			}()
		}
		// Release only after all request goroutines exist, avoiding a sequential launch-and-wait workload.
		ready.Wait()
		close(start)
		finished.Wait()
		for _, err := range failures {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	expected := uint64(b.N) * uint64(requests)
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
	b.ReportMetric(float64(requests), "requests/batch")
	b.ReportMetric(float64(client.Stats().MergedRequests)/float64(b.N), "merged/batch")
}
